#!/usr/bin/env python3
"""
Monitor MEXC perpetual contract listings and capture first-day minute OHLCV data.

The script calls the public contract detail endpoint, filters for contracts whose
index origins are limited to MEXC-provided indices (heuristic for primary listings),
and pulls the first 24 hours of 1-minute candles once the listing time has passed.

State is persisted between runs in `data/mexc_monitor_state.json`, with collected
outputs aggregated in `data/mexc_primary_listings.json` and a CSV summary file.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

DATA_DIR = Path("data")
STATE_PATH = DATA_DIR / "mexc_monitor_state.json"
OUTPUT_JSON_PATH = DATA_DIR / "mexc_primary_listings.json"
OUTPUT_CSV_PATH = DATA_DIR / "mexc_primary_listings_summary.csv"

CONTRACT_DETAIL_URL = "https://contract.mexc.com/api/v1/contract/detail"
KLINE_URL_TMPL = "https://contract.mexc.com/api/v1/contract/kline/{symbol}?period=Min1&start={start}&end={end}"

# Heuristic: contracts whose indexOrigin references only MEXC-provided feeds.
PRIMARY_INDEX_ORIGINS = {"MEXC", "MEXC2", "MEXC_FUTURE"}


def fetch_json(url: str, timeout: int = 15) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "mexc-monitor/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.load(resp)


def load_json_file(path: Path, default: Any) -> Any:
    if path.exists():
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    return default


def save_json_file(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def is_primary_listing(contract: Dict[str, Any]) -> bool:
    indices = set(contract.get("indexOrigin") or [])
    return bool(indices) and indices <= PRIMARY_INDEX_ORIGINS


def pick_series(data: Dict[str, Iterable[Any]], *keys: str) -> List[Any]:
    for key in keys:
        series = data.get(key)
        if series:
            return list(series)
    return []


def fetch_first_day(symbol: str, listing_ts_sec: int) -> Tuple[List[Dict[str, Any]], Dict[str, float]]:
    end = listing_ts_sec + 24 * 3600
    now = int(time.time())
    end = min(end, now)
    if end <= listing_ts_sec:
        raise RuntimeError("Listing start time is in the future; cannot fetch candles yet.")

    url = KLINE_URL_TMPL.format(symbol=symbol, start=listing_ts_sec, end=end)
    payload = fetch_json(url)
    if not payload.get("success"):
        raise RuntimeError(f"Kline request failed for {symbol}: {payload}")

    data = payload.get("data") or {}
    times = pick_series(data, "time")
    if not times:
        raise RuntimeError("No candle data returned.")

    opens = pick_series(data, "realOpen", "open")
    highs = pick_series(data, "realHigh", "high")
    lows = pick_series(data, "realLow", "low")
    closes = pick_series(data, "realClose", "close")
    vols = pick_series(data, "vol")
    amounts = pick_series(data, "amount")

    minutes: List[Dict[str, Any]] = []
    float_highs: List[float] = []
    float_lows: List[float] = []
    float_vols: List[float] = []
    float_amounts: List[float] = []

    for t, o, h, l, c, v, a in zip(times, opens, highs, lows, closes, vols, amounts):
        fh = float(h)
        fl = float(l)
        fv = float(v)
        fa = float(a)
        minutes.append(
            {
                "timestamp": int(t),
                "open": float(o),
                "high": fh,
                "low": fl,
                "close": float(c),
                "volume": fv,
                "notional": fa,
            }
        )
        float_highs.append(fh)
        float_lows.append(fl)
        float_vols.append(fv)
        float_amounts.append(fa)

    summary = {
        "open": float(opens[0]),
        "close": float(closes[-1]),
        "high": max(float_highs),
        "low": min(float_lows),
        "volume": sum(float_vols),
        "notional": sum(float_amounts),
        "minutesCaptured": len(minutes),
        "coverageSeconds": end - listing_ts_sec,
        "coverageComplete": (end - listing_ts_sec) >= (24 * 3600),
    }
    return minutes, summary


def isoformat_ms(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).isoformat()


def update_outputs(entries: Dict[str, Dict[str, Any]]) -> None:
    existing = load_json_file(OUTPUT_JSON_PATH, [])
    merged: Dict[str, Dict[str, Any]] = {row["symbol"]: row for row in existing}
    merged.update(entries)
    ordered = sorted(merged.values(), key=lambda row: row["listingTimestampMs"])
    save_json_file(OUTPUT_JSON_PATH, ordered)

    csv_lines = [
        "symbol,listing_utc,open,high,low,close,volume,notional_usdt,minutes_captured,coverage_complete"
    ]
    for row in ordered:
        summary = row["firstDaySummary"]
        csv_lines.append(
            ",".join(
                [
                    row["symbol"],
                    row["listingTimeUTC"],
                    f"{summary['open']}",
                    f"{summary['high']}",
                    f"{summary['low']}",
                    f"{summary['close']}",
                    f"{summary['volume']}",
                    f"{summary['notional']}",
                    f"{summary['minutesCaptured']}",
                    "true" if summary["coverageComplete"] else "false",
                ]
            )
        )
    OUTPUT_CSV_PATH.write_text("\n".join(csv_lines), encoding="utf-8")


def run_monitor(require_complete_day: bool, verbose: bool) -> int:
    ensure_data_dir()
    state = load_json_file(STATE_PATH, {"seen": {}})
    seen: Dict[str, Dict[str, Any]] = state.setdefault("seen", {})

    try:
        payload = fetch_json(CONTRACT_DETAIL_URL)
    except urllib.error.URLError as exc:
        print(f"Failed to fetch contract detail: {exc}", file=sys.stderr)
        return 1

    contracts = payload.get("data") or []
    now_ms = int(time.time() * 1000)

    collected: Dict[str, Dict[str, Any]] = {}
    processed = 0

    for contract in contracts:
        if not is_primary_listing(contract):
            continue
        symbol = contract.get("symbol")
        if not symbol:
            continue
        listing_ms = contract.get("openingTime") or contract.get("createTime") or 0
        if not listing_ms:
            continue
        seen_entry = seen.setdefault(
            symbol,
            {
                "listingTimestampMs": listing_ms,
                "firstDayCaptured": False,
                "lastChecked": None,
            },
        )
        # Update listing timestamp if endpoint backfilled a value.
        seen_entry["listingTimestampMs"] = listing_ms
        seen_entry["lastChecked"] = now_ms

        if seen_entry.get("firstDayCaptured"):
            continue

        listing_sec = listing_ms // 1000
        if listing_ms > now_ms:
            continue
        if require_complete_day and now_ms < listing_ms + 24 * 3600 * 1000:
            continue

        try:
            minutes, summary = fetch_first_day(symbol, listing_sec)
        except Exception as exc:  # pylint: disable=broad-except
            if verbose:
                print(f"[warn] {symbol}: {exc}")
            continue

        collected[symbol] = {
            "symbol": symbol,
            "baseCoin": contract.get("baseCoin"),
            "quoteCoin": contract.get("quoteCoin"),
            "listingTimestampMs": listing_ms,
            "listingTimeUTC": isoformat_ms(listing_ms),
            "indexOrigin": contract.get("indexOrigin"),
            "detectedAtMs": now_ms,
            "firstDaySummary": summary,
            "firstDayMinutes": minutes,
        }
        seen_entry["firstDayCaptured"] = True
        processed += 1
        if verbose:
            print(f"[info] Captured {symbol}: {summary['minutesCaptured']} minutes")

    if collected:
        update_outputs(collected)
        save_json_file(STATE_PATH, state)
    else:
        # Still persist latest check timestamps.
        save_json_file(STATE_PATH, state)

    if verbose:
        print(
            f"[info] processed={processed} collected={len(collected)} total_seen={len(seen)}"
        )
    return 0


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--require-complete-day",
        action="store_true",
        help="Only collect listings whose first 24 hours have fully elapsed.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print progress information.",
    )
    args = parser.parse_args(argv)

    return run_monitor(require_complete_day=args.require_complete_day, verbose=args.verbose)


if __name__ == "__main__":
    exit_code = main(sys.argv[1:])
    sys.exit(exit_code)
