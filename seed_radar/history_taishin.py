"""台新行情歷史資料下載（唯讀、每日增量、可中斷續跑）。

python -m seed_radar.history_taishin --db D:/seed/taishin_history.sqlite --live-db D:/seed/live.sqlite \
    --login-hook <本機登入程式> [--years 5] [--minute-days 30] [--max-symbols N]

- daily：還原權值日 K（adjusted=true，處理除權息與分割），預設五年；已下載區間不重抓。
- minute：近 N 日 1 分 K，用來建立「個股同時段累積量」基準（minute_profile）。
- 每次 API 回應原文（gzip）與取得時間保存於 raw；進度記錄於 progress，失敗下次重試。
- 頻率：預設每次呼叫間隔 0.6 秒；遇 HTTP 429 暫停 65 秒並放慢。盤中（08:30–13:35）預設拒絕執行，
  避免與盤中交易系統搶用 API 額度。
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import sqlite3
import time
from pathlib import Path

import pandas as pd

from .store import TZ, iso, now
from .taishin_source import SourceError, TaishinMarketData

SCHEMA = """
CREATE TABLE IF NOT EXISTS raw(symbol TEXT, kind TEXT, start TEXT, end TEXT, fetched TEXT, sha256 TEXT, body BLOB,
    PRIMARY KEY(symbol, kind, start, end, sha256));
CREATE TABLE IF NOT EXISTS daily(symbol TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL,
    turnover REAL, change REAL, adjusted INTEGER, fetched TEXT, PRIMARY KEY(symbol, date, adjusted));
CREATE TABLE IF NOT EXISTS minute(symbol TEXT, ts TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL,
    fetched TEXT, PRIMARY KEY(symbol, ts));
CREATE TABLE IF NOT EXISTS progress(symbol TEXT, kind TEXT, covered_from TEXT, covered_to TEXT, updated TEXT,
    rows INTEGER, state TEXT, error TEXT, PRIMARY KEY(symbol, kind));
CREATE TABLE IF NOT EXISTS minute_profile(symbol TEXT, minute_index INTEGER, avg_cum_lots REAL, sessions INTEGER,
    as_of TEXT, PRIMARY KEY(symbol, minute_index));
"""
CORE_ETF = ("0050", "006208", "00631L", "00632R")


def open_db(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(path, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript(SCHEMA)
    return c


def universe(live_db: str) -> list[str]:
    c = sqlite3.connect(f"file:{live_db}?mode=ro", uri=True)
    try:
        d = c.execute("SELECT MAX(date) FROM tickers").fetchone()[0]
        syms = [r[0] for r in c.execute("SELECT symbol FROM tickers WHERE date=? ORDER BY symbol", (d,))]
    finally:
        c.close()
    common = [s for s in syms if len(s) == 4 and s[0] in "123456789"]
    return list(CORE_ETF) + [s for s in common if s not in CORE_ETF]


class Fetcher:
    def __init__(self, src: TaishinMarketData, c, interval: float):
        self.src, self.c, self.interval = src, c, interval
        self.calls = self.errors = 0

    def get(self, symbol, kind, start, end, timeframe, adjusted):
        while True:
            self.src.min_interval = self.interval
            try:
                d = self.src.historical(symbol, start, end, timeframe=timeframe, adjusted=adjusted)
                self.calls += 1
                break
            except SourceError as e:
                if e.status == 429:
                    self.interval = min(self.interval * 1.5, 5)
                    print(f"  HTTP 429，暫停 65 秒，間隔改為 {self.interval:.2f}s", flush=True)
                    time.sleep(65)
                    continue
                if e.status == 404:
                    self.calls += 1
                    return []
                self.errors += 1
                raise
        body = json.dumps(d, ensure_ascii=False).encode()
        self.c.execute("INSERT OR IGNORE INTO raw VALUES(?,?,?,?,?,?,?)",
                       (symbol, kind, start, end, iso(now()), hashlib.sha256(body).hexdigest(), gzip.compress(body)))
        return d.get("data") or []


def chunks(start: dt.date, end: dt.date, days: int):
    cur = start
    while cur <= end:
        nxt = min(cur + dt.timedelta(days=days - 1), end)
        yield cur, nxt
        cur = nxt + dt.timedelta(days=1)


def sync_daily(f: Fetcher, symbol: str, start: dt.date, end: dt.date):
    c = f.c
    p = c.execute("SELECT covered_from, covered_to FROM progress WHERE symbol=? AND kind='daily' AND state='ok'", (symbol,)).fetchone()
    ranges = []
    if p:
        lo, hi = dt.date.fromisoformat(p[0]), dt.date.fromisoformat(p[1])
        if start < lo:
            ranges.append((start, lo - dt.timedelta(days=1)))
        if hi < end:
            ranges.append((hi + dt.timedelta(days=1), end))
    else:
        ranges.append((start, end))
    n = 0
    for a, b in ranges:
        for x, y in chunks(a, b, 365):
            rows = f.get(symbol, "daily_adj", x.isoformat(), y.isoformat(), "D", True)
            fetched = iso(now())
            c.executemany("INSERT OR REPLACE INTO daily VALUES(?,?,?,?,?,?,?,?,?,1,?)",
                          [(symbol, r["date"], r.get("open"), r.get("high"), r.get("low"), r.get("close"), r.get("volume"),
                            r.get("turnover"), r.get("change"), fetched) for r in rows])
            n += len(rows)
    lo = min(start, dt.date.fromisoformat(p[0])) if p else start
    hi = max(end, dt.date.fromisoformat(p[1])) if p else end
    c.execute("INSERT OR REPLACE INTO progress VALUES(?,?,?,?,?,?,?,NULL)", (symbol, "daily", lo.isoformat(), hi.isoformat(), iso(now()), n, "ok"))
    c.commit()
    return n


def sync_minute(f: Fetcher, symbol: str, start: dt.date, end: dt.date):
    c = f.c
    p = c.execute("SELECT covered_to FROM progress WHERE symbol=? AND kind='minute' AND state='ok'", (symbol,)).fetchone()
    if p:
        start = max(start, dt.date.fromisoformat(p[0]) + dt.timedelta(days=1))
    n = 0
    for x, y in chunks(start, end, 10):
        rows = f.get(symbol, "minute", x.isoformat(), y.isoformat(), "1", False)
        fetched = iso(now())
        c.executemany("INSERT OR REPLACE INTO minute VALUES(?,?,?,?,?,?,?,?)",
                      [(symbol, r["date"], r.get("open"), r.get("high"), r.get("low"), r.get("close"), r.get("volume"), fetched) for r in rows])
        n += len(rows)
    if start <= end:
        c.execute("INSERT OR REPLACE INTO progress VALUES(?,?,?,?,?,?,?,NULL)", (symbol, "minute", start.isoformat(), end.isoformat(), iso(now()), n, "ok"))
    c.commit()
    return n


def build_minute_profile(c, as_of: str, sessions: int = 20):
    """每檔最近 sessions 個交易日，每分鐘（0=09:00）累積成交量的平均（張）。只用 as_of 以前的資料。"""
    m = pd.read_sql_query("SELECT symbol, ts, volume FROM minute WHERE ts < ?", c, params=(as_of,))
    if m.empty:
        return 0
    m["t"] = pd.to_datetime(m.ts, format="ISO8601")
    m["day"] = m.t.dt.date
    m["k"] = ((m.t.dt.hour - 9) * 60 + m.t.dt.minute).astype(int)
    out = []
    for sym, g in m.groupby("symbol"):
        days = sorted(g.day.unique())[-sessions:]
        g = g[g.day.isin(days)]
        piv = g.pivot_table(index="k", columns="day", values="volume", aggfunc="sum").reindex(range(271)).fillna(0).cumsum()
        if piv.shape[1] < 5:
            continue
        avg = piv.mean(axis=1)
        out += [(sym, int(k), float(v), int(piv.shape[1]), as_of) for k, v in avg.items()]
    c.execute("DELETE FROM minute_profile")
    c.executemany("INSERT INTO minute_profile VALUES(?,?,?,?,?)", out)
    c.commit()
    return len({r[0] for r in out})


def main():
    p = argparse.ArgumentParser(description="台新歷史行情增量下載（唯讀）")
    p.add_argument("--db", required=True)
    p.add_argument("--live-db", required=True)
    p.add_argument("--login-hook")
    p.add_argument("--years", type=int, default=5)
    p.add_argument("--minute-days", type=int, default=30)
    p.add_argument("--interval", type=float, default=0.6)
    p.add_argument("--max-symbols", type=int)
    p.add_argument("--skip-daily", action="store_true")
    p.add_argument("--skip-minute", action="store_true")
    p.add_argument("--allow-intraday", action="store_true", help="允許盤中執行（預設拒絕，避免占用盤中 API 額度）")
    a = p.parse_args()
    t = now()
    if t.weekday() < 5 and dt.time(8, 30) <= t.time() <= dt.time(13, 35) and not a.allow_intraday:
        raise SystemExit("盤中不執行歷史下載（避免影響即時行情與交易系統）；收盤後再跑，或加 --allow-intraday")
    end = t.date() if t.time() >= dt.time(14, 30) else t.date() - dt.timedelta(days=1)
    c = open_db(a.db)
    syms = universe(a.live_db)[: a.max_symbols]
    f = Fetcher(TaishinMarketData(a.login_hook, a.interval), c, a.interval)
    start_d = end.replace(year=end.year - a.years)
    start_m = end - dt.timedelta(days=a.minute_days)
    t0 = time.monotonic()
    for i, s in enumerate(syms, 1):
        for kind, fn, start in (("daily", sync_daily, start_d), ("minute", sync_minute, start_m)):
            if (kind == "daily" and a.skip_daily) or (kind == "minute" and a.skip_minute):
                continue
            try:
                fn(f, s, start, end)
            except Exception as e:  # noqa: BLE001
                c.execute("INSERT OR REPLACE INTO progress VALUES(?,?,NULL,NULL,?,0,'error',?)", (s, kind, iso(now()), str(e)[:300]))
                c.commit()
        if i % 50 == 0 or i == len(syms):
            el = time.monotonic() - t0
            print(f"{now():%H:%M:%S} {i}/{len(syms)} 檔，API {f.calls} 次，錯誤 {f.errors}，已用 {el / 60:.1f} 分，"
                  f"預估剩 {el / i * (len(syms) - i) / 60:.0f} 分", flush=True)
    n = build_minute_profile(c, (end + dt.timedelta(days=1)).isoformat())
    print(json.dumps({"symbols": len(syms), "api_calls": f.calls, "errors": f.errors, "minute_profile_symbols": n,
                      "end": end.isoformat(), "finished": iso(now())}, ensure_ascii=False), flush=True)
    import os
    os._exit(0)


if __name__ == "__main__":
    main()
