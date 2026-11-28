"""即時資料中心的本機持久化 SQLite（WAL）。所有時間為 Asia/Taipei ISO-8601（含時區）。

資料檔放在 Git 以外的持久目錄（例如 D:\\...\\seed_radar_data），不提交 GitHub。
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Taipei")
SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshot_runs(id INTEGER PRIMARY KEY, market TEXT, trade_date TEXT, server_time TEXT,
    requested TEXT, received TEXT, rows INTEGER, ok INTEGER, error TEXT);
CREATE TABLE IF NOT EXISTS snap(date TEXT, minute TEXT, symbol TEXT, market TEXT, type TEXT, name TEXT,
    open REAL, high REAL, low REAL, last REAL, change_pct REAL, volume_lots REAL, value REAL,
    last_trade TEXT, received TEXT, PRIMARY KEY(date, minute, symbol));
CREATE TABLE IF NOT EXISTS ticks(symbol TEXT, exchange_time TEXT, received TEXT, price REAL, size REAL,
    cum_volume REAL, bid REAL, ask REAL, serial INTEGER, PRIMARY KEY(symbol, serial));
CREATE TABLE IF NOT EXISTS books(symbol TEXT PRIMARY KEY, exchange_time TEXT, received TEXT, bids TEXT, asks TEXT);
CREATE TABLE IF NOT EXISTS tickers(date TEXT, symbol TEXT, exchange TEXT, name TEXT, industry TEXT,
    is_normal INTEGER, fetched TEXT, PRIMARY KEY(date, symbol));
CREATE TABLE IF NOT EXISTS heartbeat(id INTEGER PRIMARY KEY CHECK(id = 1), checked TEXT, payload TEXT);
CREATE TABLE IF NOT EXISTS api_usage(ts TEXT, payload TEXT);
CREATE TABLE IF NOT EXISTS feed_events(ts TEXT, level TEXT, kind TEXT, message TEXT);
CREATE TABLE IF NOT EXISTS public_raw(source TEXT, fetched TEXT, url TEXT, sha256 TEXT, body TEXT, PRIMARY KEY(source, sha256));
CREATE TABLE IF NOT EXISTS announcements(symbol TEXT, published TEXT, subject TEXT, clause TEXT, fact_date TEXT,
    source TEXT, fetched TEXT, polarity TEXT, PRIMARY KEY(symbol, published, subject));
CREATE TABLE IF NOT EXISTS fundamentals(symbol TEXT, as_of TEXT, pe REAL, pb REAL, dividend_yield REAL,
    revenue_month TEXT, revenue_yoy REAL, revenue_mom REAL, revenue_cum_yoy REAL, industry_name TEXT,
    source TEXT, fetched TEXT, PRIMARY KEY(symbol, as_of));
CREATE TABLE IF NOT EXISTS profile(date TEXT, minute_index INTEGER, cum_fraction REAL, sample INTEGER,
    PRIMARY KEY(date, minute_index));
CREATE TABLE IF NOT EXISTS scores(run TEXT, date TEXT, minute TEXT, symbol TEXT, payload TEXT, PRIMARY KEY(run, symbol));
CREATE TABLE IF NOT EXISTS detections(date TEXT, symbol TEXT, engine TEXT, first_seen TEXT, last_seen TEXT,
    max_score REAL, first_price REAL, PRIMARY KEY(date, symbol, engine));
CREATE TABLE IF NOT EXISTS reports(ts TEXT PRIMARY KEY, status TEXT, payload TEXT);
CREATE TABLE IF NOT EXISTS eod(date TEXT, symbol TEXT, market TEXT, name TEXT, open REAL, high REAL, low REAL,
    close REAL, volume_lots REAL, value REAL, change_pct REAL, source TEXT, saved TEXT, PRIMARY KEY(date, symbol));
CREATE INDEX IF NOT EXISTS ix_snap_symbol ON snap(symbol, date, minute);
CREATE INDEX IF NOT EXISTS ix_ticks_time ON ticks(exchange_time);
CREATE TRIGGER IF NOT EXISTS scores_no_update BEFORE UPDATE ON scores BEGIN SELECT RAISE(ABORT, 'append only'); END;
CREATE TRIGGER IF NOT EXISTS scores_no_delete BEFORE DELETE ON scores BEGIN SELECT RAISE(ABORT, 'append only'); END;
CREATE TRIGGER IF NOT EXISTS reports_no_delete BEFORE DELETE ON reports BEGIN SELECT RAISE(ABORT, 'append only'); END;
"""


def now() -> dt.datetime:
    return dt.datetime.now(TZ)


def iso(t: dt.datetime | None) -> str | None:
    return t.astimezone(TZ).isoformat(timespec="milliseconds") if t else None


def from_us(v) -> dt.datetime | None:
    """Fugle 格式時間（微秒/毫秒/秒 epoch）轉台北時間。"""
    if v in (None, "", 0):
        return None
    v = float(v)
    if v > 1e14:
        v /= 1e6
    elif v > 1e11:
        v /= 1e3
    return dt.datetime.fromtimestamp(v, TZ)


def connect(path: str | Path, readonly: bool = False) -> sqlite3.Connection:
    p = Path(path)
    if readonly:
        c = sqlite3.connect(p.resolve().as_uri() + "?mode=ro", uri=True, timeout=30)
    else:
        p.parent.mkdir(parents=True, exist_ok=True)
        c = sqlite3.connect(p, timeout=30)
        c.execute("PRAGMA journal_mode=WAL")
        c.executescript(SCHEMA)
    c.row_factory = sqlite3.Row
    return c


def event(c: sqlite3.Connection, level: str, kind: str, message: str):
    c.execute("INSERT INTO feed_events VALUES(?,?,?,?)", (iso(now()), level, kind, message[:500]))
    c.commit()


def set_heartbeat(c: sqlite3.Connection, payload: dict):
    t = iso(now())
    c.execute("INSERT OR REPLACE INTO heartbeat VALUES(1,?,?)", (t, json.dumps(payload, ensure_ascii=False, default=str)))
    c.commit()


def heartbeat(c: sqlite3.Connection) -> tuple[str | None, dict]:
    r = c.execute("SELECT checked, payload FROM heartbeat WHERE id=1").fetchone()
    return (r[0], json.loads(r[1])) if r else (None, {})
