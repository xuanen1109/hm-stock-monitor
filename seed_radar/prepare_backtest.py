"""回測資料準備：官方未還原日線 + 台新還原權值日 K → 除權息調整係數；輸出 0050 基準。

python -m seed_radar.prepare_backtest --export <market_ingest 匯出資料夾> --history-db <taishin_history.sqlite> --out <資料夾>

- adj_factor = 台新還原收盤 / 官方原始收盤（同日），只在除權息日變動；缺值以前值延續（只用過去）。
- 下市或台新沒有資料的股票 adj_factor 留空（回測視為 1，報告揭露檔數）。
- bench_0050.csv：台新還原權值 0050 日 K（date, open, close）。
"""
from __future__ import annotations

import argparse
import contextlib
import json
import sqlite3
from pathlib import Path

import pandas as pd


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--export", required=True)
    p.add_argument("--history-db", required=True)
    p.add_argument("--out", required=True)
    a = p.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    bars = pd.read_csv(Path(a.export) / "bars.csv", dtype={"ticker": str})
    with contextlib.closing(sqlite3.connect(f"file:{a.history_db}?mode=ro", uri=True)) as c:
        adj = pd.read_sql_query("SELECT symbol AS ticker, date, open, close AS adj_close FROM daily WHERE adjusted=1", c)
    bench = adj[adj.ticker.eq("0050")][["date", "open", "adj_close"]].rename(columns={"adj_close": "close"}).sort_values("date")
    bench.to_csv(out / "bench_0050.csv", index=False)
    b = bars.merge(adj[["ticker", "date", "adj_close"]], on=["ticker", "date"], how="left").sort_values(["ticker", "date"])
    b["adj_factor"] = (b.adj_close / b.close).where(b.adj_close.gt(0) & b.close.gt(0))
    b["adj_factor"] = b.groupby("ticker").adj_factor.ffill()
    b = b.drop(columns=["adj_close"])
    b.to_csv(out / "bars_adj.csv", index=False)
    info = dict(rows=len(b), tickers=int(b.ticker.nunique()),
                tickers_without_adjustment=int(b.groupby("ticker").adj_factor.apply(lambda s: s.isna().all()).sum()),
                bench_rows=len(bench), bench_range=[bench.date.min(), bench.date.max()] if len(bench) else None,
                missing_limits=int(b.limit_up.isna().sum()))
    (out / "prepare_info.json").write_text(json.dumps(info, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(info, ensure_ascii=False))


if __name__ == "__main__":
    main()
