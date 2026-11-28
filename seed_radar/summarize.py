"""彙總 walk-forward 各折結果：分「2024 年至今」與「全部（約五年）」兩段，依策略 × 持有天數。

python -m seed_radar.summarize --run <回測輸出 run 資料夾> [--since 2024-01-01] [--benchmark bench_0050.csv]

- 各折是獨立帳戶（每折從現金開始）；年化、Sharpe 為各折平均，最大回撤取最差折。
- chained_*：把連續、不重疊的各折報酬依序相乘，近似整段績效（折與折之間清倉重來）。
- 每筆交易另與「同一進出場日」持有 0050 比較（需 --benchmark），避免拿低曝險帳戶直接比滿倉 0050。
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

FEE, ETF_TAX = .001425, .001


def fold_details(run: Path, f: pd.DataFrame, bench: pd.DataFrame | None) -> pd.DataFrame:
    """Per-fold return, sessions, exposure and per-trade comparison, from the saved equity/trades files."""
    f = f.copy()
    f["fold"] = f.groupby(["engine", "horizon"]).cumcount()
    rows = []
    for r in f.itertuples():
        stem = f"{r.engine}_{r.horizon}_{r.fold}"
        eq_path, tr_path = run / f"{stem}_equity.csv", run / f"{stem}_trades.csv"
        eq = pd.read_csv(eq_path) if eq_path.exists() else pd.DataFrame()
        tr = pd.read_csv(tr_path) if tr_path.exists() and tr_path.stat().st_size > 2 else pd.DataFrame()
        d = dict(engine=r.engine, horizon=r.horizon, fold=r.fold, sessions=len(eq),
                 fold_return=float(eq.equity.iloc[-1] / 1_000_000 - 1) if len(eq) else np.nan,
                 exposure=float((1 - eq.cash / eq.equity).mean()) if len(eq) else np.nan,
                 trade_ret_sum=float(tr.ret.sum()) if len(tr) else 0.0, n_trades=len(tr),
                 bench_trade_ret_sum=np.nan, beat_bench=np.nan)
        if bench is not None and len(tr):
            b_in = bench.open.reindex(pd.to_datetime(tr.entry)).to_numpy()
            b_out = bench.close.reindex(pd.to_datetime(tr.exit)).to_numpy()
            br = b_out / b_in * (1 - FEE) * (1 - FEE - ETF_TAX) - 1
            ok = np.isfinite(br)
            d.update(bench_trade_ret_sum=float(br[ok].sum()), bench_trades=int(ok.sum()),
                     trade_ret_sum_matched=float(tr.ret.to_numpy()[ok].sum()),
                     beat_bench=int((tr.ret.to_numpy()[ok] > br[ok]).sum()))
        rows.append(d)
    return f.merge(pd.DataFrame(rows), on=["engine", "horizon", "fold"])


def chain(r: pd.Series, n: pd.Series):
    total = float(np.prod(1 + r.fillna(0)) - 1)
    days = int(n.sum())
    return total, (float((1 + total) ** (252 / days) - 1) if days else None)


def agg(f: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (e, h), g in f.groupby(["engine", "horizon"]):
        g = g.sort_values("start")
        tp, fp, fn, tn = (int(g[k].sum()) for k in ("tp", "fp", "fn", "tn"))
        row = dict(engine=e, horizon=int(h), folds=len(g), trades=int(g.trades.sum()),
                   win_rate=g.win_rate.mean(), payoff_ratio=g.payoff_ratio.mean(), profit_factor=g.profit_factor.median(),
                   annualized_mean=g.annualized_return.mean(), max_drawdown_worst=g.max_drawdown.min(), sharpe_mean=g.sharpe.mean(),
                   precision=tp / (tp + fp) if tp + fp else None, recall=tp / (tp + fn) if tp + fn else None,
                   false_positive_rate=fp / (fp + tn) if fp + tn else None,
                   base_rate=(tp + fn) / (tp + fp + fn + tn) if tp + fp + fn + tn else None,
                   mean_lead_days=(g.mean_lead * g.tp).sum() / tp if tp else None,
                   bench_0050_annualized_mean=g.bench_annualized.mean() if "bench_annualized" in g else None,
                   excess_vs_0050_mean=g.excess_annualized_vs_0050.mean() if "excess_annualized_vs_0050" in g else None,
                   unresolved=int(g.unresolved.sum()))
        if "fold_return" in g:
            row["chained_return"], row["chained_cagr"] = chain(g.fold_return, g.sessions)
            row["avg_exposure"] = g.exposure.mean()
            n = g.n_trades.sum()
            row["mean_trade_return"] = g.trade_ret_sum.sum() / n if n else None
        if "bench_return" in g:
            row["bench_chained_return"], row["bench_chained_cagr"] = chain(g.bench_return, g.sessions)
        if "bench_trades" in g and g.bench_trades.sum():
            m = g.bench_trades.sum()
            row["mean_trade_return_matched"] = g.trade_ret_sum_matched.sum() / m
            row["mean_0050_same_window"] = g.bench_trade_ret_sum.sum() / m
            row["per_trade_excess_vs_0050"] = row["mean_trade_return_matched"] - row["mean_0050_same_window"]
            row["share_trades_beating_0050"] = g.beat_bench.sum() / m
        rows.append(row)
    return pd.DataFrame(rows)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run", required=True)
    p.add_argument("--since", default="2024-01-01")
    p.add_argument("--benchmark", help="bench_0050.csv（date, open, close；還原權值）")
    a = p.parse_args()
    run = Path(a.run)
    f = pd.read_csv(run / "fold_metrics.csv")
    bench = None
    if a.benchmark:
        bench = pd.read_csv(a.benchmark)
        bench["date"] = pd.to_datetime(bench.date).dt.normalize()
        bench = bench.set_index("date")
    f = fold_details(run, f, bench)
    out = {}
    for label, part in (("since_" + a.since, f[f.start >= a.since]), ("all_folds", f)):
        s = agg(part)
        s.to_csv(run / f"summary_{label}.csv", index=False)
        out[label] = dict(period=[part.start.min(), part.end.max()] if len(part) else None, table=s.round(4).to_dict("records"))
    (run / "summary.json").write_text(json.dumps(out, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    pd.set_option("display.width", 300)
    pd.set_option("display.max_columns", 40)
    for k, v in out.items():
        print(k, v["period"])
        print(pd.DataFrame(v["table"]).to_string(index=False))


if __name__ == "__main__":
    main()
