"""彙總 walk-forward 各折結果：分「2024 年至今」與「全部（約五年）」兩段，依策略 × 持有天數。

python -m seed_radar.summarize --run <回測輸出 run 資料夾> [--since 2024-01-01]

各折是獨立帳戶，不串成複利；這裡列各折平均（年化、Sharpe）、最差（最大回撤）與合計（交易數、偵測計數）。
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def agg(f: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (e, h), g in f.groupby(["engine", "horizon"]):
        tp, fp, fn, tn = (int(g[k].sum()) for k in ("tp", "fp", "fn", "tn"))
        rows.append(dict(engine=e, horizon=int(h), folds=len(g), trades=int(g.trades.sum()),
                         win_rate=g.win_rate.mean(), payoff_ratio=g.payoff_ratio.mean(), profit_factor=g.profit_factor.median(),
                         annualized_mean=g.annualized_return.mean(), max_drawdown_worst=g.max_drawdown.min(), sharpe_mean=g.sharpe.mean(),
                         precision=tp / (tp + fp) if tp + fp else None, recall=tp / (tp + fn) if tp + fn else None,
                         false_positive_rate=fp / (fp + tn) if fp + tn else None,
                         mean_lead_days=(g.mean_lead * g.tp).sum() / tp if tp else None,
                         bench_0050_annualized_mean=g.bench_annualized.mean() if "bench_annualized" in g else None,
                         excess_vs_0050_mean=g.excess_annualized_vs_0050.mean() if "excess_annualized_vs_0050" in g else None,
                         unresolved=int(g.unresolved.sum())))
    return pd.DataFrame(rows)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run", required=True)
    p.add_argument("--since", default="2024-01-01")
    a = p.parse_args()
    f = pd.read_csv(Path(a.run) / "fold_metrics.csv")
    out = {}
    for label, part in (("since_" + a.since, f[f.start >= a.since]), ("all_folds", f)):
        s = agg(part)
        s.to_csv(Path(a.run) / f"summary_{label}.csv", index=False)
        out[label] = dict(period=[part.start.min(), part.end.max()] if len(part) else None, table=s.round(4).to_dict("records"))
    (Path(a.run) / "summary.json").write_text(json.dumps(out, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    pd.set_option("display.width", 250)
    for k, v in out.items():
        print(k, v["period"])
        print(pd.DataFrame(v["table"]).to_string(index=False))


if __name__ == "__main__":
    main()
