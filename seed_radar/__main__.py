"""Offline CLI; inputs are local CSV snapshots only. No broker dependencies."""
import argparse
import json
from pathlib import Path
from datetime import datetime, timezone
import sqlite3
import uuid
import pandas as pd
from .engine import VERSION, ENGINES, read_bars, features, leaderboard
from .backtest import HORIZONS, benchmark_metrics, walk_forward

def archive(db,run,kind,records):
    with sqlite3.connect(db) as con:
        con.execute("CREATE TABLE IF NOT EXISTS audit (id INTEGER PRIMARY KEY, run TEXT NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL)")
        con.execute("CREATE TRIGGER IF NOT EXISTS no_update BEFORE UPDATE ON audit BEGIN SELECT RAISE(ABORT,'append only'); END")
        con.execute("CREATE TRIGGER IF NOT EXISTS no_delete BEFORE DELETE ON audit BEGIN SELECT RAISE(ABORT,'append only'); END")
        con.executemany("INSERT INTO audit(run,kind,payload) VALUES (?,?,?)",[(run,kind,json.dumps(r,default=str,ensure_ascii=False)) for r in records])

def main():
    p=argparse.ArgumentParser()
    p.add_argument("mode",choices=["scan","backtest"])
    for arg in ["bars","market"]: p.add_argument("--"+arg,required=True)
    for arg in ["industry","news","fundamentals"]: p.add_argument("--"+arg)
    p.add_argument("--out",default="seed_output")
    p.add_argument("--engine",choices=ENGINES+["all"],default="all")
    p.add_argument("--as-of",help="Last permissible session YYYY-MM-DD")
    p.add_argument("--benchmark",help="0050 adjusted daily CSV: date,close[,open]")
    a=p.parse_args()
    root=Path(a.out);root.mkdir(parents=True,exist_ok=True)
    run=uuid.uuid4().hex
    out=root/run;out.mkdir()
    db=root/"audit.sqlite"
    optional=lambda path:pd.read_csv(path,dtype={"ticker":str}) if path else None
    bars=read_bars(a.bars)
    market=pd.read_csv(a.market)
    market["date"]=pd.to_datetime(market.date).dt.normalize()
    if a.as_of:
        end=pd.Timestamp(a.as_of)
        bars=bars[bars.date<=end];market=market[market.date<=end]
    if bars.empty or market.empty: raise ValueError("No bars or market data")
    frame=features(bars,market,optional(a.industry),optional(a.news),optional(a.fundamentals))
    engines=ENGINES if a.engine=="all" else [a.engine]
    latest=market.date.max()
    manifest=dict(run=run,version=VERSION,mode=a.mode,created_at=datetime.now(timezone.utc).isoformat(),data_date=str(latest.date()),live_order_enabled=False,coverage="local CSV only; universe completeness and timestamps not independently verified",realtime=False)
    archive(db,run,"manifest",[manifest])
    frame.to_csv(out/"features.csv",index=False)
    if a.mode=="scan":
        rows=[]
        for e in engines:
            rows.extend(dict(r,engine=e,data_status="EOD_ONLY_NOT_REALTIME") for r in leaderboard(frame[frame.date<=latest],e) if pd.Timestamp(r["date"])==latest)
        if frame.date.max()!=latest: manifest["warning"]="Market and equity session mismatch; ranking withheld";rows=[]
        manifest["warning"]=manifest.get("warning","EOD snapshot; check exchange calendar and source freshness before use")
        archive(db,run,"ranking",rows)
        # Save all scores, not merely successful selections.
        archive(db,run,"evaluated",frame[frame.date==latest][["date","ticker","stage","eligible"]+ENGINES].to_dict("records"))
        (out/"ranking.json").write_text(json.dumps(rows,default=str,ensure_ascii=False,indent=2),encoding="utf-8")
    else:
        reports=[]
        sessions=sorted(market.date.unique())
        bench=pd.read_csv(a.benchmark) if a.benchmark else None
        for e in engines+["random","momentum"]:
            for h in HORIZONS:
                for i,(report,t,eq,o,sig) in enumerate(walk_forward(frame,bars,sessions,e,h)):
                    if bench is not None:
                        test_days=[d for d in sessions if pd.Timestamp(report["start"])<=d<=pd.Timestamp(report["end"])]
                        report.update(benchmark_metrics(bench,test_days))
                        if report.get("annualized_return") is not None and report.get("bench_annualized") is not None:
                            report["excess_annualized_vs_0050"]=report["annualized_return"]-report["bench_annualized"]
                    reports.append(report)
                    stem=f"{e}_{h}_{i}"
                    for name,df in [("trades",t),("equity",eq),("orders",o),("signals",sig)]:
                        df.to_csv(out/f"{stem}_{name}.csv",index=False)
                        if name!="equity": archive(db,run,stem+"_"+name,df.to_dict("records"))
        pd.DataFrame(reports).to_csv(out/"fold_metrics.csv",index=False)
        archive(db,run,"fold_metrics",reports)
        manifest["warning"]="No completed folds" if not reports else "Fold-level estimates only; unresolved positions may bias trade metrics"
        manifest["benchmark_0050"]="per-fold buy-and-hold on the same test sessions (fees+ETF tax)" if bench is not None else "not provided; no excess return claim"
    (out/"manifest.json").write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding="utf-8")
    print(out)

if __name__=="__main__": main()
