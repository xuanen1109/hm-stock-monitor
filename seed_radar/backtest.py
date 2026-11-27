"""Conservative daily-bar research simulator. No execution API."""
import numpy as np
import pandas as pd
from .engine import ENGINES, REGIMES, candidates

FEE=.001425
TAX=.003
SLIP=.001
HORIZONS=(1,3,5,10,20)

def simulate(bars, signals, sessions, horizon=5, initial=1_000_000, max_positions=10):
    """Next-session open entry; horizon-th session close exit, defer locked sell.
    Limit price touch at execution time conservatively blocks the order.
    Missing exits remain unresolved, marked at last known close; never silently dropped.
    Fixed cash allocation, no leverage. Whole 1000-share lots."""
    sessions=list(pd.to_datetime(sessions))
    bars=bars.copy()
    lookup={(r.date,r.ticker):r for r in bars.itertuples()}
    bydate={d:g.sort_values("score",ascending=False) for d,g in signals.groupby("date")}
    cash=float(initial); holdings={}; trades=[]; orders=[]; equity=[]
    for i,day in enumerate(sessions):
        # Orders based only on preceding session. Exits occur at close, not before buys.
        prev=sessions[i-1] if i else None
        if prev in bydate:
            for s in bydate[prev].itertuples():
                r=lookup.get((day,s.ticker))
                active_cost=sum(p["cost"] for p in holdings.values())
                budget=min(cash,initial/max_positions,max(0,initial*REGIMES.get(s.regime,0)-active_cost))
                reason=None
                if s.ticker in holdings: reason="already_held"
                elif len(holdings)>=max_positions or budget<=0: reason="exposure_cap"
                elif r is None or r.volume<=0 or not np.isfinite(r.limit_up): reason="no_entry_data"
                elif r.open>=r.limit_up: reason="limit_up"
                if reason:
                    orders.append(dict(date=day,ticker=s.ticker,status=reason)); continue
                price=r.open*(1+SLIP)
                if price>r.limit_up:
                    orders.append(dict(date=day,ticker=s.ticker,status="slippage_above_limit"));continue
                qty=int(min(budget/(price*(1+FEE)),r.volume*.005)//1000)*1000
                if qty<=0:
                    orders.append(dict(date=day,ticker=s.ticker,status="lot_or_liquidity"));continue
                cost=qty*price*(1+FEE); cash-=cost
                holdings[s.ticker]=dict(cost=cost,qty=qty,entry=day,due=i+horizon-1,mark=r.open,regime=s.regime,signal_date=prev)
                orders.append(dict(date=day,ticker=s.ticker,status="filled"))
        for ticker,p in list(holdings.items()):
            r=lookup.get((day,ticker))
            if r is not None and np.isfinite(r.close): p["mark"]=r.close
            if i<p["due"]: continue
            if r is None or r.volume<=0 or not np.isfinite(r.limit_down) or r.close<=r.limit_down: continue
            px=max(r.limit_down,r.close*(1-SLIP))
            proceeds=p["qty"]*px*(1-FEE-TAX);cash+=proceeds
            trades.append(dict(ticker=ticker,entry=p["entry"],exit=day,regime=p["regime"],signal_date=p["signal_date"],pnl=proceeds-p["cost"],ret=proceeds/p["cost"]-1))
            del holdings[ticker]
        equity.append(dict(date=day,equity=cash+sum(p["qty"]*p["mark"] for p in holdings.values()),cash=cash))
    return pd.DataFrame(trades),pd.DataFrame(equity),pd.DataFrame(orders),holdings

def metrics(trades,equity,initial=1_000_000):
    r=trades.ret if "ret" in trades else pd.Series(dtype=float)
    p=trades.pnl if "pnl" in trades else pd.Series(dtype=float)
    pos=r[r>0]; neg=r[r<0]
    curve=pd.Series([initial]+equity.equity.tolist()) if len(equity) else pd.Series([initial])
    daily=curve.pct_change().dropna()
    ratio=lambda a,b:float(a/b) if pd.notna(b) and b>0 else None
    return dict(trades=len(r),win_rate=float((r>0).mean()) if len(r) else None,
        mean_return=float(r.mean()) if len(r) else None,median_return=float(r.median()) if len(r) else None,
        payoff_ratio=ratio(pos.mean(),-neg.mean()),profit_factor=ratio(p[p>0].sum(),-p[p<0].sum()),
        annualized_return=float((curve.iloc[-1]/initial)**(252/len(daily))-1) if len(daily) else None,
        max_drawdown=float((curve/curve.cummax()-1).min()),
        sharpe=ratio(daily.mean()*np.sqrt(252),daily.std()),
        mean_excluding_top_1pct=float(r.sort_values().iloc[:max(1,int(len(r)*.99))].mean()) if len(r)>1 else None)

def labels(frame,sessions,days=5,move=.12):
    """Research definition: stock-day event = first future close >= +12% within 5
    market sessions. Require complete observations to avoid treating censored rows as negatives.
    This is a stock-day proxy, not a unique real-world hot-stock event."""
    d=frame[["date","ticker","close"]].copy()
    wide=d.pivot(index="date",columns="ticker",values="close").reindex(sessions)
    first=wide.copy()*np.nan
    complete=wide.notna()
    for j in range(1,days+1):
        f=wide.shift(-j)
        complete &= f.notna()
        first=first.mask(first.isna()&(f/wide-1>=move),j)
    first=first.where(complete)
    valid=complete.stack().rename("label_valid")
    lead=first.stack(future_stack=True).rename("lead")
    return d.merge(pd.concat([valid,lead],axis=1).reset_index(),on=["date","ticker"],how="left")

def detection_metrics(lab,signals):
    x=lab.merge(signals[["date","ticker"]].drop_duplicates().assign(signal=True),on=["date","ticker"],how="left")
    x=x[x.label_valid.eq(True)]
    s=x.signal.eq(True); y=x.lead.notna()
    tp=int((s&y).sum()); fp=int((s&~y).sum()); fn=int((~s&y).sum()); tn=int((~s&~y).sum())
    div=lambda a,b:a/b if b else None
    return dict(tp=tp,fp=fp,fn=fn,tn=tn,precision=div(tp,tp+fp),recall=div(tp,tp+fn),false_positive_rate=div(fp,fp+tn),false_discovery_rate=div(fp,tp+fp),mean_lead=float(x.loc[s&y,"lead"].mean()) if tp else None,censored=int(len(lab)-len(x)))

def walk_forward(frame,bars,sessions,engine,horizon=5,train=252,test=63):
    """Expanding history, fixed training window for threshold selection.
    Purge 20 sessions before fold boundaries, evaluate only future held-out dates.
    Each fold starts flat; test exits may be censored. No cross-fold compounded claim."""
    sessions=list(pd.to_datetime(sessions)); out=[]
    for start in range(train+20,len(sessions),test):
        train_days=sessions[start-train-20:start-20]
        test_days=sessions[start:start+test]
        if len(test_days)<21: break
        tr=frame[frame.date.isin(train_days)]; te=frame[frame.date.isin(test_days)]
        threshold=65
        if engine in ENGINES:
            results=[]
            for t in [60,65,70,75]:
                sig=candidates(tr,engine,t).assign(score=lambda z:z[engine])
                a,b,_,_=simulate(bars,sig,train_days,horizon)
                m=metrics(a,b)
                if m["trades"]>=20: results.append((m["annualized_return"],-abs(t-65),t))
            if results: threshold=max(results)[2]
            sig=candidates(te,engine,threshold).assign(score=lambda z:z[engine])
        elif engine in ("random","momentum"):
            sig=te[te.eligible].copy()
            sig["score"]=sig.ret5 if engine=="momentum" else np.random.default_rng(2026+start).random(len(sig))
            sig=sig.sort_values("score",ascending=False).groupby("date").head(3)
        else: raise ValueError("Unknown engine")
        a,b,o,u=simulate(bars,sig,test_days,horizon)
        report=metrics(a,b)
        lab=labels(te,test_days)
        report.update(detection_metrics(lab,sig))
        report.update(engine=engine,horizon=horizon,threshold=threshold,start=str(test_days[0].date()),end=str(test_days[-1].date()),signals=len(sig),orders=len(o),unresolved=len(u),by_regime={k:{"trades":len(v),"win_rate":float((v.ret>0).mean()),"mean_return":float(v.ret.mean())} for k,v in a.groupby("regime")} if len(a) else {})
        out.append((report,a,b,o,sig))
    return out
