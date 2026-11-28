"""Seed Radar research v0.1. No broker or live execution."""
import numpy as np
import pandas as pd

VERSION = "0.1.0"
WEIGHTS = {"flow": .25, "news": .25, "sector": .20, "relative": .15, "fundamental": .15}
ENGINES = ["capital", "rotation", "catalyst", "reversal", "combined"]
REGIMES = {"strong_bull":1., "bull":.8, "sideways":.5, "bear":.25, "strong_bear":0.}
REQUIRED = ["date","ticker","name","open","high","low","close","volume","limit_up","limit_down"]
def scale(x, low, high):
    return ((x-low)/(high-low)*100).clip(0,100)

def read_bars(path):
    df = pd.read_csv(path, dtype={"ticker":str})
    missing = set(REQUIRED)-set(df)
    if missing: raise ValueError(f"Missing columns: {missing}")
    df["date"] = pd.to_datetime(df.date, errors="raise").dt.normalize()
    if df.duplicated(["ticker","date"]).any(): raise ValueError("Duplicate daily bars")
    cols = REQUIRED[3:]
    df[cols] = df[cols].apply(pd.to_numeric, errors="raise")
    if (df[["open","high","low","close"]] <= 0).any().any() or (df.volume < 0).any():
        raise ValueError("Invalid OHLCV")
    if (df.high < df[["open","close","low"]].max(axis=1)).any() or (df.low > df[["open","close"]].min(axis=1)).any(): raise ValueError("Invalid price range")
    return df.sort_values(["ticker","date"]).reset_index(drop=True)

def attach_pit(bars, events, prefix):
    """Latest known snapshot per ticker; require first-seen timestamp and source.
    Only snapshots received by 13:30 Taipei time on signal date are visible.
    Each snapshot is a full record, not a patch. No backward fill."""
    if events is None or events.empty: return bars
    ev=events.copy()
    req={"ticker","available_at","source"}
    if not req <= set(ev): raise ValueError(f"{prefix}: missing point-in-time metadata")
    if ev.source.isna().any() or ev.source.astype(str).str.strip().eq("").any(): raise ValueError("Missing source")
    if not ev.available_at.astype(str).str.contains(r"(?:Z|[+-]\d\d:\d\d)$", regex=True).all(): raise ValueError("Timestamp requires timezone")
    ev["known"]=pd.to_datetime(ev.available_at, utc=True)
    ev["ticker"]=ev.ticker.astype(str)
    if ev.duplicated(["ticker","known"]).any(): raise ValueError("Duplicate PIT snapshot")
    ev=ev.drop(columns="available_at").rename(columns={c:prefix+"_"+c for c in ev if c not in ("ticker","known")})
    ev=ev.rename(columns={"known":prefix+"_known"})
    df=bars.copy()
    df["cutoff"]=(df.date+pd.Timedelta(hours=13,minutes=30)).dt.tz_localize("Asia/Taipei").dt.tz_convert("UTC")
    return pd.merge_asof(df.sort_values("cutoff"), ev.sort_values(prefix+"_known"), left_on="cutoff",right_on=prefix+"_known",by="ticker",direction="backward").drop(columns="cutoff").sort_values(["ticker","date"]).reset_index(drop=True)

def features(bars, market, industry=None, news=None, fundamentals=None):
    """market: date, close (index), optional regime from existing system.
    Industry snapshots: sector, exposure [0,1], source.
    News snapshots: novelty, credibility, impact, priced_in [0,1], headline, source.
    Fundamental snapshots: eps_revision (decimal), valuation_percentile [0,1], net_flow (signed), source.
    Inputs must be historically available, not reconstructed with future knowledge."""
    d=bars[~bars.ticker.isin(["0050","00631L","00632R"])].copy()
    for e,p in [(industry,"ind"),(news,"n"),(fundamentals,"f")]: d=attach_pit(d,e,p)
    m=market.copy().sort_values("date")
    m["date"]=pd.to_datetime(m.date).dt.normalize()
    if m.date.duplicated().any(): raise ValueError("Duplicate market date")
    m["market5"]=m.close.pct_change(5)
    if "regime" not in m:
        a=m.close.rolling(20).mean(); b=m.close.rolling(60).mean()
        m["regime"]=np.select([(m.close>a)&(a>b)&(m.market5>.02),(m.close>a)&(a>b),(m.close<a)&(a<b)&(m.market5<-.02),(m.close<a)&(a<b)],["strong_bull","bull","strong_bear","bear"],default="sideways")
        m.loc[b.isna(),"regime"]="unknown"
    d=d.merge(m[["date","market5","regime"]],on="date",how="left").sort_values(["ticker","date"])
    g=d.groupby("ticker",sort=False)
    d["ret1"]=g.close.pct_change()
    d["ret5"]=g.close.pct_change(5)
    d["ret20"]=g.close.pct_change(20)
    d["volume_ratio"]=d.volume/g.volume.transform(lambda x:x.shift().rolling(20).mean()).replace(0,np.nan)
    d["volatility_ratio"]=g.ret1.transform(lambda x:x.rolling(5).std()/x.shift(5).rolling(20).std().replace(0,np.nan))
    d["drawdown"]=d.close/g.close.transform(lambda x:x.rolling(60).max())-1
    d["relative"]=scale(d.ret5-d.market5,-.03,.06)
    d["flow"]=.65*scale(d.volume_ratio,1,3)+.35*scale(d.volatility_ratio,.7,2)
    d["overheated"]=(d.ret5>=.12)|(d.ret1>=.085)|(d.ret20>=.30)
    d["eligible"]=(d.volume>0)&(d.volume_ratio.notna())&(d.regime.isin(REGIMES))&~d.overheated
    for c in ["ind_sector","ind_exposure","n_novelty","n_credibility","n_impact","n_priced_in","f_eps_revision","f_valuation_percentile","f_net_flow"]:
        if c not in d: d[c]=np.nan
    for c in ["ind_source","n_source","n_headline","f_source"]:
        if c not in d: d[c]=None
    sg=d.groupby(["date","ind_sector"],dropna=True)
    count=sg.ret5.transform("count")
    d["peer_ret5"]=(sg.ret5.transform("sum")-d.ret5)/(count-1).replace(0,np.nan)
    d["sector"]=((.6*scale(d.peer_ret5-d.market5,0,.08)+.4*scale(d.peer_ret5-d.ret5,0,.08))*d.ind_exposure).where((count>=3)&(d.ind_exposure>=.15))
    if "n_known" in d:
        cutoff=(d.date+pd.Timedelta(hours=13,minutes=30)).dt.tz_localize("Asia/Taipei").dt.tz_convert("UTC")
        age=(cutoff-d.n_known).dt.total_seconds()/86400
        d["news"]=(100*d.n_novelty*d.n_credibility*d.n_impact*(1-d.n_priced_in)).where((age<=3)&(d.n_credibility>=.7))
    else: d["news"]=np.nan
    d["fundamental"]=(.65*scale(d.f_eps_revision,-.1,.15)+.35*scale(1-d.f_valuation_percentile,0,1))
    d["capital"]=.65*d.flow+.35*d.relative
    d["rotation"]=.65*d.sector+.20*d.flow+.15*d.relative
    d["catalyst"]=.70*d.news+.15*d.flow+.15*d.relative
    d["reversal"]=(.35*scale(-d.drawdown,.12,.35)+.25*d.fundamental+.25*d.relative+.15*scale(d.f_net_flow,-1,1)).where((d.f_eps_revision>=-.05)&(d.drawdown<=-.15)&(d.ret5>0))
    d["coverage"]=sum(d[k].notna()*w for k,w in WEIGHTS.items())
    d["combined"]=sum(d[k].fillna(0)*w for k,w in WEIGHTS.items()).where(d.coverage>=.75)
    d["stage"]=np.select([d.overheated,(d.ret5<-.06)&(d.volume_ratio>1.4),(d.ret5>.07)&(d.capital>=55),d[ENGINES].max(axis=1)>=65],["過熱期","退潮期","發酵期","萌芽期"],default="潛伏期")
    d["confidence"]=None # no calibrated probabilities yet
    d["risk"]="跌破訊號日低點／催化劑失效／資料過期（研究規則，非自動下單）"
    return d

def candidates(df, engine="combined", threshold=65):
    if engine not in ENGINES: raise ValueError("Unknown engine")
    return df[df.eligible & df[engine].ge(threshold) & df.stage.eq("萌芽期")].copy()

def leaderboard(df, engine="combined", threshold=65):
    if df.empty: return []
    cols=["date","ticker","name",engine,"stage","ret1","volume_ratio","coverage","regime","ind_sector","n_headline","n_source","ind_source","f_source","confidence","risk"]
    chosen=candidates(df,engine,threshold)
    if chosen.empty: return []
    chosen=chosen.sort_values(["ticker","date"])
    dates={d:i for i,d in enumerate(sorted(df.date.unique()))}
    chosen["session"]=chosen.date.map(dates)
    start=chosen.groupby("ticker").session.diff().ne(1)
    chosen["first_detected"]=chosen.groupby(["ticker",start.cumsum()]).date.transform("min")
    latest=chosen[chosen.date==df.date.max()].sort_values(engine,ascending=False).copy()
    latest["trigger"]=engine
    return latest[cols+["first_detected","trigger"]].replace({np.nan:None}).to_dict("records")
