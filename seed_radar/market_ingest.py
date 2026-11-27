"""Persistent TWSE/TPEx public EOD ingestion. No trading or credentials.
Use a durable mounted directory for --db. Never commit market data to Git.
Raw responses are immutable by content hash; errors are retried on subsequent runs.
"""
import argparse, datetime as dt, hashlib, json, re, sqlite3, time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo
import pandas as pd

TZ=ZoneInfo("Asia/Taipei")
TWSE="https://www.twse.com.tw/rwd/zh/afterTrading/"
TPEX="https://www.tpex.org.tw/www/zh-tw/afterTrading/otc"
def now(): return dt.datetime.now(TZ)
def open_db(path):
    Path(path).parent.mkdir(parents=True,exist_ok=True)
    c=sqlite3.connect(path,timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript("""
    CREATE TABLE IF NOT EXISTS raw(source TEXT, period TEXT, hash TEXT, received TEXT, url TEXT, body TEXT, PRIMARY KEY(source,period,hash));
    CREATE TABLE IF NOT EXISTS status(source TEXT, period TEXT, state TEXT, checked TEXT, detail TEXT, PRIMARY KEY(source,period));
    CREATE TABLE IF NOT EXISTS bars(date TEXT, exchange TEXT, ticker TEXT, name TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL, next_up REAL, next_down REAL, PRIMARY KEY(date,exchange,ticker));
    CREATE TABLE IF NOT EXISTS market(date TEXT PRIMARY KEY, close REAL NOT NULL);
    """)
    return c
def mark(c,source,period,state,detail=""):
    c.execute("INSERT OR REPLACE INTO status VALUES(?,?,?,?,?)",(source,period,state,now().isoformat(),detail[:500]))
    c.commit()
def request(c,source,period,base,params):
    url=base+"?"+urlencode(params)
    last=None
    for i in range(3):
        try:
            with urlopen(Request(url,headers={"User-Agent":"SeedRadarResearch/0.2","Accept":"application/json"}),timeout=30) as r:
                body=r.read().decode("utf-8-sig")
            obj=json.loads(body)
            if not isinstance(obj,dict): raise ValueError("unexpected JSON")
            c.execute("INSERT OR IGNORE INTO raw VALUES(?,?,?,?,?,?)",(source,period,hashlib.sha256(body.encode()).hexdigest(),now().isoformat(),url,body))
            c.commit()
            time.sleep(3)
            return obj
        except Exception as exc:
            last=exc
            time.sleep(5*(2**i))
    raise RuntimeError(str(last))
def number(v):
    try:
        x=float(str(v).replace(",","").strip())
        return x if x==x and abs(x)!=float("inf") else None
    except (TypeError,ValueError): return None
def iso(value):
    s=str(value).strip()
    if "/" in s or "-" in s:
        p=re.split(r"[/\-]",s)
        if len(p)!=3: raise ValueError(s)
        y,m,d=map(int,p); y+=1911 if y<1911 else 0
    elif len(s) in (7,8):
        y=int(s[:-4]); y+=1911 if len(s)==7 else 0
        m=int(s[-4:-2]); d=int(s[-2:])
    else: raise ValueError(s)
    return dt.date(y,m,d).isoformat()
def tables(obj):
    return obj.get("tables") or [obj]
def pick(row,*keys):
    for key in keys:
        if key in row: return row[key]
    return None
def parse_daily(obj,exchange,day):
    # Verify response date, never silently accept a latest-day snapshot for history.
    returned=obj.get("date") or obj.get("reportDate")
    if returned:
        if iso(returned)!=day: raise ValueError("response date mismatch")
    else:
        evidence=" ".join(str(x.get("title","")) for x in tables(obj))+" "+str(obj.get("title",""))
        d=dt.date.fromisoformat(day)
        roc=d.year-1911
        if not any(s in evidence for s in (f"{roc}年{d.month:02d}月{d.day:02d}日",f"{roc}年{d.month}月{d.day}日",f"{roc}/{d.month:02d}/{d.day:02d}",d.strftime("%Y/%m/%d"),day)):
            raise ValueError("response has no verified session date")
    for t in tables(obj):
        # TPEx field names carry stray spaces (e.g. "收盤 ", "成交股數  "); normalise before matching.
        fs=[str(f).strip() for f in t.get("fields",[])]; data=t.get("data",[])
        if not ({"證券代號","收盤價"}<=set(fs) or {"代號","收盤"}<=set(fs)): continue
        out=[]
        for values in data:
            r=dict(zip(fs,values))
            ticker=str(pick(r,"證券代號","代號")).strip()
            # First research universe: four-digit common-stock codes only.
            if not re.fullmatch(r"[1-9][0-9]{3}",ticker): continue
            prices=[number(pick(r,*k)) for k in [("開盤價","開盤"),("最高價","最高"),("最低價","最低"),("收盤價","收盤")]]
            volume=number(pick(r,"成交股數"))
            if any(v is None or v<=0 for v in prices) or volume is None or volume<0: continue
            o,h,l,cl=prices
            if h<max(o,l,cl) or l>min(o,h,cl): raise ValueError("invalid OHLC")
            out.append((day,exchange,ticker,str(pick(r,"證券名稱","名稱")),o,h,l,cl,volume,number(pick(r,"次日漲停價")),number(pick(r,"次日跌停價"))))
        if len(out)<300 or len({r[2] for r in out})!=len(out): raise ValueError("incomplete/duplicate session")
        return out
    raise ValueError("daily schema not recognized")
def complete(c,source,period):
    return c.execute("SELECT state FROM status WHERE source=? AND period=?",(source,period)).fetchone()==("ok",)
def index_month(c,month,refresh=False):
    key=month.isoformat()
    if not refresh and complete(c,"index",key): return
    try:
        obj=request(c,"index",key,TWSE+"FMTQIK",{"date":month.strftime("%Y%m%d"),"response":"json"})
        out=[]
        for r in obj.get("data",[]):
            day=iso(r[0]); close=number(r[4])
            if day[:7]!=key[:7] or close is None or close<=0: raise ValueError("index date/price mismatch")
            if day<=now().date().isoformat(): out.append((day,close))
        if not out: raise ValueError("empty index month")
        c.executemany("INSERT OR REPLACE INTO market VALUES(?,?)",out)
        mark(c,"index",key,"ok",str(len(out)))
    except Exception as e:
        mark(c,"index",key,"error",str(e)); raise
def daily(c,exchange,day,refresh=False):
    if not refresh and complete(c,exchange,day): return
    d=dt.date.fromisoformat(day)
    try:
        if exchange=="TWSE":
            obj=request(c,exchange,day,TWSE+"MI_INDEX",{"date":d.strftime("%Y%m%d"),"type":"ALLBUT0999","response":"json"})
        else:
            obj=request(c,exchange,day,TPEX,{"date":d.strftime("%Y/%m/%d"),"type":"EW","response":"json"})
        rows=parse_daily(obj,exchange,day)
        with c:
            c.execute("DELETE FROM bars WHERE date=? AND exchange=?",(day,exchange))
            c.executemany("INSERT INTO bars VALUES(?,?,?,?,?,?,?,?,?,?,?)",rows)
        mark(c,exchange,day,"ok",str(len(rows)))
    except Exception as e:
        mark(c,exchange,day,"error",str(e)); raise
def sync(c,start,end):
    failures=[]
    month=start.replace(day=1)
    while month<=end:
        try: index_month(c,month,refresh=month==now().date().replace(day=1))
        except Exception as e: failures.append(("index",month.isoformat(),str(e)))
        month=(month.replace(day=28)+dt.timedelta(days=4)).replace(day=1)
    days=[r[0] for r in c.execute("SELECT date FROM market WHERE date BETWEEN ? AND ? ORDER BY date",(start.isoformat(),end.isoformat()))]
    for day in days:
        for exchange in ("TWSE","TPEX"):
            try: daily(c,exchange,day)
            except Exception as e: failures.append((exchange,day,str(e)))
    if not days: failures.append(("calendar","","no verified sessions"))
    return failures
def export(c,out):
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    m=pd.read_sql_query("SELECT * FROM market ORDER BY date",c)
    b=pd.read_sql_query("SELECT * FROM bars ORDER BY ticker,date",c)
    # Next-session limit prices published by TPEx are used only on the
    # subsequent official market session; never approximate missing TWSE limits.
    next_session=dict(zip(m.date,m.date.shift(-1)))
    limits=b[["exchange","ticker","date","next_up","next_down"]].copy()
    limits["date"]=limits.date.map(next_session)
    limits=limits.dropna(subset=["date"]).rename(columns={"next_up":"limit_up","next_down":"limit_down"})
    b=b.drop(columns=["next_up","next_down"]).merge(limits,on=["exchange","ticker","date"],how="left")
    if b.duplicated(["date","ticker"]).any(): raise ValueError("ticker collision across markets")
    m.to_csv(out/"market.csv",index=False)
    b.to_csv(out/"bars.csv",index=False)
    latest=m.date.max() if len(m) else None
    status=pd.read_sql_query("SELECT * FROM status ORDER BY period,source",c)
    status.to_csv(out/"ingestion_status.csv",index=False)
    report={"latest_index":latest,"latest_by_exchange":b.groupby("exchange").date.max().to_dict(),"rows":len(b),"ticker_count":int(b.ticker.nunique()),"missing_execution_limits":int(b.limit_up.isna().sum()),"failed_requests":int(status.state.eq("error").sum()),"live_order_enabled":False,"realtime":False,"universe":"four-digit research subset; no security-type master yet","coverage_verified":False}
    (out/"coverage.json").write_text(json.dumps(report,ensure_ascii=False,indent=2))
    return report
def main():
    p=argparse.ArgumentParser()
    p.add_argument("--db",required=True);p.add_argument("--out",required=True)
    p.add_argument("--start",type=dt.date.fromisoformat);p.add_argument("--end",type=dt.date.fromisoformat)
    p.add_argument("--once",action="store_true",help="sync latest seven calendar days; retry known gaps separately")
    a=p.parse_args(); n=now()
    # Avoid claiming the in-progress trading day as a closing snapshot.
    end=min(a.end or n.date(),n.date() if n.hour>=16 else n.date()-dt.timedelta(days=1))
    start=a.start or (end-dt.timedelta(days=7) if a.once else end-dt.timedelta(days=365*5+2))
    if start>end: p.error("start after end")
    with open_db(a.db) as c:
        failures=sync(c,start,end)
        print(json.dumps(export(c,a.out),ensure_ascii=False))
        if failures:
            print(json.dumps(failures,ensure_ascii=False));raise SystemExit(2)
if __name__=="__main__": main()
