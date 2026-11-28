"""Read-only Shioaji 1.7+ tick adapter; requires an authorized market-data key.
Persistent local SQLite; no CA, account subscription, or order functions.
Run on a durable cloud host during trading hours. Subscription scope is explicit.
"""
import argparse, datetime as dt, json, os, re, sqlite3, time
from pathlib import Path
from zoneinfo import ZoneInfo
TZ=ZoneInfo("Asia/Taipei")
def now(): return dt.datetime.now(TZ)
def connect(path):
    Path(path).parent.mkdir(parents=True,exist_ok=True)
    c=sqlite3.connect(path,timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript("""
    CREATE TABLE IF NOT EXISTS ticks(exchange TEXT,ticker TEXT,exchange_time TEXT,received TEXT,price REAL,volume_shares INTEGER,sequence TEXT, PRIMARY KEY(exchange,ticker,exchange_time,sequence));
    CREATE TABLE IF NOT EXISTS live_health(id INTEGER PRIMARY KEY CHECK(id=1),checked TEXT NOT NULL,payload TEXT NOT NULL);
    """)
    return c
def health(path,requested,subscribed,reason=""):
    n=now()
    with connect(path) as c:
        rows=c.execute("SELECT ticker,MAX(exchange_time) FROM ticks GROUP BY ticker").fetchall()
        fresh={ticker for ticker,stamp in rows if 0<=(n-dt.datetime.fromisoformat(stamp)).total_seconds()<=90}
        active=sorted(set(subscribed)&fresh)
        missing=sorted(set(requested)-set(active))
        info=dict(checked=n.isoformat(),source="Shioaji authorized tick stream",requested=len(requested),subscribed=len(subscribed),fresh=len(active),fresh_tickers=active,missing_or_stale=missing,session="regular_hours" if n.weekday()<5 and dt.time(9)<=n.time()<dt.time(13,30) else "outside_regular_hours",status="partial_or_stale" if missing or reason else "fresh_ticks_observed",reason=reason,live_order_enabled=False,signal_enabled=False)
        c.execute("INSERT OR REPLACE INTO live_health VALUES(1,?,?)",(n.isoformat(),json.dumps(info,ensure_ascii=False)))
        return info
def main():
    p=argparse.ArgumentParser()
    p.add_argument("--db",required=True)
    p.add_argument("--symbols",required=True,help="File: authorized stock codes, one per line")
    p.add_argument("--max-symbols",type=int,default=20,help="Explicit account subscription cap")
    a=p.parse_args()
    codes=sorted(set(x.strip() for x in Path(a.symbols).read_text().splitlines() if x.strip()))
    if not codes or any(not re.fullmatch(r"[1-9][0-9]{3}",x) for x in codes): p.error("invalid stock list")
    if not 0<len(codes)<=a.max_symbols: p.error("subscription scope exceeds explicit limit")
    subscribed=[]
    health(a.db,codes,subscribed,"not_connected")
    if not os.getenv("SJ_API_KEY") or not os.getenv("SJ_SEC_KEY"):
        raise SystemExit("Missing read-only market-data credentials. No realtime signals.")
    import shioaji as sj
    api=sj.Shioaji(simulation=False)
    import queue
    q=queue.Queue(maxsize=20000)
    overflow=False
    def callback(exchange,tick):
        nonlocal overflow
        if getattr(tick,"simtrade",False) or getattr(tick,"intraday_odd",False): return
        try:
            stamp=tick.datetime
            stamp=stamp.replace(tzinfo=TZ) if stamp.tzinfo is None else stamp.astimezone(TZ)
            received=now()
            if stamp.date()!=received.date() or stamp>received+dt.timedelta(seconds=2): return
            price=float(tick.close)
            if price<=0: return
            seq=getattr(tick,"serial_num",None)
            if seq is None: seq=str(getattr(tick,"total_volume",""))+":"+str(tick.volume)+":"+str(price)
            q.put_nowait((str(exchange),str(tick.code),stamp.isoformat(),received.isoformat(),price,int(tick.volume)*1000,str(seq)))
        except (queue.Full,ValueError,TypeError,AttributeError): overflow=True
    try:
        api.login(api_key=os.environ["SJ_API_KEY"],secret_key=os.environ["SJ_SEC_KEY"],subscribe_trade=False)
        api.set_on_tick_stk_v1_callback(callback)
        for code in codes:
            contract=api.contracts.get(code)
            if contract is None: continue
            api.subscribe(contract,quote_type=sj.QuoteType.Tick)
            subscribed.append(code)
            time.sleep(.1)
        last=0
        while True:
            batch=[]
            try: batch.append(q.get(timeout=1))
            except queue.Empty: pass
            for _ in range(999):
                try: batch.append(q.get_nowait())
                except queue.Empty: break
            if batch:
                with connect(a.db) as c: c.executemany("INSERT OR IGNORE INTO ticks VALUES(?,?,?,?,?,?,?)",batch)
            if time.monotonic()-last>=30:
                print(json.dumps(health(a.db,codes,subscribed,"dropped_or_invalid_tick" if overflow else ""),ensure_ascii=False),flush=True)
                last=time.monotonic()
    finally:
        health(a.db,codes,[],"disconnected")
        try: api.logout()
        except Exception: pass
if __name__=="__main__": main()
