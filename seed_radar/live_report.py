"""Timestamped read-only observations from the live_feed database.
Not an intraday signal engine; refuses stale or degraded data.
"""
import argparse
import datetime as dt
import json
import sqlite3
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Taipei")
def age(now, stamp):
    parsed = dt.datetime.fromisoformat(stamp)
    if parsed.tzinfo is None:
        raise ValueError("timezone required")
    return (now - parsed).total_seconds()

def build_report(db, now=None):
    now = (now or dt.datetime.now(TZ)).astimezone(TZ)
    report = dict(report_generated_at=now.isoformat(), timezone="Asia/Taipei",
                  source="Shioaji tick stream", status="UNAVAILABLE",
                  market_data_as_of=None, latest_received_at=None,
                  news_data_as_of=None, fundamental_data_as_of=None,
                  coverage=None, observations=[], signals_enabled=False,
                  live_order_enabled=False,
                  limitations=["No verified full-market coverage or holiday calendar",
                               "No validated intraday volume baseline or scoring",
                               "News and fundamental snapshots not connected"])
    try:
        with sqlite3.connect(Path(db).resolve().as_uri()+"?mode=ro", uri=True, timeout=10) as c:
            c.execute("BEGIN")
            row = c.execute("SELECT checked,payload FROM live_health WHERE id=1").fetchone()
            if not row:
                raise ValueError("No feed heartbeat")
            checked, payload = row
            h = json.loads(payload)
            heartbeat_age = age(now, checked)
            report["feed_health_checked_at"] = checked
            report["feed_health_age_seconds"] = round(heartbeat_age,3)
            report["coverage"] = {k:h.get(k) for k in ("requested","subscribed","fresh","missing_or_stale")}
            if not 0 <= heartbeat_age <= 60:
                report.update(status="STALE",reason="Feed heartbeat expired")
                return report
            if h.get("session") != "regular_hours" or now.weekday() >= 5 or not dt.time(9)<=now.time()<dt.time(13,30):
                report.update(status="OUTSIDE_REGULAR_HOURS",reason="Outside regular session")
                return report
            if h.get("reason") or h.get("status") not in ("fresh_ticks_observed","partial_or_stale"):
                report.update(status="DEGRADED",reason=h.get("reason") or "Unexpected feed state")
                return report
            for code in sorted(set(h.get("fresh_tickers",[]))):
                tick=c.execute("""SELECT exchange,exchange_time,received,price FROM ticks
                    WHERE ticker=? ORDER BY exchange_time DESC,received DESC LIMIT 1""",(code,)).fetchone()
                if not tick:
                    continue
                exchange,stamp,received,price=tick
                tick_age=age(now,stamp)
                receive_age=age(now,received)
                delay=age(dt.datetime.fromisoformat(received),stamp)
                if not (0<=tick_age<=90 and 0<=receive_age<=90 and 0<=delay<=90 and price>0):
                    continue
                report["observations"].append(dict(ticker=code,exchange=exchange,observed_trade_price=price,
                    exchange_time=stamp,received_at=received,quote_age_seconds=round(tick_age,3),
                    exchange_to_receive_seconds=round(delay,3),signal=None))
            obs=report["observations"]
            report["coverage"]["observed_fresh_count"]=len(obs)
            if obs:
                report["market_data_as_of"]=max((r["exchange_time"] for r in obs),key=dt.datetime.fromisoformat)
                report["oldest_observed_trade_at"]=min((r["exchange_time"] for r in obs),key=dt.datetime.fromisoformat)
                report["latest_received_at"]=max((r["received_at"] for r in obs),key=dt.datetime.fromisoformat)
                report["status"]="PARTIAL_OBSERVATIONS" if len(obs)!=h.get("requested") else "OBSERVED_NOT_VALIDATED"
            else:
                report.update(status="STALE",reason="No sufficiently recent trades")
    except (sqlite3.Error,ValueError,TypeError,KeyError,OSError) as exc:
        report.update(status="UNAVAILABLE",reason=str(exc),observations=[],market_data_as_of=None,latest_received_at=None)
    return report

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--db",required=True)
    p.add_argument("--out",type=Path)
    a=p.parse_args()
    result=build_report(a.db)
    body=json.dumps(result,ensure_ascii=False,indent=2)
    if a.out:
        a.out.mkdir(parents=True,exist_ok=True)
        with (a.out/(dt.datetime.now(TZ).strftime("%Y%m%dT%H%M%S%f")+".json")).open("x",encoding="utf-8") as f:
            f.write(body)
    print(body)
    return 0 if result["status"] in ("PARTIAL_OBSERVATIONS","OBSERVED_NOT_VALIDATED") else 2
if __name__=="__main__":
    raise SystemExit(main())
