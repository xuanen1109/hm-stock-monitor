"""盤中萌芽評分（規則式研究版 0.3，尚未經歷史驗證）。

四個獨立策略，各自 0–100 分；分數是規則分數，不是獲利機率。
A 資金萌芽 capital：同時段異常量比、相對大盤強度、日內位置、（重點股）外盤比。
B 族群萌芽 rotation：產業熱度（相對強度、上漲家數比、量比），找已啟動族群中尚未大漲的落後股。
C 消息萌芽 catalyst：公開資訊觀測站重大訊息（正向關鍵字）且股價尚未反映。
D 反轉萌芽 reversal：自 60 日高點回落、賣壓減弱、營收未明顯惡化、今日資金回流。
目標是題材被廣泛注意前的早期訊號，因此排除已漲停／短線過熱的股票（不追高）。
"""
from __future__ import annotations

import contextlib
import datetime as dt
import json
import math
import sqlite3

import numpy as np
import pandas as pd

from .store import TZ, iso

VERSION = "seed-intraday-0.3.0"
OPEN = dt.time(9, 0)
SESSION_MIN = 270  # 09:00–13:30
STALE_SECONDS = 90
COMMON = r"^[1-9][0-9]{3}$"
INDEX_FOR = {"TSE": "IX0001", "OTC": "IX0043"}
INDUSTRY = {"01": "水泥", "02": "食品", "03": "塑膠", "04": "紡織纖維", "05": "電機機械", "06": "電器電纜", "08": "玻璃陶瓷",
            "09": "造紙", "10": "鋼鐵", "11": "橡膠", "12": "汽車", "14": "建材營造", "15": "航運", "16": "觀光餐旅",
            "17": "金融保險", "18": "貿易百貨", "19": "綜合", "20": "其他", "21": "化學", "22": "生技醫療", "23": "油電燃氣",
            "24": "半導體", "25": "電腦及週邊設備", "26": "光電", "27": "通信網路", "28": "電子零組件", "29": "電子通路",
            "30": "資訊服務", "31": "其他電子", "32": "文化創意", "33": "農業科技", "34": "電子商務", "35": "綠能環保",
            "36": "數位雲端", "37": "運動休閒", "38": "居家生活", "80": "管理股票", "91": "存託憑證"}
STRONG_POSITIVE = ("訂單", "接單", "得標", "漲價", "調漲", "上修", "調升", "擴產", "擴廠", "轉盈", "創新高")
ENGINES = ("capital", "rotation", "catalyst", "reversal")
ENGINE_ZH = {"capital": "A 資金萌芽", "rotation": "B 族群萌芽", "catalyst": "C 消息萌芽", "reversal": "D 反轉萌芽"}


def scale(x, lo, hi):
    return ((pd.to_numeric(x, errors="coerce") - lo) / (hi - lo) * 100).clip(0, 100)


def elapsed_minutes(t: dt.datetime) -> float:
    start = t.replace(hour=9, minute=0, second=0, microsecond=0)
    return min(max((t - start).total_seconds() / 60, 0.0), SESSION_MIN)


def profile_fraction(profile: pd.Series | None, minutes: float) -> float | None:
    """市場平均日內累積量比例（以 1 分 K 估計）。profile index = 分鐘序（0 = 09:00 那根）。"""
    if profile is None or profile.empty:
        return None
    k = int(math.floor(minutes))
    lo = float(profile.get(k - 1, 0.0)) if k >= 1 else 0.0
    hi = float(profile.get(min(k, profile.index.max()), profile.iloc[-1]))
    f = lo + (minutes - k) * (hi - lo)
    return f if f > 0 else None


def daily_baseline(market_db: str, today: str) -> pd.DataFrame:
    """由 TWSE/TPEx 官方收盤資料（market_ingest）計算今日以前的基準；只用 today 以前的資料。"""
    with contextlib.closing(sqlite3.connect(f"file:{market_db}?mode=ro", uri=True, timeout=30)) as c:
        b = pd.read_sql_query("SELECT date, exchange, ticker, close, volume FROM bars WHERE date < ? AND date >= ?",
                              c, params=(today, (dt.date.fromisoformat(today) - dt.timedelta(days=130)).isoformat()))
    if b.empty:
        return pd.DataFrame()
    b = b.sort_values(["ticker", "date"])
    g = b.groupby("ticker")
    last = g.tail(1).set_index("ticker")
    out = pd.DataFrame(index=last.index)
    out["base_date"] = last.date
    out["prev_close_official"] = last.close
    out["avg20_shares"] = g.volume.apply(lambda s: s.tail(20).mean() if len(s) >= 10 else np.nan)
    out["vol5_over_20"] = g.volume.apply(lambda s: s.tail(5).mean() / s.tail(20).mean() if len(s) >= 20 and s.tail(20).mean() > 0 else np.nan)
    out["ret5_prior"] = g.close.apply(lambda s: s.iloc[-1] / s.iloc[-6] - 1 if len(s) >= 6 else np.nan)
    out["ret20_prior"] = g.close.apply(lambda s: s.iloc[-1] / s.iloc[-21] - 1 if len(s) >= 21 else np.nan)
    out["high60"] = g.close.apply(lambda s: s.tail(60).max() if len(s) >= 40 else np.nan)
    out["sessions"] = g.size()
    out["dd60"] = out.prev_close_official / out.high60 - 1
    return out


def latest_snapshot(c: sqlite3.Connection, date: str) -> pd.DataFrame:
    q = """SELECT s.* FROM snap s JOIN (SELECT symbol, MAX(minute) m FROM snap WHERE date=? GROUP BY symbol) x
           ON s.symbol = x.symbol AND s.minute = x.m WHERE s.date = ?"""
    return pd.read_sql_query(q, c, params=(date, date))


def tick_flow(c: sqlite3.Connection, since: str) -> pd.DataFrame:
    """重點股外盤比：成交價 >= 委賣價視為外盤（主動買），<= 委買價視為內盤。"""
    t = pd.read_sql_query("SELECT symbol, price, size, bid, ask FROM ticks WHERE exchange_time >= ?", c, params=(since,))
    if t.empty:
        return pd.DataFrame(columns=["out_ratio", "tick_lots"])
    t["out"] = np.where(t.price >= t.ask, t["size"], 0.0)
    t["inn"] = np.where(t.price <= t.bid, t["size"], 0.0)
    g = t.groupby("symbol")[["out", "inn", "size"]].sum()
    g["out_ratio"] = g.out / (g.out + g.inn).replace(0, np.nan)
    return g.rename(columns={"size": "tick_lots"})[["out_ratio", "tick_lots"]]


def score(snap: pd.DataFrame, base: pd.DataFrame, tickers: pd.DataFrame, fundamentals: pd.DataFrame,
          announcements: pd.DataFrame, profile: pd.Series | None, at: dt.datetime, flow: pd.DataFrame | None = None,
          stock_baseline: pd.Series | None = None) -> tuple[pd.DataFrame, dict]:
    """回傳每檔評分與市場摘要。所有輸入必須是 at 以前已知的資料。"""
    eq = snap[snap.type.eq("EQUITY") & snap.symbol.str.match(COMMON)].copy()
    idx = snap[snap.type.eq("INDEX")].set_index("symbol")
    minutes = elapsed_minutes(at)
    frac = profile_fraction(profile, minutes)
    # 欄位不可叫 name：pandas 列物件的 .name 是索引（股票代號）
    d = eq.set_index("symbol").rename(columns={"name": "stock_name"})
    d = d.join(base, how="left")
    if not tickers.empty:
        d = d.join(tickers.set_index("symbol")[["industry", "is_normal"]], how="left")
    else:
        d["industry"], d["is_normal"] = None, None
    if not fundamentals.empty:
        d = d.join(fundamentals.set_index("symbol")[["pe", "pb", "revenue_yoy", "revenue_month", "industry_name"]], how="left")
    else:
        for k in ("pe", "pb", "revenue_yoy", "revenue_month", "industry_name"):
            d[k] = np.nan
    d["industry_zh"] = d.industry.map(INDUSTRY).fillna(d.get("industry_name")).fillna("未分類")
    d["cum_shares"] = d.volume_lots * 1000
    d["value_avg20"] = d.avg20_shares * d.prev_close_official
    d["vr_method"] = None
    d["vr"] = np.nan
    if stock_baseline is not None and not stock_baseline.empty:
        sb = stock_baseline.reindex(d.index)
        d.loc[sb.notna(), "vr"] = d.volume_lots / sb.where(sb > 0)
        d.loc[sb.notna(), "vr_method"] = "個股同時段歷史均量"
    if frac:
        m = d.vr.isna() & d.avg20_shares.gt(0)
        d.loc[m, "vr"] = d.cum_shares / (d.avg20_shares * frac)
        d.loc[m, "vr_method"] = "20日均量×市場日內量能曲線（近似）"
    trade_t = pd.to_datetime(d.last_trade, errors="coerce", format="ISO8601")
    recv_t = pd.to_datetime(d.received, errors="coerce", format="ISO8601")
    d["quote_age_s"] = (recv_t - trade_t).dt.total_seconds()
    d["fresh"] = d.quote_age_s.between(-5, STALE_SECONDS)
    idx_chg = {m: float(idx.loc[s, "change_pct"]) if s in idx.index else np.nan for m, s in INDEX_FOR.items()}
    d["index_chg"] = d.market.map(idx_chg)
    d["rel"] = d.change_pct - d.index_chg
    rng = (d.high - d.low)
    d["pos"] = np.where(rng > 0, (d["last"] - d.low) / rng.replace(0, np.nan), 0.5)
    d["liquid"] = d.value_avg20.ge(5e6) & d["last"].gt(0)
    d["eligible"] = d.liquid & d.vr.notna() & d.is_normal.fillna(1).astype(bool)
    d["overheated"] = d.change_pct.ge(7) | d.ret5_prior.ge(.12) | d.ret20_prior.ge(.30)
    # A 資金萌芽
    d["capital"] = .50 * scale(d.vr, 1.5, 4) + .30 * scale(d.rel, 0, 4) + .20 * scale(d.pos, .4, 1)
    if flow is not None and not flow.empty:
        d = d.join(flow, how="left")
        has = d.out_ratio.notna() & d.tick_lots.ge(20)
        d.loc[has, "capital"] = .85 * d.loc[has, "capital"] + .15 * scale(d.loc[has, "out_ratio"], .5, .7)
    else:
        d["out_ratio"], d["tick_lots"] = np.nan, np.nan
    d.loc[~(d.vr.ge(1.5) & d.rel.gt(0)), "capital"] = d.capital * .6
    # 爆量但收在日內低檔、或外盤比偏低：是賣壓（出貨）而非資金萌芽，降權
    d.loc[d.pos.lt(.25), "capital"] = d.capital * .6
    d.loc[d.out_ratio.lt(.35) & d.tick_lots.ge(5), "capital"] = d.capital * .8
    # B 族群萌芽
    e = d[d.eligible].copy()
    groups = {}
    for ind, g in e.groupby("industry"):
        if len(g) < 5 or ind in (None, "80", "91"):
            continue
        w = (g.value.fillna(0) + 1)
        g_rel = float(np.average(g.rel.fillna(0), weights=w))
        breadth = float(g.change_pct.gt(0).mean())
        g_vr = float(g.vr.median())
        heat = float(.4 * scale(pd.Series([g_rel]), 0, 2).iloc[0] + .3 * scale(pd.Series([breadth]), .5, .8).iloc[0]
                     + .3 * scale(pd.Series([g_vr]), 1.1, 2).iloc[0])
        leaders = (g.rel * np.log10(g.value.fillna(0) + 10)).sort_values(ascending=False).head(3).index.tolist()
        groups[ind] = dict(industry=ind, name=INDUSTRY.get(ind, ind), members=len(g), rel=round(g_rel, 2),
                           breadth=round(breadth, 2), vr=round(g_vr, 2), heat=round(heat, 1), leaders=leaders,
                           leader_names=[f"{g.loc[s, 'stock_name']}({s})" for s in leaders])
    d["group_heat"] = d.industry.map({k: v["heat"] for k, v in groups.items()})
    d["group_rel"] = d.industry.map({k: v["rel"] for k, v in groups.items()})
    leader_set = {s for v in groups.values() for s in v["leaders"]}
    rot = .55 * d.group_heat + .25 * scale(d.vr, 1.2, 3) + .20 * scale(d.group_rel - d.rel, 0, 3)
    d["rotation"] = rot.where(d.group_heat.ge(50) & ~d.index.isin(leader_set) & d.change_pct.gt(-1))
    # C 消息萌芽（只用 at 以前發布、36 小時內的公告）
    d["catalyst"], d["news_subject"], d["news_published"], d["news_source"], d["negative_news"] = np.nan, None, None, None, None
    if not announcements.empty:
        a = announcements.copy()
        a["pub"] = pd.to_datetime(a.published, format="ISO8601")
        a = a[(a.pub <= at) & (a.pub >= at - dt.timedelta(hours=36))]
        pos = a[a.polarity.eq("positive")].sort_values("pub").groupby("symbol").tail(1).set_index("symbol")
        neg = a[a.polarity.eq("negative")].groupby("symbol").subject.apply(lambda s: "；".join(s.head(2)))
        common = d.index.intersection(pos.index)
        if len(common):
            impact = pos.loc[common, "subject"].apply(lambda s: 80 if any(k in s for k in STRONG_POSITIVE) else 60)
            d.loc[common, "news_subject"] = pos.loc[common, "subject"]
            d.loc[common, "news_published"] = pos.loc[common, "published"]
            d.loc[common, "news_source"] = pos.loc[common, "source"]
            d.loc[common, "catalyst"] = .6 * impact + .2 * scale(d.loc[common, "vr"], 1, 3).fillna(0) + .2 * scale(4 - d.loc[common, "change_pct"], 0, 4)
        nc = d.index.intersection(neg.index)
        d.loc[nc, "negative_news"] = neg.loc[nc]
    # D 反轉萌芽
    cond = (d.dd60.le(-.20) & d.vol5_over_20.le(.9) & d.ret5_prior.le(0) & d.change_pct.gt(.5) & d.vr.ge(1.3)
            & (d.revenue_yoy.ge(-10) | d.revenue_yoy.isna()))
    rev = (.30 * scale(-d.dd60, .2, .45) + .25 * scale(d.vr, 1.3, 3) + .20 * scale(d.rel, 0, 3)
           + .15 * scale(d.revenue_yoy, -10, 30).fillna(30) + .10 * scale(1 - d.vol5_over_20, 0, .4))
    d["reversal"] = rev.where(cond)
    for k in ENGINES:
        d.loc[~d.eligible, k] = np.nan
    d["seed"] = d[list(ENGINES)].max(axis=1)
    d["trigger"] = d[list(ENGINES)].fillna(-1).idxmax(axis=1).where(d.seed.notna())
    d["stage"] = np.select(
        [d.change_pct.ge(9.5) | d.overheated, d.change_pct.le(-3) & d.vr.ge(2), d.change_pct.ge(4) & d.vr.ge(3),
         d.seed.ge(65), d.seed.ge(50)],
        ["過熱期", "退潮期", "發酵期", "萌芽期", "潛伏期"], default="無訊號")
    d["completeness"] = d[["vr", "rel", "industry", "revenue_yoy", "prev_close_official"]].notna().mean(axis=1).round(2)
    summary = dict(minutes=round(minutes, 1), profile_fraction=frac, index_chg=idx_chg, groups=groups,
                   universe=int(len(eq)), liquid=int(d.liquid.sum()), eligible=int(d.eligible.sum()),
                   stale=int((~d.fresh & d.liquid).sum()))
    return d, summary


def reasons(r) -> list[str]:
    out = []
    if pd.notna(r.vr) and r.vr >= 1.5:
        out.append(f"同時段量比 {r.vr:.1f} 倍（{r.vr_method}）")
    if pd.notna(r.rel):
        out.append(f"相對大盤 {r.rel:+.2f} 個百分點")
    if pd.notna(r.out_ratio):
        out.append(f"重點股外盤比 {r.out_ratio:.0%}")
    if pd.notna(r.rotation):
        out.append(f"{r.industry_zh}族群熱度 {r.group_heat:.0f}、族群相對強度 {r.group_rel:+.2f}，本檔尚未大漲（落後補漲候選）")
    if pd.notna(r.catalyst):
        out.append(f"重大訊息：{r.news_subject}")
    if pd.notna(r.reversal):
        yoy = f"{r.revenue_yoy:+.1f}%（{r.revenue_month}）" if pd.notna(r.revenue_yoy) else "無資料"
        out.append(f"距 60 日高點 {r.dd60:.0%}、近 5 日量縮至 20 日均量 {r.vol5_over_20:.0%}、月營收年增 {yoy}")
    return out


def risks(r, market_regime: str | None) -> list[str]:
    out = []
    if r.negative_news:
        out.append(f"同期利空公告：{r.negative_news}")
    if pd.notna(r.pos) and r.pos < .4:
        out.append(f"股價位於今日區間下緣（日內位置 {r.pos:.0%}），買盤延續性不足")
    if pd.notna(r.out_ratio) and r.out_ratio < .45:
        out.append(f"外盤比 {r.out_ratio:.0%} 偏低，賣壓較重")
    if pd.notna(r.ret20_prior) and r.ret20_prior > .2:
        out.append(f"近 20 日已漲 {r.ret20_prior:.0%}")
    if pd.notna(r.index_chg) and r.index_chg < -1:
        out.append(f"大盤今日 {r.index_chg:+.2f}%，系統性風險")
    if market_regime:
        out.append(f"既有大盤引擎狀態：{market_regime}")
    if pd.notna(r.pe) and r.pe > 60:
        out.append(f"本益比 {r.pe:.0f} 倍偏高")
    if not r.fresh:
        out.append("最新成交超過 90 秒，可能為低成交量或行情過期")
    out.append("規則分數尚未經歷史回測驗證，不代表獲利機率")
    return out


def invalidation(r) -> list[str]:
    out = [f"跌破今日低點 {r['low']:g}","同時段量比回落至 1.2 倍以下", "行情超過 90 秒未更新或資料健康狀態非「正常」"]
    if pd.notna(r.catalyst):
        out.append("公司澄清或公告內容被否認")
    if pd.notna(r.rotation):
        out.append("族群熱度跌破 50 或領先股翻黑")
    return out


def payload(r) -> dict:
    keys = ["stock_name", "market", "industry_zh", "last", "change_pct", "high", "low", "volume_lots", "value", "vr", "vr_method",
            "rel", "index_chg", "pos", "out_ratio", "capital", "rotation", "catalyst", "reversal", "seed", "trigger", "stage",
            "fresh", "quote_age_s", "last_trade", "received", "completeness", "pe", "revenue_yoy", "revenue_month",
            "news_subject", "news_published", "news_source", "negative_news", "dd60", "ret5_prior", "ret20_prior"]
    out = {}
    for k in keys:
        v = r.get(k)  # 不用 getattr：Series 方法（如 .last）會遮蔽同名欄位
        if isinstance(v, (float, np.floating)):
            v = None if not np.isfinite(v) else round(float(v), 4)
        elif isinstance(v, (np.integer,)):
            v = int(v)
        elif isinstance(v, (np.bool_,)):
            v = bool(v)
        out[k] = v
    return out


def dumps(x) -> str:
    return json.dumps(x, ensure_ascii=False, default=str)
