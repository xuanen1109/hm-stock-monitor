"""TWSE / TPEx OpenAPI 公開免費資料：重大訊息、本益比/股價淨值比、月營收。

每筆都保存原始回應（sha256 去重）與取得時間。重大訊息 OpenAPI 為每日更新
（前一交易日及當日早盤前的公告），不是盤中逐筆即時公告，報告會如實標示。
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import time

import requests

from .store import TZ, iso, now

UA = {"User-Agent": "SeedRadarResearch/0.3 (read-only research)", "Accept": "application/json"}
SOURCES = {
    "twse_material": "https://openapi.twse.com.tw/v1/opendata/t187ap04_L",
    "tpex_material": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O",
    "twse_pe": "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL",
    "tpex_pe": "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis",
    "twse_revenue": "https://openapi.twse.com.tw/v1/opendata/t187ap05_L",
    "tpex_revenue": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O",
}
POSITIVE = ("訂單", "接單", "得標", "簽約", "合約", "漲價", "調漲", "擴產", "擴廠", "新廠", "量產", "上修", "調升",
            "創新高", "成長", "轉盈", "獲利", "策略聯盟", "合作", "認證", "取得專利", "庫藏股", "買回", "授權")
NEGATIVE = ("虧損", "下修", "調降", "減產", "停工", "停產", "火災", "爆炸", "違約", "跳票", "裁罰", "罰鍰", "重編",
            "訴訟", "掏空", "資安事件", "退票", "減損", "處分", "解任", "辭任", "辭職", "辭世", "暫停交易", "停止買賣")
ROUTINE = ("更名", "董事會", "股東會", "除權", "除息", "法說會", "受邀", "澄清", "媒體報導", "代子公司")


def roc_date(s: str) -> dt.date:
    s = str(s).strip()
    return dt.date(int(s[:-4]) + 1911, int(s[-4:-2]), int(s[-2:]))


def roc_datetime(d: str, t: str) -> dt.datetime:
    t = str(t).strip().zfill(6)
    day = roc_date(d)
    return dt.datetime(day.year, day.month, day.day, int(t[:2]), int(t[2:4]), int(t[4:6]), tzinfo=TZ)


def polarity(subject: str) -> str:
    s = re.sub(r"\s+", "", subject or "")
    if any(k in s for k in NEGATIVE):
        return "negative"
    if any(k in s for k in POSITIVE) and not s.startswith("澄清"):
        return "positive"
    if any(k in s for k in ROUTINE):
        return "routine"
    return "unclassified"


def fetch(c, source: str, retries: int = 2):
    url = SOURCES[source]
    last = None
    for i in range(retries + 1):
        try:
            r = requests.get(url, headers=UA, timeout=40)
            r.raise_for_status()
            body = r.content.decode("utf-8-sig")
            data = json.loads(body)
            fetched = iso(now())
            c.execute("INSERT OR IGNORE INTO public_raw VALUES(?,?,?,?,?)",
                      (source, fetched, url, hashlib.sha256(body.encode()).hexdigest(), body))
            c.commit()
            return data, fetched
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(3 * (i + 1))
    raise RuntimeError(f"{source}: {type(last).__name__}: {str(last)[:160]}")


def _f(v):
    try:
        x = float(str(v).replace(",", "").strip())
        return x if x == x else None
    except (TypeError, ValueError):
        return None


def update_announcements(c) -> dict:
    out = {}
    for src, code_key, subj_key in (("twse_material", "公司代號", "主旨 "), ("tpex_material", "SecuritiesCompanyCode", "主旨")):
        try:
            data, fetched = fetch(c, src)
        except RuntimeError as e:
            out[src] = {"ok": False, "error": str(e)}
            continue
        rows = []
        for r in data:
            subject = (r.get(subj_key) or r.get("主旨") or r.get("主旨 ") or "").replace("\r", "").replace("\n", "")
            try:
                pub = roc_datetime(r["發言日期"], r["發言時間"])
            except (KeyError, ValueError):
                continue
            rows.append((str(r.get(code_key, "")).strip(), iso(pub), subject[:300], r.get("符合條款"),
                         r.get("事實發生日"), src, fetched, polarity(subject)))
        c.executemany("INSERT OR IGNORE INTO announcements VALUES(?,?,?,?,?,?,?,?)", rows)
        c.commit()
        latest = max((x[1] for x in rows), default=None)
        out[src] = {"ok": True, "rows": len(rows), "fetched": fetched, "latest_published": latest}
    return out


def update_fundamentals(c) -> dict:
    out, pe, rev = {}, {}, {}
    for src, code, pe_k, pb_k, dy_k in (("twse_pe", "Code", "PEratio", "PBratio", "DividendYield"),
                                         ("tpex_pe", "SecuritiesCompanyCode", "PriceEarningRatio", "PriceBookRatio", "YieldRatio")):
        try:
            data, fetched = fetch(c, src)
            for r in data:
                pe[str(r.get(code)).strip()] = (roc_date(r["Date"]).isoformat(), _f(r.get(pe_k)), _f(r.get(pb_k)), _f(r.get(dy_k)), src, fetched)
            out[src] = {"ok": True, "rows": len(data), "fetched": fetched, "as_of": max(v[0] for v in pe.values()) if pe else None}
        except (RuntimeError, KeyError, ValueError) as e:
            out[src] = {"ok": False, "error": str(e)[:200]}
    for src in ("twse_revenue", "tpex_revenue"):
        try:
            data, fetched = fetch(c, src)
            for r in data:
                ym = str(r.get("資料年月", "")).strip()
                month = f"{int(ym[:-2]) + 1911}-{ym[-2:]}" if len(ym) >= 4 else None
                rev[str(r.get("公司代號")).strip()] = (month, _f(r.get("營業收入-去年同月增減(%)")), _f(r.get("營業收入-上月比較增減(%)")),
                                                    _f(r.get("累計營業收入-前期比較增減(%)")), r.get("產業別"), src, fetched)
            out[src] = {"ok": True, "rows": len(data), "fetched": fetched, "month": max((v[0] for v in rev.values() if v[0]), default=None)}
        except (RuntimeError, KeyError, ValueError) as e:
            out[src] = {"ok": False, "error": str(e)[:200]}
    rows = []
    for sym in set(pe) | set(rev):
        p = pe.get(sym, (None,) * 6)
        v = rev.get(sym, (None,) * 7)
        as_of = p[0] or (v[0] + "-01" if v[0] else None)
        if not as_of:
            continue
        src = "+".join(x for x in (p[4], v[5]) if x)
        fetched = max(x for x in (p[5], v[6]) if x)
        rows.append((sym, as_of, p[1], p[2], p[3], v[0], v[1], v[2], v[3], v[4], src, fetched))
    c.executemany("INSERT OR REPLACE INTO fundamentals VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    c.commit()
    out["rows"] = len(rows)
    return out
