"""萌芽觀察報告：每份報告開頭固定列出資料時間、延遲、來源、覆蓋率與健康狀態（Asia/Taipei）。

健康狀態：正常／延遲／部分缺失／中斷／非交易時段。狀態不是「正常」時，所有候選股標示為
「僅供觀察」，不得作為有效即時交易訊號。
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pandas as pd

from .intraday import ENGINE_ZH, ENGINES, VERSION, invalidation, payload, reasons, risks
from .store import TZ, iso

STATUS = ("正常", "延遲", "部分缺失", "中斷", "非交易時段")


def trading_window(t: dt.datetime) -> bool:
    return t.weekday() < 5 and dt.time(9, 0) <= t.time() <= dt.time(13, 30, 59)


def health(t: dt.datetime, hb_checked: str | None, hb: dict, trade_date: str | None) -> tuple[str, list[str]]:
    notes = []
    if trade_date and trade_date != t.date().isoformat():
        return "非交易時段", [f"行情日期 {trade_date} 不是今天（休市日）"]
    if not trading_window(t):
        return "非交易時段", ["不在 09:00–13:30 一般交易時段"]
    if not hb_checked:
        return "中斷", ["沒有資料中心心跳紀錄"]
    age = (t - dt.datetime.fromisoformat(hb_checked)).total_seconds()
    snaps = hb.get("snapshot_ok", {})
    ok_markets = [m for m, s in snaps.items() if s and (t - dt.datetime.fromisoformat(s)).total_seconds() <= 150]
    if age > 150 or not ok_markets:
        return "中斷", [f"心跳 {age:.0f} 秒前；最近成功快照市場：{ok_markets or '無'}"]
    status = "正常"
    if len(ok_markets) < 2:
        status = "部分缺失"
        notes.append(f"僅 {ok_markets} 市場快照正常")
    if hb.get("ws_enabled") and not hb.get("ws_connected"):
        status = "部分缺失"
        notes.append("WebSocket 逐筆串流未連線（重點股外盤比與即時延遲缺失）")
    lag = hb.get("snapshot_server_lag_s")
    p90 = hb.get("ws_delay_p90_s")
    if status == "正常" and ((lag is not None and lag > 90) or (p90 is not None and p90 > 5)):
        status = "延遲"
        notes.append(f"快照伺服器時間落後 {lag} 秒／逐筆延遲 p90 {p90} 秒")
    return status, notes


def _t(s):
    return s if s else "無"


def build(t: dt.datetime, d: pd.DataFrame, summary: dict, meta: dict, top: int = 20) -> dict:
    status = meta["status"]
    valid = status == "正常"
    cand = d[d.stage.eq("萌芽期")].sort_values("seed", ascending=False)
    rows = []
    for sym, r in cand.head(top).iterrows():
        p = payload(r)
        p.update(symbol=sym, first_detected=meta["first_seen"].get(sym), engines={ENGINE_ZH[k]: p[k] for k in ENGINES if p[k] is not None},
                 trigger_zh=ENGINE_ZH.get(r.trigger), reasons=reasons(r), risks=risks(r, meta.get("market_regime")),
                 invalidation=invalidation(r), catalyst=(r.news_subject or "未偵測到公開催化劑（全球產業新聞尚未接入）"),
                 supply_chain=f"{r.industry_zh}（以交易所產業分類代理，尚無營收曝險資料）",
                 news_source=r.news_source or "無", signal_valid=bool(valid and r.fresh),
                 signal_note="有效即時觀察訊號（規則分數，未驗證）" if valid and r.fresh else "僅供觀察：資料健康狀態或個股行情時間不符")
        rows.append(p)
    hot = sorted(summary["groups"].values(), key=lambda g: -g["heat"])[:8]
    return dict(
        report_generated_at=iso(t), timezone="Asia/Taipei", engine_version=VERSION,
        market_data_trade_time_latest=meta.get("trade_latest"), market_data_trade_time_median=meta.get("trade_median"),
        system_received_at=meta.get("received"), latest_news_published_at=meta.get("news_published"),
        news_fetched_at=meta.get("news_fetched"), fundamentals_as_of=meta.get("fund_as_of"),
        fundamentals_fetched_at=meta.get("fund_fetched"), quote_delay=meta.get("delay"),
        data_sources=meta.get("sources"), coverage=meta.get("coverage"), health_status=status,
        health_notes=meta.get("health_notes"), index_change_pct=summary["index_chg"],
        volume_profile_fraction=summary["profile_fraction"], hot_groups=hot,
        candidates_total=int(len(cand)), candidates=rows, signals_enabled=valid,
        live_order_enabled=False,
        disclaimer="研究用規則分數，尚未經歷史回測驗證，不是獲利機率，也不是投資建議；萌芽引擎不下單。")


def _fmt(v, f="{:.2f}"):
    return "—" if v is None else f.format(v)


def markdown(rep: dict) -> str:
    cov = rep["coverage"] or {}
    dl = rep["quote_delay"] or {}
    L = [f"# 台股萌芽雷達觀察報告　{rep['report_generated_at'][:16].replace('T', ' ')}", "",
         "| 項目 | 內容 |", "|---|---|",
         f"| 報告產生時間 | {rep['report_generated_at']} |",
         f"| 行情實際成交時間 | 最新 {_t(rep['market_data_trade_time_latest'])}；中位數 {_t(rep['market_data_trade_time_median'])} |",
         f"| 系統接收時間 | {_t(rep['system_received_at'])} |",
         f"| 最新新聞／公告發布時間 | {_t(rep['latest_news_published_at'])} |",
         f"| 新聞取得時間 | {_t(rep['news_fetched_at'])} |",
         f"| 基本面資料日期／取得時間 | {_t(rep['fundamentals_as_of'])}／{_t(rep['fundamentals_fetched_at'])} |",
         f"| 行情延遲 | 逐筆成交→接收 p50 {_fmt(dl.get('ws_p50_s'), '{:.3f}')} 秒、p90 {_fmt(dl.get('ws_p90_s'), '{:.3f}')} 秒（{dl.get('ws_samples', 0)} 筆）；快照伺服器時間→接收 {_fmt(dl.get('snapshot_lag_s'), '{:.1f}')} 秒 |",
         f"| 資料來源 | {'；'.join(rep['data_sources'] or [])} |",
         f"| 掃描股票數／市場覆蓋率 | 掃描 {cov.get('scanned')} 檔普通股（上市 {cov.get('scanned_tse')}、上櫃 {cov.get('scanned_otc')}），交易所清單 {cov.get('listed_common')} 檔，覆蓋率 {_fmt(cov.get('coverage_pct'), '{:.1f}')}%；可評分 {cov.get('eligible')} 檔；逐筆串流重點股 {cov.get('ws_symbols')} 檔 |",
         f"| 資料健康狀態 | **{rep['health_status']}** {'；'.join(rep['health_notes'] or [])} |", ""]
    ic = rep["index_change_pct"]
    L.append(f"加權指數 {_fmt(ic.get('TSE'), '{:+.2f}')}%、櫃買指數 {_fmt(ic.get('OTC'), '{:+.2f}')}%。"
             f"量比基準：市場日內累積量比例 {_fmt(rep['volume_profile_fraction'], '{:.1%}')}。")
    L += ["", "## 族群熱度（前 8）", "", "| 族群 | 檔數 | 熱度 | 相對強度 | 上漲比 | 量比中位數 | 領先股 |", "|---|---|---|---|---|---|---|"]
    for g in rep["hot_groups"]:
        L.append(f"| {g['name']} | {g['members']} | {g['heat']} | {g['rel']:+.2f} | {g['breadth']:.0%} | {g['vr']} | {'、'.join(g.get('leader_names') or g['leaders'])} |")
    L += ["", f"## 萌芽候選（共 {rep['candidates_total']} 檔，列前 {len(rep['candidates'])}）", ""]
    if not rep["signals_enabled"]:
        L.append(f"> 資料健康狀態為「{rep['health_status']}」，以下僅供觀察，不是有效即時訊號。")
        L.append("")
    if not rep["candidates"]:
        L.append("目前沒有符合萌芽期條件的股票。")
    for i, c in enumerate(rep["candidates"], 1):
        L += [f"### {i}. {c['stock_name']}（{c['symbol']}）　萌芽分數 {c['seed']:.0f}　{c['stage']}",
              f"- 觸發策略：{c['trigger_zh']}；各策略分數：{'、'.join(f'{k} {v:.0f}' for k, v in c['engines'].items())}",
              f"- 首次偵測：{_t(c['first_detected'])}",
              f"- 最新價 {c['last']:g}（{c['change_pct']:+.2f}%），成交時間 {c['last_trade']}，接收 {c['received']}",
              f"- 同時段異常量比：{_fmt(c['vr'], '{:.2f}')} 倍（{c['vr_method']}）",
              f"- 觸發原因：{'；'.join(c['reasons'])}",
              f"- 潛在催化劑：{c['catalyst']}",
              f"- 相關供應鏈：{c['supply_chain']}",
              f"- 新聞原始來源：{c['news_source']}",
              f"- 潛在利空：{'；'.join(c['risks'])}",
              f"- 訊號失效條件：{'；'.join(c['invalidation'])}",
              f"- 資料完整度：{c['completeness']:.0%}；{c['signal_note']}", ""]
    L += ["---", rep["disclaimer"]]
    return "\n".join(L)


def write(out: Path, rep: dict, archive: bool):
    out.mkdir(parents=True, exist_ok=True)
    body = json.dumps(rep, ensure_ascii=False, indent=1, default=str)
    md = markdown(rep)
    (out / "latest.json").write_text(body, encoding="utf-8")
    (out / "latest.md").write_text(md, encoding="utf-8")
    if archive:
        t = dt.datetime.fromisoformat(rep["report_generated_at"])
        day = out / t.strftime("%Y-%m-%d")
        day.mkdir(exist_ok=True)
        (day / t.strftime("%H%M.md")).write_text(md, encoding="utf-8")
        (day / t.strftime("%H%M.json")).write_text(body, encoding="utf-8")
