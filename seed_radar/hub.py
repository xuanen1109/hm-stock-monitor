"""盤中即時資料中心（唯讀，台新行情）。

python -m seed_radar.hub --db D:/seed/live.sqlite --market-db D:/seed/market.sqlite --out D:/seed/reports \
    --login-hook <本機登入程式>

- 每分鐘：上市、上櫃全市場快照（各 1 次呼叫）→ 保存 → 全市場萌芽評分 → 報告。
- WebSocket：重點股逐筆成交（外盤比、實際延遲）與五檔；斷線自動重新取得 token 重連。
- 每 10 分鐘：重大訊息（TWSE/TPEx OpenAPI）。開盤前：股票清單、基本面、市場日內量能曲線。
- 收盤後：保存當日收盤快照為 eod（台新來源），可選擇結束（--until-close）。
- 紀錄 API 使用量、心跳、斷線與錯誤事件。沒有任何下單功能。
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import json
import queue
import sqlite3
import threading
import time
import traceback
from pathlib import Path

import numpy as np
import pandas as pd

from . import intraday, public_data, report, store
from .store import TZ, event, from_us, iso, now
from .taishin_source import SOURCE_NAME, SourceError, TaishinMarketData

MARKETS = ("TSE", "OTC")


class Stream:
    """WebSocket 逐筆成交／五檔。回呼只放進佇列，主執行緒寫入資料庫。"""

    def __init__(self, src: TaishinMarketData):
        self.src = src
        self.q: queue.Queue = queue.Queue(maxsize=300_000)
        self.books: dict = {}
        self.lock = threading.Lock()
        self.connected = False
        self.last_msg = None
        self.delays = collections.deque(maxlen=20_000)
        self.trades = set()
        self.book_syms = set()
        self.errors = collections.deque(maxlen=20)
        self.dropped = 0
        self.ws = None
        self.messages = 0

    def _on(self, raw):
        recv = now()
        try:
            m = json.loads(raw)
        except ValueError:
            return
        self.last_msg = recv
        self.messages += 1
        ev = m.get("event")
        if ev == "data":
            d = m.get("data") or {}
            t = from_us(d.get("time"))
            if m.get("channel") == "trades" and t and d.get("price"):
                self.delays.append((recv, (recv - t).total_seconds()))
                try:
                    self.q.put_nowait((d.get("symbol"), iso(t), iso(recv), d.get("price"), d.get("size"), d.get("volume"),
                                       d.get("bid"), d.get("ask"), d.get("serial")))
                except queue.Full:
                    self.dropped += 1
            elif m.get("channel") == "books" and t:
                with self.lock:
                    self.books[d.get("symbol")] = (iso(t), iso(recv), json.dumps(d.get("bids")), json.dumps(d.get("asks")))
        elif ev == "error":
            self.errors.append((iso(recv), str(m.get("data"))[:200]))

    def _disc(self, *a):
        self.connected = False
        self.errors.append((iso(now()), f"disconnect {a}"[:200]))

    def start(self):
        self.src.refresh_realtime()
        ws = self.src.websocket()
        ws.on("message", self._on)
        ws.on("disconnect", self._disc)
        ws.on("error", lambda e: self.errors.append((iso(now()), f"error {e}"[:200])))
        ws.connect()
        self.ws, self.connected, self.last_msg = ws, True, now()
        old_t, old_b = self.trades, self.book_syms
        self.trades, self.book_syms = set(), set()
        self.subscribe(sorted(old_t), sorted(old_b))

    def subscribe(self, trades, books):
        new_t = [s for s in trades if s not in self.trades]
        new_b = [s for s in books if s not in self.book_syms]
        for i in range(0, len(new_t), 200):
            self.ws.subscribe({"channel": "trades", "symbols": new_t[i:i + 200]})
            time.sleep(.2)
        if new_b:
            self.ws.subscribe({"channel": "books", "symbols": new_b})
        self.trades |= set(new_t)
        self.book_syms |= set(new_b)

    def stop(self):
        try:
            if self.ws:
                self.ws.disconnect()
        except Exception:  # noqa: BLE001
            pass
        self.connected = False

    def flush(self, c) -> int:
        rows = []
        while True:
            try:
                rows.append(self.q.get_nowait())
            except queue.Empty:
                break
        if rows:
            c.executemany("INSERT OR IGNORE INTO ticks VALUES(?,?,?,?,?,?,?,?,?)", rows)
        with self.lock:
            books, self.books = self.books, {}
        if books:
            c.executemany("INSERT OR REPLACE INTO books VALUES(?,?,?,?,?)", [(k, *v) for k, v in books.items()])
        c.commit()
        return len(rows)

    def delay_stats(self, since: dt.datetime) -> dict:
        x = sorted(v for t, v in list(self.delays) if t >= since)
        if not x:
            return {"ws_samples": 0}
        return {"ws_samples": len(x), "ws_p50_s": round(x[len(x) // 2], 3), "ws_p90_s": round(x[int(len(x) * .9)], 3),
                "ws_max_s": round(x[-1], 3)}


class Hub:
    def __init__(self, a):
        self.a = a
        self.c = store.connect(a.db)
        self.out = Path(a.out)
        self.src: TaishinMarketData | None = None
        self.stream: Stream | None = None
        self.snapshot_ok: dict = {}
        self.server_lag = None
        self.trade_date = None
        self.day = None
        self.base = pd.DataFrame()
        self.profile = None
        self.tickers = pd.DataFrame()
        self.fund = pd.DataFrame()
        self.ann = pd.DataFrame()
        self.news_meta = {}
        self.fund_meta = {}
        self.last_news = 0.0
        self.last_focus = 0.0
        self.ws_backoff = 5
        self.ws_next = 0.0
        self.eod_saved = False
        self.last_archive = None
        self.minute_of_last_score = None

    # ------------------------------------------------------------ 連線
    def ensure_source(self):
        delay = 5
        while self.src is None:
            try:
                self.src = TaishinMarketData(self.a.login_hook)
                event(self.c, "INFO", "login", f"台新行情登入成功（{self.src.host}）")
            except SourceError as e:
                event(self.c, "ERROR", "login", str(e))
                time.sleep(delay)
                delay = min(delay * 2, 300)

    # ------------------------------------------------------------ 每日初始化
    def day_init(self, t: dt.datetime):
        today = t.date().isoformat()
        self.day, self.eod_saved = today, False
        rows = []
        # 上市（TSE）與上櫃（OTC）；不含興櫃（ESB）等不在快照中的市場，避免覆蓋率分母失真
        for ex, mk in (("TWSE", "TSE"), ("TPEx", "OTC")):
            try:
                allr = {x["symbol"]: x for x in (self.src.tickers(ex, market=mk).get("data") or [])}
                normal = {x["symbol"] for x in (self.src.tickers(ex, market=mk, isNormal="true").get("data") or [])}
                rows += [(today, s, ex, x.get("name"), x.get("industry"), int(s in normal), iso(now())) for s, x in allr.items()]
            except SourceError as e:
                event(self.c, "ERROR", "tickers", f"{ex}: {e}")
        if rows:
            self.c.executemany("INSERT OR REPLACE INTO tickers VALUES(?,?,?,?,?,?,?)", rows)
            self.c.commit()
        self.tickers = pd.read_sql_query("SELECT symbol, exchange, name, industry, is_normal FROM tickers WHERE date=?", self.c, params=(today,))
        try:
            self.fund_meta = public_data.update_fundamentals(self.c)
        except Exception as e:  # noqa: BLE001
            self.fund_meta = {"error": str(e)[:200]}
            event(self.c, "ERROR", "fundamentals", str(e)[:300])
        self.fund = pd.read_sql_query("""SELECT f.* FROM fundamentals f JOIN (SELECT symbol, MAX(as_of) m FROM fundamentals GROUP BY symbol) x
                                         ON f.symbol=x.symbol AND f.as_of=x.m""", self.c)
        self.refresh_news(force=True)
        try:
            self.base = intraday.daily_baseline(self.a.market_db, today)
        except Exception as e:  # noqa: BLE001
            self.base = pd.DataFrame()
            event(self.c, "ERROR", "baseline", f"{type(e).__name__}: {e}")
        self.profile = self.load_profile(today)
        self.stock_curve = self.load_stock_curve(today)
        event(self.c, "INFO", "day_init", f"清單 {len(self.tickers)}、基準 {len(self.base)}、基本面 {len(self.fund)}、"
              f"量能曲線 {'有' if self.profile is not None else '無'}、個股同時段基準 {0 if self.stock_curve is None else self.stock_curve.shape[1]} 檔")

    def load_stock_curve(self, today: str):
        """history_taishin 建立的個股同時段平均累積量（張），列=分鐘序、欄=股票。只接受 today 以前建立的基準。"""
        if not self.a.history_db or not Path(self.a.history_db).exists():
            return None
        try:
            with sqlite3.connect(f"file:{self.a.history_db}?mode=ro", uri=True) as hc:
                m = pd.read_sql_query("SELECT symbol, minute_index, avg_cum_lots FROM minute_profile WHERE as_of <= ?", hc, params=(today,))
        except sqlite3.Error:
            return None
        return None if m.empty else m.pivot(index="minute_index", columns="symbol", values="avg_cum_lots")

    def stock_baseline(self, t: dt.datetime):
        cur = getattr(self, "stock_curve", None)
        if cur is None:
            return None
        x = intraday.elapsed_minutes(t)
        k = int(x)
        lo = cur.loc[k - 1] if k >= 1 else cur.loc[0] * 0
        hi = cur.loc[min(k, cur.index.max())]
        return lo + (x - k) * (hi - lo)

    def load_profile(self, today: str):
        r = pd.read_sql_query("SELECT minute_index, cum_fraction FROM profile WHERE date=? ORDER BY minute_index", self.c, params=(today,))
        if len(r) > 200:
            return r.set_index("minute_index").cum_fraction
        syms = ["2330", "2317", "2454", "2308", "2382", "2881", "2882", "2891", "2303", "3711", "2412", "1301", "2002", "2603",
                "3008", "2357", "6505", "2886", "3034", "2345", "5274", "3443", "6669", "2379", "1216", "8069", "6488", "5347", "3105", "6147"]
        start = (dt.date.fromisoformat(today) - dt.timedelta(days=14)).isoformat()
        end = (dt.date.fromisoformat(today) - dt.timedelta(days=1)).isoformat()
        curves = []
        for s in syms:
            try:
                data = self.src.historical(s, start, end, timeframe="1").get("data") or []
            except SourceError:
                continue
            df = pd.DataFrame(data)
            if df.empty:
                continue
            df["ts"] = pd.to_datetime(df.date, format="ISO8601")
            df["day"] = df.ts.dt.date
            df["k"] = ((df.ts.dt.hour - 9) * 60 + df.ts.dt.minute).astype(int)
            for _, g in df.groupby("day"):
                v = g.set_index("k").volume.reindex(range(0, 271), fill_value=0).astype(float)
                if v.sum() > 0:
                    curves.append(v.cumsum() / v.sum())
        if len(curves) < 20:
            event(self.c, "WARNING", "profile", f"日內量能曲線樣本不足（{len(curves)}）")
            return None
        prof = pd.concat(curves, axis=1).mean(axis=1)
        self.c.executemany("INSERT OR REPLACE INTO profile VALUES(?,?,?,?)", [(today, int(k), float(v), len(curves)) for k, v in prof.items()])
        self.c.commit()
        return prof

    def reload_baseline_if_changed(self, t: dt.datetime):
        """官方日線資料庫更新（例如補齊缺漏）後重新載入基準；只讀取今日以前的資料。"""
        try:
            p = Path(self.a.market_db)
            m = max(x.stat().st_mtime for x in (p, Path(str(p) + "-wal")) if x.exists())  # WAL 寫入不一定更新主檔時間
        except (OSError, ValueError):
            return
        if getattr(self, "_base_mtime", None) is None:
            self._base_mtime = m
            return
        if m != self._base_mtime and time.monotonic() - getattr(self, "_base_loaded", 0) > 300:
            self._base_mtime, self._base_loaded = m, time.monotonic()
            self.base = intraday.daily_baseline(self.a.market_db, t.date().isoformat())
            event(self.c, "INFO", "baseline", f"官方日線已更新，重新載入基準 {len(self.base)} 檔")

    def refresh_news(self, force=False):
        if not force and time.monotonic() - self.last_news < self.a.news_minutes * 60:
            return
        self.last_news = time.monotonic()
        try:
            self.news_meta = public_data.update_announcements(self.c)
        except Exception as e:  # noqa: BLE001
            self.news_meta = {"error": str(e)[:200]}
        since = iso(now() - dt.timedelta(hours=48))
        self.ann = pd.read_sql_query("SELECT * FROM announcements WHERE published >= ?", self.c, params=(since,))

    # ------------------------------------------------------------ 快照
    def snapshot(self, t: dt.datetime):
        minute = t.strftime("%H:%M")
        for m in MARKETS:
            sent = now()
            try:
                d = self.src.snapshot(m)
            except SourceError as e:
                self.c.execute("INSERT INTO snapshot_runs(market,requested,received,ok,error) VALUES(?,?,?,0,?)", (m, iso(sent), iso(now()), str(e)))
                self.c.commit()
                event(self.c, "ERROR", "snapshot", f"{m}: {e}")
                continue
            recv = now()
            data = d.get("data") or []
            self.trade_date = d.get("date")
            st = d.get("time")
            server = None
            if st and self.trade_date:
                server = dt.datetime.fromisoformat(f"{self.trade_date}T{str(st).zfill(6)[:2]}:{str(st).zfill(6)[2:4]}:{str(st).zfill(6)[4:6]}+08:00")
                self.server_lag = round((recv - server).total_seconds(), 1)
            rows = [(self.trade_date, minute, x.get("symbol"), m, x.get("type"), x.get("name"), x.get("openPrice"), x.get("highPrice"),
                     x.get("lowPrice"), x.get("lastPrice") or x.get("closePrice"), x.get("changePercent"), x.get("tradeVolume"),
                     x.get("tradeValue"), iso(from_us(x.get("lastUpdated"))), iso(recv)) for x in data]
            self.c.executemany("INSERT OR REPLACE INTO snap VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
            self.c.execute("INSERT INTO snapshot_runs(market,trade_date,server_time,requested,received,rows,ok) VALUES(?,?,?,?,?,?,1)",
                           (m, self.trade_date, iso(server), iso(sent), iso(recv), len(rows)))
            self.c.commit()
            if self.trade_date == t.date().isoformat():
                self.snapshot_ok[m] = iso(recv)

    # ------------------------------------------------------------ 串流
    def maintain_stream(self, t: dt.datetime, d: pd.DataFrame | None):
        if not self.a.ws or not report.trading_window(t):
            return
        if self.stream and self.stream.connected and self.stream.last_msg and (t - self.stream.last_msg).total_seconds() > 75:
            event(self.c, "WARNING", "ws", "超過 75 秒沒有任何訊息（含心跳），重新連線")
            self.stream.stop()
        if self.stream is None or not self.stream.connected:
            if time.monotonic() < self.ws_next:
                return
            try:
                if self.stream is None:
                    self.stream = Stream(self.src)
                self.stream.start()
                event(self.c, "INFO", "ws", f"WebSocket 已連線，恢復訂閱 {len(self.stream.trades)} 檔")
                self.ws_backoff = 5
            except Exception as e:  # noqa: BLE001
                event(self.c, "ERROR", "ws", f"連線失敗：{type(e).__name__}: {str(e)[:150]}；{self.ws_backoff} 秒後重試")
                self.ws_next = time.monotonic() + self.ws_backoff
                self.ws_backoff = min(self.ws_backoff * 2, 300)
                return
        if d is not None and time.monotonic() - self.last_focus > 300:
            self.last_focus = time.monotonic()
            liquid = d[d.liquid].copy()
            focus = liquid.sort_values("value", ascending=False).head(self.a.focus).index.tolist()
            cands = liquid[liquid.seed.ge(50)].sort_values("seed", ascending=False).index.tolist()
            want = list(dict.fromkeys(cands[:200] + focus))[: self.a.max_subs]
            books = cands[: self.a.books]
            try:
                self.stream.subscribe(want, books)
            except Exception as e:  # noqa: BLE001
                event(self.c, "ERROR", "ws", f"訂閱失敗：{e}")
                self.stream.connected = False

    # ------------------------------------------------------------ 評分＋報告
    def cycle(self, t: dt.datetime):
        if self.day != t.date().isoformat():
            self.day_init(t)
        self.snapshot(t)
        n_ticks = self.stream.flush(self.c) if self.stream else 0
        self.refresh_news()
        self.reload_baseline_if_changed(t)
        today = t.date().isoformat()
        snap = intraday.latest_snapshot(self.c, today)
        d, summary = None, None
        if not snap.empty:
            flow = intraday.tick_flow(self.c, iso(t - dt.timedelta(minutes=30))) if self.stream else None
            d, summary = intraday.score(snap, self.base, self.tickers, self.fund, self.ann, self.profile, t, flow,
                                        self.stock_baseline(t))
        self.maintain_stream(t, d)
        delay = self.stream.delay_stats(t - dt.timedelta(minutes=1)) if self.stream else {"ws_samples": 0}
        delay["snapshot_lag_s"] = self.server_lag
        hb = dict(snapshot_ok=self.snapshot_ok, snapshot_server_lag_s=self.server_lag, trade_date=self.trade_date,
                  ws_enabled=bool(self.a.ws and report.trading_window(t)), ws_connected=bool(self.stream and self.stream.connected),
                  ws_last_msg=iso(self.stream.last_msg) if self.stream else None, ws_subscribed=len(self.stream.trades) if self.stream else 0,
                  ws_delay_p50_s=delay.get("ws_p50_s"), ws_delay_p90_s=delay.get("ws_p90_s"), ws_dropped=self.stream.dropped if self.stream else 0,
                  ws_errors=list(self.stream.errors)[-3:] if self.stream else [], ticks_written=n_ticks, login_count=self.src.login_count)
        store.set_heartbeat(self.c, hb)
        usage = self.src.usage.drain()
        usage["websocket"] = {"messages": self.stream.messages if self.stream else 0, "subscribed": hb["ws_subscribed"]}
        self.c.execute("INSERT INTO api_usage VALUES(?,?)", (iso(t), json.dumps(usage, ensure_ascii=False)))
        self.c.commit()
        if d is None:
            return
        checked, hbp = store.heartbeat(self.c)
        status, notes = report.health(now(), checked, hbp, self.trade_date)
        first_seen = self.update_detections(d, t, status)
        self.save_scores(d, t)
        rep = report.build(now(), d, summary, self.meta(d, status, notes, delay, first_seen))
        self.c.execute("INSERT OR REPLACE INTO reports VALUES(?,?,?)", (rep["report_generated_at"], status, json.dumps(rep, ensure_ascii=False, default=str)))
        self.c.commit()
        archive = self.last_archive is None or t.minute % 15 == 0 or (t - self.last_archive).total_seconds() >= 900
        report.write(self.out, rep, archive)
        if archive:
            self.last_archive = t
        print(f"{t:%H:%M:%S} {status} 掃描 {rep['coverage']['scanned']} 檔 候選 {rep['candidates_total']} 逐筆 {n_ticks} "
              f"延遲p50 {delay.get('ws_p50_s')}s", flush=True)

    def meta(self, d, status, notes, delay, first_seen) -> dict:
        trade = pd.to_datetime(d.last_trade[d.liquid], errors="coerce", format="ISO8601").dropna()
        listed = self.tickers[self.tickers.symbol.str.match(intraday.COMMON)] if not self.tickers.empty else self.tickers
        news_pub = [v.get("latest_published") for v in self.news_meta.values() if isinstance(v, dict) and v.get("latest_published")]
        news_f = [v.get("fetched") for v in self.news_meta.values() if isinstance(v, dict) and v.get("fetched")]
        fund_as = [v.get("as_of") or v.get("month") for v in self.fund_meta.values() if isinstance(v, dict) and (v.get("as_of") or v.get("month"))]
        fund_f = [v.get("fetched") for v in self.fund_meta.values() if isinstance(v, dict) and v.get("fetched")]
        n_listed = len(listed)
        return dict(status=status, health_notes=notes, delay=delay, first_seen=first_seen,
                    trade_latest=iso(trade.max()) if len(trade) else None, trade_median=iso(trade.median()) if len(trade) else None,
                    received=d.received.max(), news_published=max(news_pub) if news_pub else None, news_fetched=max(news_f) if news_f else None,
                    fund_as_of="、".join(sorted(set(fund_as))) if fund_as else None, fund_fetched=max(fund_f) if fund_f else None,
                    sources=[SOURCE_NAME + "：上市／上櫃全市場快照每分鐘 + 重點股 WebSocket 逐筆",
                             "TWSE/TPEx 官方每日收盤行情（量能基準）", "TWSE/TPEx OpenAPI 重大訊息（每日更新，非盤中即時）",
                             "TWSE/TPEx OpenAPI 本益比、月營收"],
                    coverage=dict(scanned=int(len(d)), scanned_tse=int(d.market.eq("TSE").sum()), scanned_otc=int(d.market.eq("OTC").sum()),
                                  listed_common=n_listed, coverage_pct=round(len(d) / n_listed * 100, 1) if n_listed else None,
                                  eligible=int(d.eligible.sum()), stale_liquid=int((~d.fresh & d.liquid).sum()),
                                  ws_symbols=len(self.stream.trades) if self.stream else 0,
                                  note="普通股（四碼代號）；ETF、權證、債券不在萌芽評分範圍"))

    def update_detections(self, d, t, status) -> dict:
        today, ts = t.date().isoformat(), iso(t)
        sig = d[d.stage.eq("萌芽期") & d.fresh] if status == "正常" else d.iloc[0:0]
        for sym, r in sig.iterrows():
            for k in intraday.ENGINES:
                v = r[k]
                if pd.notna(v) and v >= 65:
                    self.c.execute("""INSERT INTO detections VALUES(?,?,?,?,?,?,?) ON CONFLICT(date,symbol,engine) DO UPDATE SET
                                      last_seen=excluded.last_seen, max_score=MAX(max_score, excluded.max_score)""",
                                   (today, sym, k, ts, ts, float(v), float(r["last"])))
        self.c.commit()
        rows = self.c.execute("SELECT symbol, MIN(first_seen) FROM detections WHERE date=? GROUP BY symbol", (today,)).fetchall()
        return {r[0]: r[1] for r in rows}

    def save_scores(self, d, t):
        """保存所有評分過的萌芽與潛伏候選（每分鐘），全市場橫斷面每 15 分鐘一次；只增不改。"""
        run = iso(t)
        keep = d if t.minute % 15 == 0 else d[d.seed.ge(50)]
        self.c.executemany("INSERT OR IGNORE INTO scores VALUES(?,?,?,?,?)",
                           [(run, t.date().isoformat(), t.strftime("%H:%M"), s, intraday.dumps(intraday.payload(r))) for s, r in keep.iterrows()])
        self.c.commit()

    def save_eod(self, t):
        today = t.date().isoformat()
        rows = self.c.execute("""SELECT s.symbol, s.market, s.name, s.open, s.high, s.low, s.last, s.volume_lots, s.value, s.change_pct
                                 FROM snap s JOIN (SELECT symbol, MAX(minute) m FROM snap WHERE date=? GROUP BY symbol) x
                                 ON s.symbol=x.symbol AND s.minute=x.m WHERE s.date=? AND s.type='EQUITY'""", (today, today)).fetchall()
        self.c.executemany("INSERT OR REPLACE INTO eod VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                           [(today, *r, "taishin_snapshot_after_close", iso(now())) for r in rows])
        self.c.commit()
        self.eod_saved = True
        event(self.c, "INFO", "eod", f"已保存 {today} 收盤快照 {len(rows)} 檔（正式歷史以交易所收盤資料為準）")

    # ------------------------------------------------------------ 主迴圈
    def run(self):
        self.ensure_source()
        while True:
            t = now()
            if t.weekday() >= 5 or t.time() >= dt.time(14, 0) or (self.eod_saved and self.a.until_close):
                break
            if t.time() < dt.time(8, 58):
                time.sleep(min(60, (t.replace(hour=8, minute=58, second=0) - t).total_seconds()))
                continue
            try:
                self.cycle(t)
                if self.trade_date and self.trade_date != t.date().isoformat():
                    event(self.c, "INFO", "calendar", f"今日不是交易日（行情日期 {self.trade_date}）")
                    break
                if t.time() >= dt.time(13, 33) and not self.eod_saved:
                    self.save_eod(t)
            except Exception as e:  # noqa: BLE001
                event(self.c, "ERROR", "cycle", f"{type(e).__name__}: {e} | {traceback.format_exc()[-400:]}")
                print("cycle error", type(e).__name__, e, flush=True)
            if self.a.once:
                break
            nxt = (now() + dt.timedelta(minutes=1)).replace(second=3, microsecond=0)
            while now() < nxt:
                if self.stream:
                    self.stream.flush(self.c)
                time.sleep(min(5, max(0.1, (nxt - now()).total_seconds())))
        if self.stream:
            self.stream.flush(self.c)
            self.stream.stop()


def main():
    p = argparse.ArgumentParser(description="台股萌芽雷達盤中資料中心（唯讀，無下單）")
    p.add_argument("--db", required=True)
    p.add_argument("--market-db", required=True, help="market_ingest 產生的官方日線 SQLite")
    p.add_argument("--out", required=True, help="報告輸出資料夾")
    p.add_argument("--login-hook", help="本機台新登入程式（不在 Git）")
    p.add_argument("--history-db", help="history_taishin 的 SQLite（提供個股同時段量能基準）")
    p.add_argument("--focus", type=int, default=600, help="WebSocket 重點股（依成交值）")
    p.add_argument("--max-subs", type=int, default=900, help="WebSocket 訂閱上限（實測 1000 檔可訂閱）")
    p.add_argument("--books", type=int, default=30)
    p.add_argument("--news-minutes", type=int, default=10)
    p.add_argument("--no-ws", dest="ws", action="store_false")
    p.add_argument("--until-close", action="store_true")
    p.add_argument("--once", action="store_true")
    a = p.parse_args()
    Hub(a).run()
    import os
    os._exit(0)  # fugle WebSocket 執行緒非 daemon，確保程序結束


if __name__ == "__main__":
    main()
