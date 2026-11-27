import datetime as dt
import re
import sqlite3
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from seed_radar import intraday, public_data, report
from seed_radar.market_ingest import parse_daily
from seed_radar.store import TZ

AT = dt.datetime(2026, 11, 27, 11, 0, tzinfo=TZ)


def snap_rows(**over):
    base = dict(date="2026-11-27", minute="11:00", market="TSE", type="EQUITY", open=100., high=103., low=99.,
                last=102., change_pct=2.0, volume_lots=3000., value=3e8,
                last_trade=(AT - dt.timedelta(seconds=5)).isoformat(), received=AT.isoformat())
    rows = []
    for i in range(8):
        r = dict(base, symbol=f"24{i:02d}", name=f"半導體{i}")
        rows.append(r)
    rows[0].update(over)
    rows.append(dict(base, symbol="IX0001", name="加權", type="INDEX", change_pct=0.2))
    rows.append(dict(base, symbol="IX0043", name="櫃買", type="INDEX", market="OTC", change_pct=0.1))
    return pd.DataFrame(rows)


def baseline(symbols):
    return pd.DataFrame(dict(base_date="2026-11-26", prev_close_official=100., avg20_shares=2_000_000., vol5_over_20=1.,
                             ret5_prior=0.01, ret20_prior=0.02, high60=110., sessions=60, dd60=-0.09), index=symbols)


def tickers(symbols):
    return pd.DataFrame(dict(symbol=symbols, exchange="TWSE", name="x", industry="24", is_normal=1))


PROFILE = pd.Series(np.linspace(0.1, 1.0, 271), index=range(271))


class IntradayTests(unittest.TestCase):
    def run_score(self, snap, ann=pd.DataFrame()):
        syms = [s for s in snap.symbol if not s.startswith("IX")]
        return intraday.score(snap, baseline(syms), tickers(syms), pd.DataFrame(), ann, PROFILE, AT)

    def test_same_time_volume_ratio_uses_profile(self):
        d, s = self.run_score(snap_rows())
        frac = intraday.profile_fraction(PROFILE, 120.0)
        self.assertAlmostEqual(s["profile_fraction"], frac)
        self.assertAlmostEqual(d.loc["2400", "vr"], 3_000_000 / (2_000_000 * frac))
        self.assertEqual(d.loc["2400", "stock_name"], "半導體0")

    def test_limit_up_is_overheated_not_seed(self):
        d, _ = self.run_score(snap_rows(change_pct=9.9, last=109.9, high=109.9, volume_lots=20000.))
        self.assertEqual(d.loc["2400", "stage"], "過熱期")

    def test_moderate_move_with_abnormal_volume_is_seed(self):
        d, _ = self.run_score(snap_rows(change_pct=2.5, last=102.5, high=102.6, volume_lots=9000.))
        self.assertGreaterEqual(d.loc["2400", "capital"], 65)
        self.assertEqual(d.loc["2400", "stage"], "萌芽期")

    def test_heavy_volume_at_day_low_is_not_seed(self):
        d, _ = self.run_score(snap_rows(change_pct=2.5, last=99.2, high=103., low=99., volume_lots=9000.))
        self.assertLess(d.loc["2400", "capital"], 65)
        self.assertNotEqual(d.loc["2400", "stage"], "萌芽期")

    def test_stale_quote_flagged(self):
        d, _ = self.run_score(snap_rows(last_trade=(AT - dt.timedelta(seconds=200)).isoformat()))
        self.assertFalse(bool(d.loc["2400", "fresh"]))
        self.assertTrue(bool(d.loc["2401", "fresh"]))

    def test_future_announcement_ignored(self):
        ann = pd.DataFrame([dict(symbol="2400", published=(AT + dt.timedelta(minutes=5)).isoformat(), subject="接獲大額訂單",
                                 source="twse_material", polarity="positive")])
        d, _ = self.run_score(snap_rows(), ann)
        self.assertTrue(pd.isna(d.loc["2400", "catalyst"]))
        ann.loc[0, "published"] = (AT - dt.timedelta(hours=2)).isoformat()
        d, _ = self.run_score(snap_rows(), ann)
        self.assertGreater(d.loc["2400", "catalyst"], 0)

    def test_baseline_has_no_lookahead(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "m.sqlite"
            c = sqlite3.connect(db)
            c.execute("CREATE TABLE bars(date TEXT, exchange TEXT, ticker TEXT, name TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL, next_up REAL, next_down REAL)")
            days = pd.bdate_range("2026-09-01", "2026-11-27")
            c.executemany("INSERT INTO bars VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                          [(x.date().isoformat(), "TWSE", "2330", "t", 1, 1, 1, 100 + i, 1000 + (10**9 if x.date().isoformat() == "2026-11-27" else 0), None, None)
                           for i, x in enumerate(days)])
            c.commit(); c.close()
            b = intraday.daily_baseline(str(db), "2026-11-27")
            self.assertEqual(b.loc["2330", "base_date"], "2026-11-26")
            self.assertLess(b.loc["2330", "avg20_shares"], 10**6)


class HealthTests(unittest.TestCase):
    def hb(self, age=10, markets=("TSE", "OTC"), lag=3, ws=True, p90=0.1):
        t = AT - dt.timedelta(seconds=age)
        return t.isoformat(), dict(snapshot_ok={m: t.isoformat() for m in markets}, snapshot_server_lag_s=lag,
                                   ws_enabled=True, ws_connected=ws, ws_delay_p90_s=p90)

    def test_states(self):
        today = AT.date().isoformat()
        self.assertEqual(report.health(AT, *self.hb(), today)[0], "正常")
        self.assertEqual(report.health(AT, *self.hb(lag=120), today)[0], "延遲")
        self.assertEqual(report.health(AT, *self.hb(markets=("TSE",)), today)[0], "部分缺失")
        self.assertEqual(report.health(AT, *self.hb(ws=False), today)[0], "部分缺失")
        self.assertEqual(report.health(AT, *self.hb(age=400), today)[0], "中斷")
        self.assertEqual(report.health(AT.replace(hour=15), *self.hb(), today)[0], "非交易時段")
        self.assertEqual(report.health(AT, *self.hb(), "2026-11-26")[0], "非交易時段")


class PublicDataTests(unittest.TestCase):
    def test_roc_time_and_polarity(self):
        self.assertEqual(public_data.roc_datetime("1151126", "70003").isoformat(), "2026-11-26T07:00:03+08:00")
        self.assertEqual(public_data.polarity("公告本公司接獲大額訂單"), "positive")
        self.assertEqual(public_data.polarity("公告本公司董事辭世"), "negative")
        self.assertEqual(public_data.polarity("澄清媒體報導營收創新高"), "routine")

    def test_tpex_padded_field_names(self):
        fields = ["代號", "名稱", "收盤 ", "漲跌", "開盤 ", "最高 ", "最低", "成交股數  ", "次日漲停價 ", "次日跌停價"]
        data = [[f"{1000 + i}", "x", "10", "0", "10", "11", "9", "1,000", "11", "9"] for i in range(300)]
        rows = parse_daily({"date": "20261012", "tables": [{"fields": fields, "data": data}]}, "TPEX", "2026-10-12")
        self.assertEqual(len(rows), 300)
        self.assertEqual(rows[0][8], 1000.0)


class LimitTests(unittest.TestCase):
    def test_twse_limits_parsed_and_exported(self):
        from seed_radar import market_ingest as mi
        fields = ["證券代號", "證券名稱", "漲停價", "開盤競價基準", "跌停價", "開盤競價基準", "收盤價"]
        data = [[f"{1100 + i}", "x", "11.00", "10.00", "9.00", "10.00", "10.00"] for i in range(300)] + [["0050", "etf", "1", "1", "1", "1", "1"]]
        orig = mi.request
        mi.request = lambda c, s, p, b, params: {"date": "20261126", "fields": fields, "data": data}
        try:
            with tempfile.TemporaryDirectory() as tmp:
                c = mi.open_db(str(Path(tmp) / "m.sqlite"))
                mi.twse_limits(c, "2026-11-26")
                self.assertEqual(c.execute("SELECT COUNT(*) FROM limits").fetchone()[0], 300)
                c.execute("INSERT INTO market VALUES('2026-11-26', 100)")
                c.execute("INSERT INTO bars VALUES('2026-11-26','TWSE','1100','x',10,10,10,10,1000,NULL,NULL)")
                c.commit()
                mi.export(c, Path(tmp) / "out")
                b = pd.read_csv(Path(tmp) / "out" / "bars.csv")
                self.assertEqual(float(b.limit_up.iloc[0]), 11.0)
                c.close()
        finally:
            mi.request = orig


class DividendTests(unittest.TestCase):
    def test_ex_dividend_drop_is_not_a_loss(self):
        from seed_radar.backtest import simulate
        days = pd.bdate_range("2024-01-01", periods=6)
        close = [100., 100., 90., 90., 90., 90.]  # 第 3 天除息 10 元
        factor = [0.9, 0.9, 1.0, 1.0, 1.0, 1.0]  # 還原收盤 / 原始收盤
        bars = pd.DataFrame(dict(date=days, ticker="T", name="t", open=close, high=close, low=close, close=close,
                                 volume=1e7, limit_up=[x * 1.1 for x in close], limit_down=[x * .9 - .01 for x in close], adj_factor=factor))
        sig = pd.DataFrame(dict(date=[days[0]], ticker=["T"], score=[90], regime=["bull"]))
        trades, _, _, _ = simulate(bars, sig, days, horizon=3, initial=2_000_000)
        self.assertEqual(len(trades), 1)
        self.assertGreater(trades.ret.iloc[0], -0.01)  # 只剩交易成本，不是 -10%


class BenchmarkTests(unittest.TestCase):
    def test_buy_and_hold_costs_and_window(self):
        from seed_radar.backtest import benchmark_metrics
        days = pd.bdate_range("2024-01-01", periods=10)
        bench = pd.DataFrame(dict(date=days, close=np.linspace(100, 110, 10)))
        m = benchmark_metrics(bench, days[2:8])
        entry, last = bench.close.iloc[2], bench.close.iloc[7]
        self.assertAlmostEqual(m["bench_return"], last / entry * (1 - .001425) * (1 - .001425 - .001) - 1)
        self.assertEqual(m["bench_max_drawdown"], 0)


class SafetyTests(unittest.TestCase):
    def test_no_order_functions_in_seed_radar(self):
        pat = re.compile(r"\.(place_order|modify_price|modify_volume|cancel_order|batch_place_order|accounting)\b")
        for f in Path(__file__).resolve().parents[1].joinpath("seed_radar").glob("*.py"):
            self.assertIsNone(pat.search(f.read_text(encoding="utf-8")), f.name)


if __name__ == "__main__":
    unittest.main()
