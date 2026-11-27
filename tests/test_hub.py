import datetime as dt
import json
import tempfile
import types
import unittest
from pathlib import Path

from seed_radar import hub, store
from seed_radar.store import TZ


class FakeWS:
    def __init__(self, log):
        self.log, self.handlers = log, {}

    def on(self, ev, fn):
        self.handlers[ev] = fn

    def connect(self):
        self.log.append("connect")

    def subscribe(self, p):
        self.log.append(("sub", p["channel"], tuple(p["symbols"])))

    def disconnect(self):
        self.log.append("disconnect")


class FakeSrc:
    def __init__(self):
        self.log, self.refreshed = [], 0

    def refresh_realtime(self):
        self.refreshed += 1

    def websocket(self):
        return FakeWS(self.log)


class StreamTests(unittest.TestCase):
    def test_reconnect_resubscribes_previous_symbols(self):
        src = FakeSrc()
        s = hub.Stream(src)
        s.start()
        s.subscribe(["2330", "2317"], ["2330"])
        s.connected = False  # 模擬斷線
        s.start()
        self.assertEqual(src.refreshed, 2)  # 重連時重新取得 token
        subs = [x for x in src.log if isinstance(x, tuple)]
        self.assertIn(("sub", "trades", ("2317", "2330")), subs[2:])
        self.assertEqual(s.trades, {"2330", "2317"})

    def test_trade_message_queued_with_delay(self):
        s = hub.Stream(FakeSrc())
        t = dt.datetime.now(TZ) - dt.timedelta(milliseconds=200)
        s._on(json.dumps({"event": "data", "channel": "trades", "data": {"symbol": "2330", "price": 100, "size": 1,
                                                                          "volume": 10, "bid": 99.5, "ask": 100, "serial": 7,
                                                                          "time": int(t.timestamp() * 1e6)}}))
        self.assertEqual(s.q.qsize(), 1)
        self.assertGreater(s.delays[-1][1], 0.1)
        with tempfile.TemporaryDirectory() as tmp:
            c = store.connect(Path(tmp) / "x.sqlite")
            self.assertEqual(s.flush(c), 1)
            self.assertEqual(c.execute("SELECT COUNT(*) FROM ticks").fetchone()[0], 1)
            c.close()

    def test_silent_stream_triggers_reconnect(self):
        a = types.SimpleNamespace(ws=True, focus=10, max_subs=10, books=2, db=":memory:", out=".")
        h = hub.Hub.__new__(hub.Hub)
        h.a, h.c = a, store.connect(Path(tempfile.mkdtemp()) / "y.sqlite")
        src = FakeSrc()
        h.src, h.stream, h.ws_next, h.ws_backoff, h.last_focus = src, hub.Stream(src), 0.0, 5, 1e18
        h.stream.start()
        t = dt.datetime(2026, 11, 27, 10, 0, tzinfo=TZ)
        h.stream.last_msg = t - dt.timedelta(seconds=120)
        h.maintain_stream(t, None)
        self.assertIn("disconnect", src.log)
        self.assertEqual(src.refreshed, 2)
        self.assertTrue(h.stream.connected)
        h.c.close()


if __name__ == "__main__":
    unittest.main()
