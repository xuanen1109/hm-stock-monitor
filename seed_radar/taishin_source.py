"""台新 Nova SDK 行情來源（唯讀）。

- 只使用 `init_realtime()` 與 `sdk.marketdata`（Fugle 格式 REST / WebSocket）。
- 不呼叫任何下單、改單、帳務或交割函式；本模組沒有任何下單介面。
- 登入沿用本機既有、已驗證成功的登入程式（login hook，需提供 `login() -> (sdk, account)`）。
  帳密、憑證與 token 只留在本機記憶體，不寫入 Git、資料庫、報告或日誌。
"""
from __future__ import annotations

import contextlib
import importlib.util
import io
import os
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path

import requests

HOOK_ENV = "SEED_TAISHIN_LOGIN_HOOK"
SOURCE_NAME = "台新證券 Nova API 行情（Fugle 格式 REST/WebSocket）"
FORBIDDEN = ("place_order", "modify_price", "modify_volume", "cancel_order", "batch_place_order")


class SourceError(RuntimeError):
    def __init__(self, msg: str, status: int | None = None):
        super().__init__(msg)
        self.status = status


class Usage:
    """API 使用量：依端點類別累計呼叫數、錯誤數與往返時間（不含任何憑證）。"""

    def __init__(self):
        self.lock = threading.Lock()
        self.calls = defaultdict(int)
        self.errors = defaultdict(int)
        self.rtt_ms = defaultdict(float)

    def add(self, kind: str, ms: float, ok: bool):
        with self.lock:
            self.calls[kind] += 1
            self.rtt_ms[kind] += ms
            if not ok:
                self.errors[kind] += 1

    def drain(self) -> dict:
        with self.lock:
            out = {k: {"calls": self.calls[k], "errors": self.errors[k],
                       "avg_rtt_ms": round(self.rtt_ms[k] / self.calls[k], 1) if self.calls[k] else None}
                   for k in sorted(self.calls)}
            self.calls.clear(); self.errors.clear(); self.rtt_ms.clear()
        return out


def _load_hook(path: Path):
    root = path.resolve().parent.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    spec = importlib.util.spec_from_file_location("seed_taishin_login", path)
    mod = importlib.util.module_from_spec(spec)
    return mod, spec, root


class TaishinMarketData:
    """台新行情連線。REST 用 requests.Session；WebSocket 由 SDK 建立。"""

    def __init__(self, hook: str | os.PathLike | None = None, min_interval: float = 0.25):
        path = Path(hook or os.environ.get(HOOK_ENV, ""))
        if not str(path) or not path.is_file():
            raise SourceError(f"找不到台新登入程式；請用 --login-hook 或環境變數 {HOOK_ENV} 指向本機登入檔")
        self._hook = path
        self.min_interval = min_interval
        self.usage = Usage()
        self._http = requests.Session()
        self._last = 0.0
        self._lock = threading.Lock()
        self.login_count = 0
        self.login()

    # ------------------------------------------------------------ 登入
    def login(self):
        mod, spec, root = _load_hook(self._hook)
        cwd = os.getcwd()
        buf = io.StringIO()  # 吞掉登入程式可能印出的帳號資訊，避免進入日誌
        try:
            os.chdir(root)
            with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
                spec.loader.exec_module(mod)
                sdk, account = mod.login()
                sdk.init_realtime(account)
        except Exception as e:  # noqa: BLE001
            raise SourceError(f"台新登入失敗：{type(e).__name__}") from None
        finally:
            os.chdir(cwd)
        self._sdk, self._account = sdk, account
        self._bind()
        self.login_count += 1

    def refresh_realtime(self):
        """重新取得行情 token 並建立新的 REST/WebSocket 客戶端（斷線重連用）。"""
        try:
            self._sdk.init_realtime(self._account)
            self._bind()
        except Exception:  # noqa: BLE001
            self.login()

    def _bind(self):
        cfg = self._sdk.marketdata.rest_client.stock.intraday.config
        self._base = cfg["base_url"].rstrip("/")
        self._headers = {"Authorization": f"Bearer {cfg['bearer_token']}"}

    @property
    def host(self) -> str:
        return self._base.split("/")[2]

    def websocket(self):
        return self._sdk.marketdata.websocket_client.stock

    # ------------------------------------------------------------ REST
    def get(self, path: str, kind: str | None = None, **params) -> dict:
        kind = kind or path.split("/")[0]
        with self._lock:
            wait = self.min_interval - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()
        t = time.perf_counter()
        for attempt in (1, 2):
            try:
                r = self._http.get(f"{self._base}/{path}", headers=self._headers, params=params, timeout=20)
            except requests.RequestException as e:
                self.usage.add(kind, (time.perf_counter() - t) * 1000, False)
                raise SourceError(f"{kind} 連線失敗：{type(e).__name__}") from None
            ms = (time.perf_counter() - t) * 1000
            if r.status_code == 401 and attempt == 1:
                self.usage.add(kind, ms, False)
                self.refresh_realtime()
                continue
            ok = r.status_code < 400
            self.usage.add(kind, ms, ok)
            if not ok:
                raise SourceError(f"{kind} HTTP {r.status_code}", r.status_code)
            return r.json()
        raise SourceError(f"{kind} 授權失敗", 401)

    def snapshot(self, market: str) -> dict:
        return self.get(f"snapshot/quotes/{market}", kind="snapshot")

    def tickers(self, exchange: str, type_: str = "EQUITY", **flags) -> dict:
        return self.get("intraday/tickers", kind="tickers", type=type_, exchange=exchange, **flags)

    def quote(self, symbol: str) -> dict:
        return self.get(f"intraday/quote/{symbol}", kind="quote")

    def candles(self, symbol: str, timeframe: str = "1") -> dict:
        return self.get(f"intraday/candles/{symbol}", kind="intraday_candles", timeframe=timeframe)

    def historical(self, symbol: str, start: str, end: str, timeframe: str = "D", adjusted: bool = False) -> dict:
        params = {"from": start, "to": end, "timeframe": timeframe, "fields": "open,high,low,close,volume,turnover,change"}
        if adjusted:
            params["adjusted"] = "true"
        return self.get(f"historical/candles/{symbol}", kind="historical", **params)
