# Seed Radar 資料串接（研究分支）

所有資料庫放在 **Git 以外的持久目錄**（例：`<資料目錄>\`），不提交 GitHub。
沒有啟用任何付費服務。萌芽引擎只讀行情、分析與回測，**沒有任何下單、改單或帳務功能**
（`tests/test_intraday.py::SafetyTests` 會檢查 seed_radar 內不得出現下單函式）。

## 0. 台新行情實測結果（2026-11-27 11:41–11:43 Asia/Taipei，盤中）

以既有、已驗證的台新登入程式（本機 login hook，帳密／憑證／token 不進 Git、不寫日誌）登入，
`sdk.init_realtime()` 後取得行情客戶端。行情實際由 Fugle 行情伺服器提供（REST host：api.fugle.tw）。

| 項目 | 結果 |
|---|---|
| 登入 | 成功，0.6 秒；與正在執行的自動交易程序同時登入，未影響其 5 分鐘循環 |
| 即時個股成交價／成交量 | `intraday/quote`：200；2330 最新成交 11:41:45.059，資料更新 11:41:47.496，接收 11:41:48.366 |
| 五檔委買委賣 | quote 內含 bids/asks 各 5 檔；WebSocket `books` 頻道亦可 |
| 一分鐘 K 線 | `intraday/candles?timeframe=1`：當日 09:00 起每分鐘（162 根至 11:41） |
| 大盤及產業指數 | IX0001 加權、IX0043 櫃買、IX0028 半導體等；INDEX 清單上市 182 檔 |
| 全台上市櫃清單 | `intraday/tickers`：TWSE 1,545、TPEx 1,792（含興櫃，需用 market=TSE/OTC 篩） |
| 全市場快照 | `snapshot/quotes/TSE` 1,554 筆、`/OTC` 1,433 筆，各一次呼叫，約 0.26 秒、350 KB |
| 即時訂閱數量 | WebSocket `trades` 逐步訂閱至 1,000 檔，伺服器全數確認、無錯誤（未再往上測） |
| API 頻率 | 15.5 秒內 40 次 quote 全部 200、無 429；未做極限測試，以免影響同帳號的自動交易程序 |
| 行情延遲 | WebSocket 成交時間→本機接收：p50 −0.015 秒、最大 +0.031 秒（1,548 筆）；負值代表本機時鐘比交易所慢約數十毫秒（Windows 時間服務未啟動），實際延遲為次秒級，精確毫秒需先校時。快照伺服器時間→接收約 3–5 秒 |
| 歷史行情權限 | 日 K 可回溯至 2010 年（`historical/candles`）；1 分 K 可取得至少 60–70 日前；`historical/stats` 可用 |

## 1. 盤中即時資料中心 `seed_radar.hub`

```powershell
python -m seed_radar.hub --db <資料目錄>\live.sqlite `
  --market-db <資料目錄>\market.sqlite --out <資料目錄>\reports `
  --history-db <資料目錄>\taishin_history.sqlite `
  --login-hook <本機台新登入程式> --until-close
```

- **全市場掃描**：每分鐘上市＋上櫃快照各 1 次呼叫，涵蓋所有上市櫃普通股（報告列出掃描數／交易所清單數／覆蓋率）。
- **重點股逐筆**：WebSocket 訂閱成交值前 600 檔＋候選股（上限 900），計算外盤比與實際延遲；候選前 30 檔訂閱五檔。
- **斷線重連**：WebSocket 斷線或 75 秒無訊息（含伺服器心跳）→ 重新取得 token 重連，退避 5→300 秒；REST 401 自動重取 token。
- **心跳與健康狀態**：每分鐘寫入 heartbeat；報告分「正常／延遲／部分缺失／中斷／非交易時段」。
  個股最新成交超過 90 秒標示「可能過期」，不產生有效即時訊號；以快照成功時間與 WebSocket 心跳區分低成交量與真正斷線。
- **新聞／公告**：每 10 分鐘抓 TWSE/TPEx OpenAPI 重大訊息（此 API 為每日更新，非盤中逐筆，報告會標示）。
- **基本面**：每日開盤前抓本益比、股價淨值比、殖利率、月營收（保存原始回應與取得時間）。
- **API 使用量**：每分鐘記錄各端點呼叫數、錯誤數、平均往返時間及 WebSocket 訊息數（api_usage）。
- **收盤後**：13:33 後保存當日收盤快照為 eod（台新來源，正式歷史以交易所收盤資料為準），`--until-close` 時結束。
- 所有評分（萌芽／潛伏候選每分鐘，全市場每 15 分鐘）寫入只增不改的 scores；首次偵測寫入 detections；每份報告寫入 reports。

報告：`reports/latest.md`、`reports/latest.json`，每 15 分鐘另存 `reports/YYYY-MM-DD/HHMM.md`。

## 2. 歷史資料

### 2a. 交易所官方日線（無倖存者偏差的全市場橫斷面）
```powershell
python -m seed_radar.market_ingest --db <資料目錄>\market.sqlite --out <資料目錄>\export
python -m seed_radar.market_ingest --db <資料目錄>\market.sqlite --out <資料目錄>\export --once
```
第一行預設約五年；已成功的日期不重抓，失敗日期下次重試，原始 JSON 以 sha256 保存。TPEx 欄位名稱含多餘空白，已正規化（2026-11-27 修正）。

### 2b. 台新還原權值日 K 與 1 分 K（每日增量）
```powershell
python -m seed_radar.history_taishin --db <資料目錄>\taishin_history.sqlite `
  --live-db <資料目錄>\live.sqlite --login-hook <本機台新登入程式>
```
- 還原權值日 K 五年（處理除權息、分割），1 分 K 近 30 日；依 progress 只補缺口。
- 由 1 分 K 建立每檔「同時段平均累積量」（minute_profile），hub 次日起改用個股同時段量比；沒有時退回「20 日均量×市場日內量能曲線」並在報告標示為近似值。
- 預設盤中（08:30–13:35）拒絕執行，避免占用盤中 API 額度；遇 429 自動暫停並放慢。
- 只涵蓋目前上市櫃股票（有倖存者偏差），回測的股票池以 2a 官方日線為準。

## 3. 舊的永豐 Shioaji 原型

`live_feed.py`／`live_report.py`（Shioaji）保留但不再是主要行情來源，盤中資料改由 `hub.py`（台新）提供。

## 4. 尚未完成／限制
- 重大訊息目前只接 OpenAPI 每日檔；盤中即時公告（公開資訊觀測站即時重大訊息）與全球產業新聞尚未接入。
- 供應鏈曝險以交易所產業分類代理，尚無營收曝險比例資料。
- 「雲端永久儲存」尚未設定：資料目前保存在本機 D 槽持久目錄（非雲端）。需使用者選定免費或既有的雲端空間後再同步備份。
- 分數為規則分數，尚未經歷史回測驗證，不是獲利機率。
