# Seed Radar 0.3 — 研究用原型（非原始 V1.0）

獨立於既有新聞爬蟲、Firestore 與 LINE。行情改用台新證券 Nova API（唯讀行情客戶端），
**沒有實盤下單**；現有主程式、排程與新聞通知未修改。
原始 Seed Radar V1.0 和大盤交易引擎尚未取得，因此本分支不能宣稱完成原版合併。

- 盤中即時資料中心、全市場每分鐘萌芽評分與報告：`python -m seed_radar.hub`（見 DATA_PIPELINE.md）
- 盤中四策略（A 資金／B 族群／C 消息／D 反轉）：`seed_radar/intraday.py`；日線版本：`seed_radar/engine.py`
- 台新歷史資料增量下載：`python -m seed_radar.history_taishin`；交易所官方日線：`python -m seed_radar.market_ingest`

## 執行（在 repo 根目錄）
```sh
python -m pip install -r seed_radar/requirements.txt
python -m unittest discover -s tests -v
python -m seed_radar scan --bars bars.csv --market market.csv --industry industry.csv --news news.csv --fundamentals fundamentals.csv
python -m seed_radar backtest --bars bars.csv --market market.csv --engine all
```
可選 --as-of YYYY-MM-DD、--out 路徑。所有輸出為研究用日線，不是即時行情。每次執行獨立 run ID；audit.sqlite 有禁止一般 UPDATE/DELETE 的 trigger，但非防管理者竄改的永久保存。須另行備份。

## 資料契約
- bars.csv: date,ticker,name,open,high,low,close,volume,limit_up,limit_down。成交量為股，價格須未還原且同一基準。須含歷史上市/櫃及下市股票；未取得全市場資料時不能宣稱全市場回測。公司行動、除權息、下市現金結算仍待實作；未結算部位必須揭露。
- market.csv: date,close,regime（選填，僅當日收盤可知狀態）。date 必須涵蓋完整交易日曆；不能用個股交易日代替。regime 使用 strong_bull,bull,sideways,bear,strong_bear。缺省以 20/60 日均線和五日報酬作研究代理，並非原交易引擎。
- industry.csv: ticker,available_at,source,sector,exposure（0–1，實質營收曝險比例）；每筆是完整快照，不可用現在的供應鏈回填過去。
- news.csv: ticker,available_at,source,headline,novelty,credibility,impact,priced_in（四項皆為 0–1）。應由外部資料管線按首次可取得時間、事件去重、來源驗證、正負向影響及實質曝險建立。現在只接受已處理快照，不含新聞爬取/LLM 推演；source 優先原始 URL。
- fundamentals.csv: ticker,available_at,source,eps_revision（小數）、valuation_percentile（0–1）、net_flow（外部標準化 -1 到 1，不是原始買賣超股數）。禁止以現今共識回填歷史。完整快照保留來源與時區，截止時間為當日 13:30 Asia/Taipei。
- 尚未提供來源的欄位不填假數據。缺新聞及基本面時，綜合引擎可拒絕出訊號，仍可單獨研究 capital。

## 初始研究口徑（未驗證假設）
- 四策略各自評分，綜合權重 25/25/20/15/15。規則式分數不是機率；confidence 留空。
- 萌芽：初始分數 65，過熱排除（單日漲幅 8.5%、五日 12%、20日 30% 任一）。族群需至少三檔及營收曝險 15%。
- 飆股代理定義：以訊號日收盤為基準，未來五個交易日任一收盤上漲至少 12%。用股票日作觀測單位，尚未去重為獨立事件。缺完整後續資料剔除並報 censored；precision/recall/FPR 和 FDR 分列。
- 收盤訊號、次交易日開盤買入、第 1/3/5/10/20 個持有交易日收盤出場；漲停開盤拒買、跌停收盤延後賣。整張、前日成交量 0.5% 上限、每側手續費 0.1425%、股票賣出稅 0.3%、每側滑價 0.1%，均可在程式內改版本；不保證可成交。最低手續費、交易所特殊交易規則及盤中逐筆資料尚未建模。
- 初始資金 100 萬、最多十檔、五種市場曝險上限 100/80/50/25/0%；依進場成本估計上限，非強制再平衡。未結算持倉以最近收盤估價，若下市不可視為已實現。
- Walk-forward 訓練 252 日、隔離 20 日、測試 63 日，每折獨立帳戶。門檻選擇只用訓練區間且至少 20 筆；樣本不足採 65。各折報告並保留訂單拒絕、訊號、交易與淨值。不把不同獨立帳戶串成假複利。隨機與動能基準共用成交引擎，但每日候選數與策略不完全相同。
- 0050 基準、跨折連續帳戶、去極端交易後完整淨值/回撤、獨立事件召回率、統計信賴區間尚待完成。報告目前有排除最高 1% 交易的平均報酬。
- 歷史回測不等於模擬交易實盤；尚未有真實市場結果，55–65% 勝率、2:1 盈虧比、<15% 回撤、1000 筆、提前 1–5 日均未驗證。

## 後續整合
1. 取得原始 V1.0、原大盤引擎及資料授權；使用 TWSE/TPEx/MOPS 等免費來源先建歷史日線、歷史成分及公司行動資料庫。所有下載記錄來源、取得時間、校驗碼及缺漏。
2. 將原大盤輸出轉為 market.csv 的 regime；既有新聞資料必須補齊首次可取得時間和實質曝險再餵 news.csv。不可把歷史爬蟲當下擷取時間偽裝為歷史發布時間。
3. 日線資料品質通過後再做五年回測；盤中同時段量比、委買賣、法人逐時資料另接供應商，資料不足顯示不可用。
4. 另建持久模擬帳戶和分鐘掃描服務，新聞 5–15 分鐘輪詢。GitHub Actions 不適合作為可靠每分鐘行情服務。另建 Firestore collection，避免覆寫 stock_news。先人工審核，再啟用通知。
5. 策略權重競爭須另做前向模擬、版本核准與回滾；目前只有門檻訓練與獨立策略比較。禁止自動修改正式規則。
