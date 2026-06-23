# 台股 AI 監控系統

一個完全自動化、無需人工介入的台股新聞監控系統。

## 功能特色

✅ **完全自動化** - GitHub Actions 每日自動執行  
✅ **零成本運營** - 完全免費，沒有任何隱藏成本  
✅ **實時推送** - LINE 第一時間通知關鍵新聞  
✅ **歷史記錄** - Firestore 儲存所有新聞用於回測  

## 系統需要

- GitHub 帳號
- Firebase 帳號
- LINE Official Account

## 快速開始

1. Clone 此專案
2. 設置 GitHub Secrets (6 個環境變數)
3. 推送到 GitHub
4. GitHub Actions 會自動在每天盤前 8:30 和盤後 4:30 執行

## 檔案說明

- `stock_mapping.json` - 股票代碼對照表
- `stock_scraper_v2.py` - 爬蟲和過濾邏輯
- `push_to_firebase.py` - Firebase 推送
- `notify_line.py` - LINE 推送
- `.github/workflows/stock_monitor.yml` - GitHub Actions 自動化

## 需要設置的 Secrets

在 GitHub repo Settings > Secrets 中新增：

- FIREBASE_PROJECT_ID
- FIREBASE_PRIVATE_KEY
- FIREBASE_CLIENT_EMAIL
- LINE_CHANNEL_ACCESS_TOKEN
- LINE_USER_ID
- FINMIND_API_KEY (可選)

## 下一步

詳細設置指南請參考 SETUP_GUIDE.md

祝你交易順利！🚀