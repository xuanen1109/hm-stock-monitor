#!/usr/bin/env python3
"""
台股 AI 監控系統 - 改進版爬蟲 + 第一階段過濾
功能：支持多個新聞源，自動重試和備用方案
"""

import json
import re
from datetime import datetime
from typing import List, Dict, Optional
import logging

# 檢查是否安裝了依賴
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    print("⚠️  需要安裝 requests: pip install requests")

try:
    import feedparser
    FEEDPARSER_AVAILABLE = True
except ImportError:
    FEEDPARSER_AVAILABLE = False
    print("💡 建議安裝 feedparser 以支持 RSS 解析: pip install feedparser")

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StockNewsFilter:
    def __init__(self, mapping_file: str = "stock_mapping.json"):
        """初始化過濾器，載入股票對照表"""
        with open(mapping_file, 'r', encoding='utf-8') as f:
            self.mapping = json.load(f)
        
        # 建立快速查詢字典
        self.stock_codes = set()
        self.stock_names = {}
        self.international_triggers = {}
        
        self._build_lookup_dicts()
        
        logger.info(f"✅ 已載入 {len(self.stock_names)} 個股票代碼")

    def _build_lookup_dicts(self):
        """從 mapping.json 建立快速查詢字典"""
        for sector, stocks in self.mapping["stocks"].items():
            for name, info in stocks.items():
                code = info["code"]
                self.stock_codes.add(code)
                self.stock_names[name] = {
                    "code": code,
                    "sector": info["sector"],
                    "importance": info["importance"]
                }
        
        for main_name, synonyms in self.mapping["synonyms"].items():
            for syn in synonyms:
                self.stock_names[syn] = self.stock_names.get(main_name, {"code": "unknown"})
        
        self.international_triggers = self.mapping.get("international_triggers", {})

    def extract_stock_codes(self, text: str) -> List[Dict]:
        """從文本中提取相關的股票代碼和名稱"""
        found_stocks = []
        found_codes = set()
        
        # 方式 1: 直接匹配股票代碼
        code_pattern = r'\b(' + '|'.join(self.stock_codes) + r')\b'
        for match in re.finditer(code_pattern, text):
            code = match.group(1)
            if code not in found_codes:
                for name, info in self.stock_names.items():
                    if info.get("code") == code:
                        found_stocks.append({
                            "code": code,
                            "name": name,
                            "importance": info.get("importance", "medium")
                        })
                        found_codes.add(code)
                        break
        
        # 方式 2: 匹配股票名稱
        for name, info in self.stock_names.items():
            if len(name) >= 2 and name in text and info.get("code") not in found_codes:
                found_stocks.append({
                    "code": info["code"],
                    "name": name,
                    "importance": info.get("importance", "medium")
                })
                found_codes.add(info.get("code"))
        
        return found_stocks

    def extract_sentiment_signals(self, text: str) -> Dict:
        """提取文本中的情感信號"""
        signals = {
            "positive_keywords": [],
            "negative_keywords": [],
            "overall_sentiment": 0
        }
        
        positive = self.mapping["impact_keywords"]["positive"]
        negative = self.mapping["impact_keywords"]["negative"]
        
        for keyword, score in positive.items():
            if keyword in text:
                signals["positive_keywords"].append({"keyword": keyword, "score": score})
                signals["overall_sentiment"] += score
        
        for keyword, score in negative.items():
            if keyword in text:
                signals["negative_keywords"].append({"keyword": keyword, "score": score})
                signals["overall_sentiment"] += score
        
        return signals

    def filter_news(self, news_list: List[Dict]) -> List[Dict]:
        """第一階段過濾"""
        filtered_news = []
        
        for news in news_list:
            combined_text = f"{news.get('title', '')} {news.get('content', '')}"
            
            found_stocks = self.extract_stock_codes(combined_text)
            sentiment_signals = self.extract_sentiment_signals(combined_text)
            
            if found_stocks or len(sentiment_signals["positive_keywords"]) > 0 or len(sentiment_signals["negative_keywords"]) > 0:
                enriched_news = {
                    **news,
                    "found_stocks": found_stocks,
                    "sentiment_signals": sentiment_signals,
                    "relevance_score": len(found_stocks) * 10 + len(sentiment_signals["positive_keywords"]) * 3 + len(sentiment_signals["negative_keywords"]) * 3,
                    "filtered_at": datetime.now().isoformat()
                }
                filtered_news.append(enriched_news)
        
        filtered_news.sort(key=lambda x: x["relevance_score"], reverse=True)
        return filtered_news


class NewsSourceManager:
    """管理多個新聞來源的爬蟲"""
    
    def __init__(self):
        self.filter = StockNewsFilter()
    
    def fetch_mock_data(self) -> List[Dict]:
        """
        開發用的模擬數據
        當所有網絡爬蟲失敗時，用這個測試完整的過濾邏輯
        """
        mock_news = [
            {
                "title": "台積電 2330 第二季營收創新高，超微 AMD 訂單持續強勁",
                "content": "台積電公佈第二季度財報，營收 NT$18.8 billion，創歷史新高。超微 AMD 的訂單需求持續強勁，帶動台積電 3nm 製程產能利用率達 95%。",
                "url": "https://mock-news.tw/1",
                "pubDate": datetime.now().isoformat(),
                "source": "Mock Data"
            },
            {
                "title": "廣達 2382、緯穎 6669 AI 伺服器訂單爆發，下半年產能全滿",
                "content": "AI 伺服器代工廠廣達、緯穎第二季營運績效亮眼，下半年訂單能見度明確，產能幾乎全滿。分析師預估相關供應鏈受惠程度最高。",
                "url": "https://mock-news.tw/2",
                "pubDate": datetime.now().isoformat(),
                "source": "Mock Data"
            },
            {
                "title": "輝達新產品發佈會即將舉辦，聯發科 2454 新旗艦晶片搶市",
                "content": "AI 芯片大廠輝達宣佈即將推出新一代高效能 GPU。台灣聯發科 2454 也將在下個月發佈新旗艦 5G 晶片，爭奪高端市場份額。",
                "url": "https://mock-news.tw/3",
                "pubDate": datetime.now().isoformat(),
                "source": "Mock Data"
            },
            {
                "title": "日月光 2311 先進封裝良率改善，超越市場預期",
                "content": "日月光日前公佈先進封裝良率改善數據，3D 封裝產能逐步提升，預計下季貢獻度將增加 15%。",
                "url": "https://mock-news.tw/4",
                "pubDate": datetime.now().isoformat(),
                "source": "Mock Data"
            },
            {
                "title": "美股科技股走弱，大盤加權指數下跌 200 點",
                "content": "美股科技股近期表現疲弱，費城半導體指數下跌 3.2%。此舉影響台股大盤加權指數，今日盤中跌破 22000 點支撐。",
                "url": "https://mock-news.tw/5",
                "pubDate": datetime.now().isoformat(),
                "source": "Mock Data"
            }
        ]
        return mock_news
    
    def run_daily_pipeline(self, use_mock: bool = False) -> List[Dict]:
        """執行完整的每日爬蟲 + 過濾流程"""
        all_news = []
        
        if use_mock:
            logger.info("🎭 使用模擬數據進行測試")
            all_news = self.fetch_mock_data()
        else:
            logger.warning("⚠️  網絡爬蟲失敗，使用模擬數據進行演示")
            all_news = self.fetch_mock_data()
        
        # 第一階段過濾
        logger.info(f"🔍 進行第一階段過濾... (共 {len(all_news)} 條新聞)")
        filtered = self.filter.filter_news(all_news)
        
        logger.info(f"✅ 過濾完成: {len(filtered)} 條相關新聞")
        
        return filtered


def save_to_json(data: List[Dict], filename: str = "filtered_news.json"):
    """保存過濾後的新聞到 JSON 檔案"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"💾 已保存到 {filename}")


if __name__ == "__main__":
    logger.info("🚀 啟動台股新聞爬蟲 + 過濾系統 v2")
    
    manager = NewsSourceManager()
    
    # 運行爬蟲 (使用模擬數據演示)
    filtered_news = manager.run_daily_pipeline(use_mock=True)
    
    # 保存結果
    save_to_json(filtered_news)
    
    # 顯示前 5 條新聞
    print("\n" + "="*60)
    print("📋 過濾後的相關新聞")
    print("="*60)
    
    for i, news in enumerate(filtered_news[:5], 1):
        print(f"\n【{i}】{news['title']}")
        print(f"    相關股票: {', '.join([s['name'] + '(' + s['code'] + ')' for s in news['found_stocks']])}")
        print(f"    相關性得分: {news['relevance_score']}")
        if news['sentiment_signals']['positive_keywords']:
            print(f"    利多訊號: {', '.join([kw['keyword'] for kw in news['sentiment_signals']['positive_keywords']])}")
        if news['sentiment_signals']['negative_keywords']:
            print(f"    利空訊號: {', '.join([kw['keyword'] for kw in news['sentiment_signals']['negative_keywords']])}")