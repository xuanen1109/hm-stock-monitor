#!/usr/bin/env python3
"""
台股 AI 監控系統 - Firebase Firestore 推送
功能：將過濾後的新聞推送到 Firebase Firestore
"""

import json
import os
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FirestorePusher:
    def __init__(self):
        """初始化 Firebase 連接"""
        try:
            # 檢查環境變數
            self.project_id = os.getenv("FIREBASE_PROJECT_ID")
            self.private_key = os.getenv("FIREBASE_PRIVATE_KEY")
            self.client_email = os.getenv("FIREBASE_CLIENT_EMAIL")
            
            if not all([self.project_id, self.private_key, self.client_email]):
                logger.warning("⚠️  Firebase 認證資訊不完整，使用本地模式")
                self.firebase = None
                return
            
            # 動態載入 firebase-admin
            try:
                import firebase_admin
                from firebase_admin import credentials, firestore
                
                # 建立認證
                cred_dict = {
                    "type": "service_account",
                    "project_id": self.project_id,
                    "private_key": self.private_key.replace('\\n', '\n'),
                    "client_email": self.client_email
                }
                
                cred = credentials.Certificate(cred_dict)
                firebase_admin.initialize_app(cred)
                
                self.db = firestore.client()
                logger.info("✅ Firebase 連接成功")
                self.firebase = True
                
            except ImportError:
                logger.warning("⚠️  firebase-admin 未安裝，使用本地模式")
                self.firebase = None
        
        except Exception as e:
            logger.error(f"❌ Firebase 初始化失敗: {e}")
            self.firebase = None
    
    def push_news_batch(self, news_list: list) -> bool:
        """
        批量推送新聞到 Firestore
        """
        try:
            if not self.firebase or not self.db:
                logger.warning("⚠️  Firestore 未連接，跳過推送")
                return False
            
            today = datetime.now().strftime("%Y-%m-%d")
            batch = self.db.batch()
            
            # 推送到日期集合
            for i, news in enumerate(news_list, 1):
                doc_ref = self.db.collection("stock_news").document(today).collection("articles").document(f"news_{i:03d}")
                
                batch.set(doc_ref, {
                    "title": news.get("title", ""),
                    "content": news.get("content", ""),
                    "url": news.get("url", ""),
                    "source": news.get("source", ""),
                    "pubDate": news.get("pubDate", ""),
                    "found_stocks": news.get("found_stocks", []),
                    "sentiment_signals": news.get("sentiment_signals", {}),
                    "relevance_score": news.get("relevance_score", 0),
                    "filtered_at": news.get("filtered_at", ""),
                    "pushed_at": datetime.now().isoformat(),
                    "read": False
                })
            
            # 提交批量操作
            batch.commit()
            logger.info(f"✅ 成功推送 {len(news_list)} 條新聞到 Firestore (日期: {today})")
            
            # 更新 latest 集合（保持最新 50 筆）
            self._update_latest_news(news_list)
            
            return True
        
        except Exception as e:
            logger.error(f"❌ Firestore 推送失敗: {e}")
            return False
    
    def _update_latest_news(self, news_list: list):
        """更新 latest 集合，保持最新的 50 筆新聞"""
        try:
            if not self.firebase or not self.db:
                return
            
            # 查詢現有的 latest 新聞
            docs = self.db.collection("stock_news").document("meta").collection("latest").limit(50).stream()
            
            all_news = list(docs) + [{"data": lambda: news} for news in news_list]
            
            # 按時間戳排序（新的在前）
            all_news = sorted(
                all_news,
                key=lambda x: x.get("pushed_at", "") if isinstance(x, dict) else x.get("pushed_at", ""),
                reverse=True
            )[:50]
            
            # 清空並重寫
            batch = self.db.batch()
            
            for i, news in enumerate(all_news, 1):
                if isinstance(news, dict):
                    doc_ref = self.db.collection("stock_news").document("meta").collection("latest").document(f"news_{i:03d}")
                    batch.set(doc_ref, news)
            
            batch.commit()
            logger.info("✅ 更新了 latest 集合")
        
        except Exception as e:
            logger.warning(f"⚠️  更新 latest 失敗: {e}")
    
    def save_summary(self, news_list: list) -> dict:
        """
        建立每日摘要
        """
        try:
            if not self.firebase or not self.db:
                return {}
            
            today = datetime.now().strftime("%Y-%m-%d")
            
            # 計算統計資訊
            all_stocks = {}
            positive_count = 0
            negative_count = 0
            
            for news in news_list:
                # 統計股票出現次數
                for stock in news.get("found_stocks", []):
                    code = stock["code"]
                    if code not in all_stocks:
                        all_stocks[code] = {
                            "name": stock["name"],
                            "count": 0,
                            "importance": stock.get("importance", "")
                        }
                    all_stocks[code]["count"] += 1
                
                # 統計情感
                sentiment = news.get("sentiment_signals", {}).get("overall_sentiment", 0)
                if sentiment > 0:
                    positive_count += 1
                elif sentiment < 0:
                    negative_count += 1
            
            summary = {
                "date": today,
                "total_news": len(news_list),
                "unique_stocks": len(all_stocks),
                "positive_news": positive_count,
                "negative_news": negative_count,
                "top_stocks": sorted(all_stocks.items(), key=lambda x: x[1]["count"], reverse=True)[:10],
                "created_at": datetime.now().isoformat()
            }
            
            # 推送摘要到 Firestore
            self.db.collection("stock_news").document("summaries").collection("daily").document(today).set(summary)
            
            logger.info(f"✅ 已保存每日摘要: {len(news_list)} 條新聞，{len(all_stocks)} 個不同股票")
            
            return summary
        
        except Exception as e:
            logger.error(f"❌ 保存摘要失敗: {e}")
            return {}


def main():
    """主程式"""
    logger.info("🚀 啟動 Firebase 推送")
    
    # 讀取過濾後的新聞
    try:
        with open("filtered_news.json", 'r', encoding='utf-8') as f:
            filtered_news = json.load(f)
        logger.info(f"📖 讀取了 {len(filtered_news)} 條過濾後的新聞")
    except FileNotFoundError:
        logger.error("❌ 找不到 filtered_news.json，請先執行爬蟲")
        return
    
    # 初始化 Firebase
    pusher = FirestorePusher()
    
    # 推送新聞
    if filtered_news:
        success = pusher.push_news_batch(filtered_news)
        
        if success:
            # 生成並保存摘要
            summary = pusher.save_summary(filtered_news)
            
            # 輸出摘要
            print("\n" + "="*60)
            print("📊 每日摘要")
            print("="*60)
            print(f"日期: {summary.get('date', 'N/A')}")
            print(f"總新聞數: {summary.get('total_news', 0)}")
            print(f"涉及股票數: {summary.get('unique_stocks', 0)}")
            print(f"利多新聞: {summary.get('positive_news', 0)}")
            print(f"利空新聞: {summary.get('negative_news', 0)}")
            
            if summary.get('top_stocks'):
                print("\n📈 最常提及的股票:")
                for i, (code, info) in enumerate(summary['top_stocks'][:5], 1):
                    print(f"   {i}. {info['name']} ({code}) - 出現 {info['count']} 次")
    else:
        logger.warning("❌ 沒有過濾到任何新聞")


if __name__ == "__main__":
    main()