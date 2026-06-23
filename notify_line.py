#!/usr/bin/env python3
"""
台股 AI 監控系統 - LINE Messaging API 推送
功能：每日將重點新聞推送到 LINE
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


class LINENotifier:
    def __init__(self):
        """初始化 LINE 連接"""
        self.channel_access_token = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
        self.user_id = os.getenv("LINE_USER_ID")
        
        if not self.channel_access_token or not self.user_id:
            logger.warning("⚠️  LINE 認證資訊不完整")
            self.line = None
            return
        
        try:
            # 動態載入 line-bot-sdk
            from linebot import LineBotApi
            from linebot.models import TextSendMessage, FlexSendMessage
            
            self.line_bot_api = LineBotApi(self.channel_access_token)
            self.TextSendMessage = TextSendMessage
            self.FlexSendMessage = FlexSendMessage
            self.line = True
            
            logger.info("✅ LINE 連接成功")
        
        except ImportError:
            logger.warning("⚠️  line-bot-sdk 未安裝: pip install line-bot-sdk")
            self.line = None
        except Exception as e:
            logger.error(f"❌ LINE 初始化失敗: {e}")
            self.line = None
    
    def send_daily_summary(self, news_list: list, summary: dict) -> bool:
        """
        發送每日摘要
        """
        try:
            if not self.line or not self.line_bot_api:
                logger.warning("⚠️  LINE 未連接，跳過推送")
                return False
            
            # 格式化摘要訊息
            date = summary.get('date', 'N/A')
            total = summary.get('total_news', 0)
            positive = summary.get('positive_news', 0)
            negative = summary.get('negative_news', 0)
            
            # 計算淨情感
            net_sentiment = positive - negative
            sentiment_emoji = "📈" if net_sentiment > 0 else "📉" if net_sentiment < 0 else "➡️"
            
            message = f"""【台股 AI 監控 - 每日摘要】
{date}

📊 新聞統計
- 總篇數: {total} 篇
- 利多新聞: {positive} 篇 ✅
- 利空新聞: {negative} 篇 ❌
- 淨情感: {sentiment_emoji} ({net_sentiment:+d})

涉及個股: {summary.get('unique_stocks', 0)} 檔

📈 最常提及的股票:
"""
            
            for i, (code, info) in enumerate(summary.get('top_stocks', [])[:5], 1):
                message += f"{i}. {info['name']} ({code}) - 提及 {info['count']} 次\n"
            
            message += "\n💡 詳細新聞請查看儀表板: [連結待補充]"
            
            # 發送訊息
            self.line_bot_api.push_message(
                self.user_id,
                self.TextSendMessage(text=message)
            )
            
            logger.info("✅ 已推送每日摘要到 LINE")
            return True
        
        except Exception as e:
            logger.error(f"❌ LINE 推送摘要失敗: {e}")
            return False
    
    def send_top_news(self, news_list: list, top_n: int = 3) -> bool:
        """
        發送前 N 條最相關的新聞
        """
        try:
            if not self.line or not self.line_bot_api:
                logger.warning("⚠️  LINE 未連接，跳過推送")
                return False
            
            # 只發送相關性最高的新聞
            top_news = news_list[:top_n]
            
            for idx, news in enumerate(top_news, 1):
                stocks_str = ", ".join([
                    f"{s['name']}({s['code']})" 
                    for s in news.get('found_stocks', [])[:3]  # 最多顯示 3 個股票
                ])
                
                sentiment = news.get('sentiment_signals', {}).get('overall_sentiment', 0)
                sentiment_emoji = "📈 利多" if sentiment > 0 else "📉 利空" if sentiment < 0 else "➡️ 中立"
                
                message = f"""【重點新聞 {idx}】{sentiment_emoji}

📰 {news['title']}

相關股票: {stocks_str}
相關性評分: {news.get('relevance_score', 0)}/100
來源: {news.get('source', 'Unknown')}
時間: {news.get('pubDate', 'N/A')}

🔗 原文: {news.get('url', 'N/A')}
"""
                
                self.line_bot_api.push_message(
                    self.user_id,
                    self.TextSendMessage(text=message)
                )
                
                logger.info(f"✅ 已推送第 {idx} 條新聞到 LINE")
            
            return True
        
        except Exception as e:
            logger.error(f"❌ LINE 推送新聞失敗: {e}")
            return False
    
    def send_critical_alert(self, news: dict) -> bool:
        """
        發送緊急警報（當出現特別重要的新聞時）
        例如：台積電新聞、輝達新聞等
        """
        try:
            if not self.line or not self.line_bot_api:
                return False
            
            # 檢查是否是關鍵股票
            critical_stocks = ["2330", "2454", "2382", "6669"]  # 台積電、聯發科、廣達、緯穎
            
            stocks = [s['code'] for s in news.get('found_stocks', [])]
            is_critical = any(code in critical_stocks for code in stocks)
            
            if not is_critical:
                return False
            
            message = f"""🚨 【緊急警報】關鍵個股新聞

{news['title']}

涉及股票: {', '.join([s['name'] for s in news.get('found_stocks', [])])}

⚡ 這是一則關鍵個股的新聞，請立即查閱完整內容!

🔗 {news.get('url', 'N/A')}
"""
            
            self.line_bot_api.push_message(
                self.user_id,
                self.TextSendMessage(text=message)
            )
            
            logger.info("🚨 已推送緊急警報到 LINE")
            return True
        
        except Exception as e:
            logger.error(f"❌ LINE 推送警報失敗: {e}")
            return False


def main():
    """主程式"""
    logger.info("🚀 啟動 LINE 推送")
    
    # 讀取過濾後的新聞
    try:
        with open("filtered_news.json", 'r', encoding='utf-8') as f:
            filtered_news = json.load(f)
        logger.info(f"📖 讀取了 {len(filtered_news)} 條新聞")
    except FileNotFoundError:
        logger.error("❌ 找不到 filtered_news.json")
        return
    
    # 初始化 LINE
    notifier = LINENotifier()
    
    if not filtered_news:
        logger.warning("❌ 沒有新聞可以推送")
        return
    
    # 建立每日摘要
    today = datetime.now().strftime("%Y-%m-%d")
    all_stocks = {}
    positive_count = 0
    negative_count = 0
    
    for news in filtered_news:
        for stock in news.get('found_stocks', []):
            code = stock['code']
            if code not in all_stocks:
                all_stocks[code] = {
                    "name": stock['name'],
                    "count": 0,
                    "importance": stock.get("importance", "")
                }
            all_stocks[code]['count'] += 1
        
        sentiment = news.get('sentiment_signals', {}).get('overall_sentiment', 0)
        if sentiment > 0:
            positive_count += 1
        elif sentiment < 0:
            negative_count += 1
    
    summary = {
        'date': today,
        'total_news': len(filtered_news),
        'unique_stocks': len(all_stocks),
        'positive_news': positive_count,
        'negative_news': negative_count,
        'top_stocks': sorted(all_stocks.items(), key=lambda x: x[1]['count'], reverse=True)[:10]
    }
    
    # 推送摘要
    notifier.send_daily_summary(filtered_news, summary)
    
    # 推送前 3 條最相關的新聞
    if filtered_news:
        notifier.send_top_news(filtered_news, top_n=3)
    
    # 檢查是否有關鍵警報
    for news in filtered_news:
        notifier.send_critical_alert(news)


if __name__ == "__main__":
    main()