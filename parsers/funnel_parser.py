import requests
import pandas as pd
import sqlite3
import time
from datetime import datetime, timedelta
from utils.logger import log

API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwMjE3djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc1OTI1NjAyOSwiaWQiOiIwMTk1ZWZmYy0xMzkxLTc5MzgtODMzNS05MmU0OTZiMDJmMTIiLCJpaWQiOjQ2MTg0MjIyLCJvaWQiOjEyNTE1MiwicyI6MTA3Mzc0MTg2MCwic2lkIjoiZDUxNzJkMzgtY2NmNC00Njc1LTk3NzUtZTNjYWFlMzU4MTNkIiwidCI6ZmFsc2UsInVpZCI6NDYxODQyMjJ9.0gxVjd5KTc9lF2L53e1aU-kP5qstB1cta_3y3J4wk6330nJJDZhwufMTbxuYEVPDz9y4DT51wZ8E8hBNRnwQ-A"  # ВСТАВЬ свой токен
nm_ids = [221271976, 226070057, 242232557, 202397477, 202410680, 221282712, 221286234, 221297013, 221302491, 228466070, 221309737, 220153493, 220156032, 221550593, 221510907, 220099175, 220102598, 221457879, 221557108, 221333226, 221328689, 223084627, 221568957, 228472488, 228481434, 228486132, 232242485, 232245044, 241712687, 241724604, 241728954, 241743698, 241733698, 240756091, 240759175, 241640497, 241643288, 240817485, 240772290, 240788230, 241646550, 241648668, 240800994, 241650455, 240810358, 241675246, 241683099, 241673716, 241671651, 341660641, 253899763, 253901303, 253904160, 253930727, 253932533, 253951797, 253935924, 253944734, 253954121, 282933985, 282930197, 287175828, 287405946, 287394586, 282925140, 287387639, 287410201, 287210296, 287189018, 287938384, 287272632, 287935736, 287815696, 287826830, 287282170, 287237715, 287250048, 287923303, 287411459, 301510803, 301521900, 301532388, 301517629, 301500581, 301529171, 301498019, 301491992, 301487789, 301567788, 287943389, 221455925, 321876497, 370471780, 370480035, 370489588, 370491526, 371681456, 371664076, 371706064, 371711825, 371713737, 221268061, 221462995, 220093122]  # обновлённый список артикулов
DB_PATH = "data/wb.db"

def parse_funnel():
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        cursor = conn.cursor()

        today = datetime.today()
        dates = [(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(30)]
        dates.reverse()
        log(f"📅 Диапазон дат: {dates[0]} — {dates[-1]}")

        url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail"
        headers = {"Authorization": API_KEY, "Content-Type": "application/json"}
        all_rows = []

        for date in dates:
            payload = {
                "nmIDs": nm_ids,
                "period": {
                    "begin": f"{date} 00:00:00",
                    "end": f"{date} 23:59:59"
                },
                "timezone": "Europe/Moscow",
                "aggregationLevel": "day",
                "page": 1
            }
            log(f"📦 Funnel {date}: отправка запроса...")
            resp = requests.post(url, headers=headers, json=payload)

            if resp.status_code == 200:
                for item in resp.json().get("data", {}).get("cards", []):
                    stat = item.get("statistics", {}).get("selectedPeriod", {})
                    row = {
                        "article": item.get("nmID"),
                        "date": date,
                        "open_card": stat.get("openCardCount", 0),
                        "cart_add": stat.get("addToCartCount", 0),
                        "cart_conversion": stat.get("conversions", {}).get("addToCartPercent", 0),
                        "orders": stat.get("ordersCount", 0),
                        "revenue": stat.get("ordersSumRub", 0),
                        "cart_to_order_conv": stat.get("conversions", {}).get("cartToOrderPercent", 0),
                        "buyouts": stat.get("buyoutsCount", 0),
                        "buyout_sum": stat.get("buyoutsSumRub", 0),
                        "buyout_percent": stat.get("conversions", {}).get("buyoutsPercent", 0),
                        "cancel_count": stat.get("cancelCount", 0),
                        "cancel_sum": stat.get("cancelSumRub", 0),
                        "avg_orders_per_day": stat.get("avgOrdersCountPerDay", 0),
                        "avg_price": stat.get("avgPriceRub", 0),
                        "object_id": item.get("object", {}).get("id"),
                        "object_name": item.get("object", {}).get("name"),
                        "stocks_mp": item.get("stocks", {}).get("stocksMp"),
                        "stocks_wb": item.get("stocks", {}).get("stocksWb"),
                    }
                    orders = row["orders"]
                    row["price"] = round(row["revenue"] / orders, 2) if orders else None
                    all_rows.append(row)
            else:
                log(f"❌ Funnel {date} — ошибка {resp.status_code}: {resp.text}")

            time.sleep(21)

        if all_rows:
            df = pd.DataFrame(all_rows)
            df.to_sql("funnel", conn, if_exists="append", index=False)
            log(f"✅ Funnel: записано {len(df)} строк")
        else:
            log("⚠️ Funnel: пустой ответ")

        conn.commit()
        conn.close()
    except Exception as e:
        log(f"❌ Funnel: ошибка {e}")
