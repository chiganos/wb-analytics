import requests
import pandas as pd
import sqlite3
import time
from datetime import datetime, timedelta
from utils.logger import log

API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwMjE3djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc1OTI1NjAyOSwiaWQiOiIwMTk1ZWZmYy0xMzkxLTc5MzgtODMzNS05MmU0OTZiMDJmMTIiLCJpaWQiOjQ2MTg0MjIyLCJvaWQiOjEyNTE1MiwicyI6MTA3Mzc0MTg2MCwic2lkIjoiZDUxNzJkMzgtY2NmNC00Njc1LTk3NzUtZTNjYWFlMzU4MTNkIiwidCI6ZmFsc2UsInVpZCI6NDYxODQyMjJ9.0gxVjd5KTc9lF2L53e1aU-kP5qstB1cta_3y3J4wk6330nJJDZhwufMTbxuYEVPDz9y4DT51wZ8E8hBNRnwQ-A"  # ВСТАВЬ свой токен
nm_ids = [241733698]  # обновлённый список артикулов
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
