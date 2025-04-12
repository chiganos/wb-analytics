# parsers/funnel_parser.py
import requests
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import time
import os

API_KEY = os.getenv("eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwMjE3djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc1OTI1NjAyOSwiaWQiOiIwMTk1ZWZmYy0xMzkxLTc5MzgtODMzNS05MmU0OTZiMDJmMTIiLCJpaWQiOjQ2MTg0MjIyLCJvaWQiOjEyNTE1MiwicyI6MTA3Mzc0MTg2MCwic2lkIjoiZDUxNzJkMzgtY2NmNC00Njc1LTk3NzUtZTNjYWFlMzU4MTNkIiwidCI6ZmFsc2UsInVpZCI6NDYxODQyMjJ9.0gxVjd5KTc9lF2L53e1aU-kP5qstB1cta_3y3J4wk6330nJJDZhwufMTbxuYEVPDz9y4DT51wZ8E8hBNRnwQ-A")  # рекомендовано хранить ключ в переменной окружения
DB_PATH = "data/wb.db"
nm_ids = [253944734, 253901303, 241712687]

def run_funnel_parser():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()

    end_date = datetime.today()
    dates = [(end_date - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(30)]
    dates.reverse()

    # Добавляем недостающие колонки
    new_columns = {
        "price": "REAL",
        "cancel_count": "INTEGER",
        "cancel_sum": "REAL",
        "avg_orders_per_day": "REAL",
        "avg_price": "REAL",
        "object_id": "INTEGER",
        "object_name": "TEXT",
        "stocks_wb": "INTEGER",
        "stocks_mp": "INTEGER"
    }

    for col, col_type in new_columns.items():
        try:
            cursor.execute(f"ALTER TABLE funnel ADD COLUMN {col} {col_type}")
        except sqlite3.OperationalError:
            pass

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
        print(f"📅 {date} | 📦 Payload: {payload}")
        resp = requests.post(url, headers=headers, json=payload)

        if resp.status_code == 200:
            cards = resp.json().get("data", {}).get("cards", [])
            for item in cards:
                selected = item.get("statistics", {}).get("selectedPeriod", {})
                row = {
                    "article": item.get("nmID"),
                    "date": date,
                    "open_card": selected.get("openCardCount", 0),
                    "cart_add": selected.get("addToCartCount", 0),
                    "cart_conversion": selected.get("conversions", {}).get("addToCartPercent", 0),
                    "orders": selected.get("ordersCount", 0),
                    "revenue": selected.get("ordersSumRub", 0),
                    "cart_to_order_conv": selected.get("conversions", {}).get("cartToOrderPercent", 0),
                    "buyouts": selected.get("buyoutsCount", 0),
                    "buyout_sum": selected.get("buyoutsSumRub", 0),
                    "buyout_percent": selected.get("conversions", {}).get("buyoutsPercent", 0),
                    "cancel_count": selected.get("cancelCount", 0),
                    "cancel_sum": selected.get("cancelSumRub", 0),
                    "avg_orders_per_day": selected.get("avgOrdersCountPerDay", 0),
                    "avg_price": selected.get("avgPriceRub", 0),
                    "object_id": item.get("object", {}).get("id"),
                    "object_name": item.get("object", {}).get("name"),
                    "stocks_mp": item.get("stocks", {}).get("stocksMp"),
                    "stocks_wb": item.get("stocks", {}).get("stocksWb"),
                }
                orders = row["orders"]
                row["price"] = round(row["revenue"] / orders, 2) if orders else None
                all_rows.append(row)
        else:
            print(f"❌ Funnel error {resp.status_code} на дату {date}")
            print("📨 Ответ:", resp.text)

        time.sleep(21)

    if all_rows:
        df = pd.DataFrame(all_rows)
        df.to_sql('funnel', conn, if_exists='append', index=False)
        print(f"✅ Funnel записан: {len(df)} строк")

        cursor.execute('''
            DELETE FROM funnel WHERE rowid NOT IN (
                SELECT MAX(rowid) FROM funnel GROUP BY article, date
            )
        ''')
        print("🧹 Удалены дубликаты в таблице funnel")
    else:
        print("⚠️ Нет данных для записи")

    conn.commit()
    conn.close()
