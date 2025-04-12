import requests
import pandas as pd
import sqlite3
import time
from datetime import datetime, timedelta

API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwMjE3djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc1OTI1NjAyOSwiaWQiOiIwMTk1ZWZmYy0xMzkxLTc5MzgtODMzNS05MmU0OTZiMDJmMTIiLCJpaWQiOjQ2MTg0MjIyLCJvaWQiOjEyNTE1MiwicyI6MTA3Mzc0MTg2MCwic2lkIjoiZDUxNzJkMzgtY2NmNC00Njc1LTk3NzUtZTNjYWFlMzU4MTNkIiwidCI6ZmFsc2UsInVpZCI6NDYxODQyMjJ9.0gxVjd5KTc9lF2L53e1aU-kP5qstB1cta_3y3J4wk6330nJJDZhwufMTbxuYEVPDz9y4DT51wZ8E8hBNRnwQ-A"  # <-- Вставь свой токен
DB_PATH = "data/wb.db"
NM_IDS = [253944734, 253901303, 241712687]
DAYS = 30

def parse_positions():
    print("🔄 Запуск парсера позиций (positions)")
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        cursor = conn.cursor()

        # Колонка open_card
        try:
            cursor.execute("ALTER TABLE positions ADD COLUMN open_card INTEGER")
            print("➕ Добавлена колонка open_card")
        except sqlite3.OperationalError:
            pass

        url = "https://seller-analytics-api.wildberries.ru/api/v2/search-report/product/search-texts"
        headers = {"Authorization": API_KEY, "Content-Type": "application/json"}
        all_rows = []

        end = datetime.today()
        dates = [(end - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(DAYS)][::-1]

        for date in dates:
            for nm_id in NM_IDS:
                print(f"📅 {date} | 🔍 nm_id: {nm_id}")
                payload = {
                    "currentPeriod": {"start": date, "end": date},
                    "nmIds": [nm_id],
                    "topOrderBy": "openCard",
                    "orderBy": {"field": "avgPosition", "mode": "asc"},
                    "limit": 30
                }

                for attempt in range(3):
                    resp = requests.post(url, json=payload, headers=headers)
                    if resp.status_code == 200:
                        break
                    elif resp.status_code == 429:
                        print(f"⏳ 429-лимит, попытка {attempt + 1}, ждём 60 сек...")
                        time.sleep(60)
                    else:
                        print(f"⚠️ Ошибка {resp.status_code} на {date}")
                        break

                if resp.status_code == 200:
                    data = resp.json().get("data", {}).get("items", [])
                    for item in data:
                        all_rows.append({
                            "article": item.get("nmId"),
                            "search_query": item.get("text"),
                            "avg_position": item.get("avgPosition", {}).get("current"),
                            "visibility": item.get("visibility", {}).get("current"),
                            "orders": item.get("orders", {}).get("current"),
                            "open_card": item.get("openCard", {}).get("current"),
                            "basket_conversion": item.get("openToCart", {}).get("current"),
                            "order_conversion": item.get("cartToOrder", {}).get("current"),
                            "date": date
                        })
                time.sleep(21)

        df = pd.DataFrame(all_rows)
        if not df.empty:
            df.to_sql("positions", conn, if_exists="append", index=False)
            print(f"✅ Positions записаны: {len(df)} строк")

            cursor.execute('''
                DELETE FROM positions WHERE rowid NOT IN (
                    SELECT MAX(rowid) FROM positions
                    GROUP BY article, date, search_query
                )
            ''')
            print("🧹 Удалены дубликаты")
        else:
            print("⚠️ Нет данных для записи")
    except Exception as e:
        print(f"❌ Ошибка парсера positions: {e}")
    finally:
        conn.commit()
        conn.close()
