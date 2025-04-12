import requests
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import time
from utils.logger import log

# === Настройки ===
API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwMjE3djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc1OTI1NjAyOSwiaWQiOiIwMTk1ZWZmYy0xMzkxLTc5MzgtODMzNS05MmU0OTZiMDJmMTIiLCJpaWQiOjQ2MTg0MjIyLCJvaWQiOjEyNTE1MiwicyI6MTA3Mzc0MTg2MCwic2lkIjoiZDUxNzJkMzgtY2NmNC00Njc1LTk3NzUtZTNjYWFlMzU4MTNkIiwidCI6ZmFsc2UsInVpZCI6NDYxODQyMjJ9.0gxVjd5KTc9lF2L53e1aU-kP5qstB1cta_3y3J4wk6330nJJDZhwufMTbxuYEVPDz9y4DT51wZ8E8hBNRnwQ-A"  # ← вставь свой токен Wildberries
nm_ids = [241733698]    # ← список артикулов
DB_PATH = "data/wb.db"

def parse_positions():
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        cursor = conn.cursor()

        today = datetime.today()
        dates = [(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(30)]
        dates.reverse()
        log(f"📅 Диапазон дат: {dates[0]} — {dates[-1]}")

        # Добавим колонку open_card, если нет
        try:
            cursor.execute("ALTER TABLE positions ADD COLUMN open_card INTEGER")
            log("➕ Добавлена колонка open_card")
        except sqlite3.OperationalError:
            pass

        url = "https://seller-analytics-api.wildberries.ru/api/v2/search-report/product/search-texts"
        headers = {"Authorization": API_KEY, "Content-Type": "application/json"}
        all_rows = []

        for date in dates:
            for nm_id in nm_ids:
                log(f"📦 Positions {date} | 🔍 nm_id: {nm_id}")
                payload = {
                    "currentPeriod": {"start": date, "end": date},
                    "nmIds": [nm_id],
                    "topOrderBy": "openCard",
                    "orderBy": {"field": "avgPosition", "mode": "asc"},
                    "limit": 30
                }

                for attempt in range(3):
                    resp = requests.post(url, headers=headers, json=payload)

                    if resp.status_code == 200:
                        break
                    elif resp.status_code == 429:
                        log(f"⏳ 429-лимит, попытка {attempt+1}, ждём 60 сек...")
                        time.sleep(60)
                    else:
                        log(f"❌ Positions {date} — ошибка {resp.status_code}: {resp.text}")
                        break

                if resp.status_code == 200:
                    items = resp.json().get("data", {}).get("items", [])
                    for item in items:
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

        if all_rows:
            df = pd.DataFrame(all_rows)
            df.to_sql("positions", conn, if_exists="append", index=False)
            log(f"✅ Positions: записано {len(df)} строк")

            # Удалим дубликаты
            cursor.execute('''
                DELETE FROM positions WHERE rowid NOT IN (
                    SELECT MAX(rowid) FROM positions
                    GROUP BY article, date, search_query
                )
            ''')
            log("🧹 Удалены дубликаты в таблице positions")
        else:
            log("⚠️ Positions: пустой ответ")

        conn.commit()
        conn.close()
    except Exception as e:
        log(f"❌ Ошибка в positions_parser: {e}")
