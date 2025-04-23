import requests
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import time
from utils.logger import log

# === Настройки ===
API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwMjE3djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc1OTI1NjAyOSwiaWQiOiIwMTk1ZWZmYy0xMzkxLTc5MzgtODMzNS05MmU0OTZiMDJmMTIiLCJpaWQiOjQ2MTg0MjIyLCJvaWQiOjEyNTE1MiwicyI6MTA3Mzc0MTg2MCwic2lkIjoiZDUxNzJkMzgtY2NmNC00Njc1LTk3NzUtZTNjYWFlMzU4MTNkIiwidCI6ZmFsc2UsInVpZCI6NDYxODQyMjJ9.0gxVjd5KTc9lF2L53e1aU-kP5qstB1cta_3y3J4wk6330nJJDZhwufMTbxuYEVPDz9y4DT51wZ8E8hBNRnwQ-A"
ARTICLES_CSV = "articles.csv"
DAYS_TO_PARSE = 45

# Загружаем список артикулов из CSV
articles_df = pd.read_csv(ARTICLES_CSV)
nm_ids = articles_df["article"].dropna().astype(int).tolist()

# === Функция получения уже сохранённых дат ===
def get_existing_dates(article_id, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT date FROM positions WHERE article = ?", (article_id,))
    return set(row[0] for row in cursor.fetchall())

# === Основной код парсинга ===
def parse_positions():
    conn = sqlite3.connect("data/wb.db")
    today = datetime.today().date()

    for nm_id in nm_ids:
        log(f"\n▶ Парсим артикул {nm_id}")
        existing_dates = get_existing_dates(nm_id, conn)

        for delta in range(DAYS_TO_PARSE):
            target_date = (today - timedelta(days=delta)).isoformat()
            if target_date in existing_dates:
                log(f"Пропускаем {target_date} — уже в базе")
                continue

            url = f"https://statistics-api.wildberries.ru/api/v1/supplier/search"  # пример URL
            headers = {"Authorization": API_KEY}
            params = {
                "dateFrom": target_date,
                "dateTo": target_date,
                "nm": nm_id
            }

            response = requests.get(url, headers=headers, params=params)
            if response.status_code != 200:
                log(f"Ошибка {response.status_code} при запросе за {target_date}")
                time.sleep(2)
                continue

            data = response.json()
            if not data:
                log(f"Нет данных за {target_date}")
                continue

            # Сохраняем в базу
            for record in data:
                conn.execute(
                    """
                    INSERT INTO positions (article, search_query, avg_position, visibility, orders, 
                        basket_conversion, order_conversion, date, open_card)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        nm_id,
                        record.get("searchQuery"),
                        record.get("avgPosition"),
                        record.get("visibility"),
                        record.get("orders"),
                        record.get("basketConversion"),
                        record.get("orderConversion"),
                        target_date,
                        record.get("openCard"),
                    )
                )
            conn.commit()
            log(f"Сохранено за {target_date}: {len(data)} записей")
            time.sleep(1.5)

    conn.close()
    log("\n✅ Парсинг завершён")

if __name__ == "__main__":
    parse_positions()