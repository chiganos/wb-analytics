import requests
import pandas as pd
import sqlite3
import time
from datetime import datetime, timedelta

API_KEY_ADV = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwMjE3djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc1OTI2NTQ4NywiaWQiOiIwMTk1ZjA4Yy02MmM5LTczZWUtYTk3NS1hOGM1ZGIxZTIyNzAiLCJpaWQiOjQ2MTg0MjIyLCJvaWQiOjEyNTE1MiwicyI6NjQsInNpZCI6ImQ1MTcyZDM4LWNjZjQtNDY3NS05Nzc1LWUzY2FhZTM1ODEzZCIsInQiOmZhbHNlLCJ1aWQiOjQ2MTg0MjIyfQ.9HUrbPlA6BpTm5Ru9yyLJVfDGUhbGgeaHM6Oob0RMz2M1EUIGgL0NxwrmUgiI_YDaxBUPHv_5yQd_O_Xa2DADA"  # <-- Вставь свой токен
DB_PATH = "data/wb.db"
CAMPAIGN_MAP = {
    221271976: 19615675
}
DAYS = 30

def parse_ads():
    print("🔄 Запуск парсера рекламы (ads)")
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        url = "https://advert-api.wildberries.ru/adv/v2/fullstats"
        headers = {"Authorization": API_KEY_ADV, "Content-Type": "application/json"}
        rows = []

        end = datetime.today()
        dates = [(end - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(DAYS)][::-1]
        last_request_time = 0

        for article, adv_id in CAMPAIGN_MAP.items():
            for i, date in enumerate(dates):
                print(f"📅 [{article}] {i + 1}/{DAYS}: {date}")
                payload = [{"id": adv_id, "dates": [date]}]

                for attempt in range(3):
                    elapsed = time.time() - last_request_time
                    if elapsed < 21:
                        time.sleep(21 - elapsed)

                    resp = requests.post(url, json=payload, headers=headers)
                    last_request_time = time.time()

                    if resp.status_code == 200:
                        break
                    elif resp.status_code == 429:
                        print(f"⏳ Попытка {attempt + 1}: лимит. Ждём 60 сек...")
                        time.sleep(60)
                    else:
                        print(f"❌ Ошибка {resp.status_code}: {resp.text}")
                        break

                if resp.status_code == 200:
                    raw = resp.json()
                    if isinstance(raw, list):
                        for stat in raw:
                            rows.append({
                                "article": article,
                                "date": date,
                                "shows": stat.get("views", 0),
                                "clicks": stat.get("clicks", 0),
                                "ctr": round(stat.get("ctr", 0), 4),
                                "cpc": stat.get("cpc", 0),
                                "cost": stat.get("sum", 0),
                                "cr": round(stat.get("cr", 0), 4),
                                "orders": stat.get("orders", 0),
                                "baskets": stat.get("atbs", 0)
                            })

        df = pd.DataFrame(rows)
        if not df.empty:
            df.to_sql("ads", conn, if_exists="append", index=False)
            print(f"✅ Записано в ads: {len(df)} строк")
        else:
            print("⚠️ Нет данных для записи")
    except Exception as e:
        print(f"❌ Ошибка парсера ads: {e}")
    finally:
        conn.commit()
        conn.close()
