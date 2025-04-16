import requests
import pandas as pd
import sqlite3
import time
from datetime import datetime, timedelta
from utils.logger import log

# === Настройки ===
API_KEY_ADV = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwMjE3djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc1OTI2NTQ4NywiaWQiOiIwMTk1ZjA4Yy02MmM5LTczZWUtYTk3NS1hOGM1ZGIxZTIyNzAiLCJpaWQiOjQ2MTg0MjIyLCJvaWQiOjEyNTE1MiwicyI6NjQsInNpZCI6ImQ1MTcyZDM4LWNjZjQtNDY3NS05Nzc1LWUzY2FhZTM1ODEzZCIsInQiOmZhbHNlLCJ1aWQiOjQ2MTg0MjIyfQ.9HUrbPlA6BpTm5Ru9yyLJVfDGUhbGgeaHM6Oob0RMz2M1EUIGgL0NxwrmUgiI_YDaxBUPHv_5yQd_O_Xa2DADA"  # ← вставь свой токен для рекламы Wildberries
DB_PATH = "data/wb.db"

# === Артикулы и ID кампаний (сопоставление вручную) ===
campaign_map = {
    241733698: 21138230,
    228486132: 17124451,
    301510803: 22216755,
    241712687: 24679732,
    321876497: 23841619,
    287943389: 21797248,
    220093122: 24456528,
    221268061: 24272173,
    371664076: 24487622,
    371681456: 24487369,
    370491526: 24424073,
    370471780: 24424038,
    241650455: 18615852,
    241733698: 21138230,
    221557108: 21064000,
    371711825: 24581519,
    370480035: 24487475,
    221309737: 22316736,
    232250467: 22803108,
    220153493: 21323348,
    221271976: 19615675,
    371706064: 24424539,
    370489588: 24423551,
    371713737: 24487703,
    342549833: 23845482,
    325066983: 23208050,
    221510907: 16515083,  # Пример: артикул: ID рекламной кампании
    # Добавляй по аналогии
}

def parse_ads():
    try:
        conn = sqlite3.connect(DB_PATH)
        end_date = datetime.today()
        dates = [(end_date - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(29, -1, -1)]
        log(f"📅 Диапазон дат: {dates[0]} — {dates[-1]}")

        url = "https://advert-api.wildberries.ru/adv/v2/fullstats"
        headers = {"Authorization": API_KEY_ADV, "Content-Type": "application/json"}

        last_request_time = 0
        all_rows = []

        for article, adv_id in campaign_map.items():
            for i, date in enumerate(dates):
                log(f"📆 [{i+1}/{len(dates)}] {date} | Артикул: {article}")
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
                        log(f"⏳ Лимит 429, попытка {attempt+1}, ждём 60 сек...")
                        time.sleep(60)
                    else:
                        log(f"❌ Ошибка {resp.status_code}: {resp.text}")
                        break

                if resp.status_code == 200:
                    raw = resp.json()
                    if isinstance(raw, list):
                        for stat in raw:
                            all_rows.append({
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

        if all_rows:
            df = pd.DataFrame(all_rows)
            df.to_sql("ads", conn, if_exists="append", index=False)
            log(f"✅ Записано в ads: {len(df)} строк")

            # Удаление дубликатов
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM ads WHERE rowid NOT IN (
                    SELECT MAX(rowid) FROM ads
                    GROUP BY article, date
                )
            ''')
            log("🧹 Удалены дубликаты в таблице ads")
        else:
            log("⚠️ Нет данных для записи")

        conn.commit()
        conn.close()

    except Exception as e:
        log(f"❌ Ошибка в ads_parser: {e}")
