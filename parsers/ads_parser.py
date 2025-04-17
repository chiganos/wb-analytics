import requests
import pandas as pd
import sqlite3
import time
from datetime import datetime, timedelta
from utils.logger import log  # для логов в Render

# === Настройки ===
API_KEY_ADV = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwMjE3djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc1OTI2NTQ4NywiaWQiOiIwMTk1ZjA4Yy02MmM5LTczZWUtYTk3NS1hOGM1ZGIxZTIyNzAiLCJpaWQiOjQ2MTg0MjIyLCJvaWQiOjEyNTE1MiwicyI6NjQsInNpZCI6ImQ1MTcyZDM4LWNjZjQtNDY3NS05Nzc1LWUzY2FhZTM1ODEzZCIsInQiOmZhbHNlLCJ1aWQiOjQ2MTg0MjIyfQ.9HUrbPlA6BpTm5Ru9yyLJVfDGUhbGgeaHM6Oob0RMz2M1EUIGgL0NxwrmUgiI_YDaxBUPHv_5yQd_O_Xa2DADA"
campaign_map = {
    221271976: 19615675,
    325198127: 23208345,
    325047938: 23207993,
    287237715: 21797677,
    221335159: 16520021,
    325196893: 23308665,
    287387639: 24434039,
    240769249: 22015480,
    325198127: 23308674,
    342772296: 23845599,
    223082066: 22276139,
    196005679: 23845710,
    301498019: 22538906,
    325047938: 23308508,
    196005679: 23845709,
    342772296: 23845597,
    220045061: 22803407,
    253901303: 23265432,
    325190642: 23208314,
    221512293: 23884774,
    223082066: 22276196,
    342585780: 23845370,
    325097246: 23308671,
    221512293: 23884776,
    228466070: 17124411,
    325201047: 23208282,
    325097246: 23208083,
    240769249: 18615752,
    325196893: 23208025,
    221335159: 23360647,
    241740204: 22276138,
    342585780: 23845369,
    240817485: 19194839,
    221304033: 24095296,
    371711825: 24424837,
    220045061: 22803371,
    342549833: 23845483,
    241740204: 22276140,
    221462995: 24244392,
    232250467: 22803109,
    341660641: 23937393,
    325066983: 23308673,
    325182611: 23248245,
    325167677: 23208261,
    325155513: 23208243,
    301529171: 22217075,
    301500581: 22217064,
    301532388: 22216803,
    301521900: 22216776,
    287282170: 22215991,
    287815696: 21798546,
    287826830: 21798213,
    287938384: 21798094,
    287175828: 21797914,
    282925140: 21797753,
    287272632: 21797567,
    287410201: 21795029,
    287405946: 21794917,
    287387639: 21794806,
    287394586: 21794620,
    182343735: 20724205,
    221286234: 19908618,
    253904160: 19525911,
    253899763: 19519428,
    253944734: 19519418,
    253901303: 19519410,
    253932533: 19518049,
    241683099: 18615887,
    241673716: 18615881,
    241646550: 18615818,
    241640497: 18615803,
    207037418: 17757020,
    207031416: 17756994,
    151450378: 17567462,
    220099175: 16520204,
    # добавь остальные: артикул: ID кампании
}
DB_PATH = "data/wb.db"

# === Генерация дат ===
today = datetime.today()
dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(29, -1, -1)]

def parse_ads():
    log("🔄 Парсим рекламу за 30 дней...")
    url = "https://advert-api.wildberries.ru/adv/v2/fullstats"
    headers = {"Authorization": API_KEY_ADV, "Content-Type": "application/json"}
    last_request_time = 0
    rows = []

    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()

    # === Создание таблицы при необходимости ===
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ads (
            article TEXT,
            date TEXT,
            shows INTEGER,
            clicks INTEGER,
            ctr REAL,
            cpc REAL,
            cost REAL,
            cr REAL,
            orders INTEGER,
            baskets INTEGER
        )
    """)
    conn.commit()

    for article, adv_id in campaign_map.items():
        for i, date in enumerate(dates):
            log(f"📅 [{article}] {i+1}/{len(dates)}: {date}")
            payload = [{"id": adv_id, "dates": [date]}]

            for attempt in range(3):
                elapsed = time.time() - last_request_time
                if elapsed < 21:
                    time.sleep(21 - elapsed)
                try:
                    resp = requests.post(url, json=payload, headers=headers)
                    last_request_time = time.time()

                    if resp.status_code == 200:
                        break
                    elif resp.status_code == 429:
                        log(f"⏳ Попытка {attempt+1}: лимит. Ждём 60 сек...")
                        time.sleep(60)
                    else:
                        log(f"❌ Ошибка {resp.status_code}: {resp.text}")
                        break
                except Exception as e:
                    log(f"⚠️ Сетевая ошибка: {e}")
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

    if rows:
        df = pd.DataFrame(rows)
        df.to_sql("ads", conn, if_exists="append", index=False)
        log(f"✅ Записано в ads: {len(df)} строк")
    else:
        log("⚠️ Нет данных для записи")

    conn.commit()
    conn.close()
