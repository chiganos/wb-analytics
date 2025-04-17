import requests
import pandas as pd
import sqlite3
import time
from datetime import datetime, timedelta
from utils.logger import log

# === Настройки ===
API_KEY = "ВАШ_ТОКЕН"
nm_ids = [241673716, 241671651, 341660641, 253899763, 253901303, 253904160, 253930727, 253932533, 253951797, 253935924, 253944734, 253954121, 282933985, 282930197, 287175828, 287405946, 287394586, 282925140, 287387639, 287410201, 287210296, 287189018, 287938384, 287272632, 287935736, 287815696, 287826830, 287282170, 287237715, 287250048, 287923303, 287411459, 301510803, 301521900, 301532388, 301517629, 301500581, 301529171, 301498019, 301491992, 301487789, 301567788, 287943389, 221455925, 321876497, 370471780, 370480035, 370489588, 370491526, 371681456, 371664076, 371706064, 371711825, 371713737, 221268061, 221462995, 220093122]  # ← список артикулов
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
                    try:
                        resp = requests.post(url, headers=headers, json=payload)
                        if resp.status_code == 200:
                            break
                        elif resp.status_code == 429:
                            log(f"⏳ 429-лимит, попытка {attempt+1}, ждём 60 сек...")
                            time.sleep(60)
                        else:
                            log(f"❌ Positions {date} — ошибка {resp.status_code}: {resp.text}")
                            break
                    except Exception as e:
                        log(f"⚠️ Positions {date} — сбой соединения: {e}")
                        break

                if resp.status_code == 200:
                    items = resp.json().get("data", {}).get("items", [])
                    rows = []
                    for item in items:
                        rows.append({
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

                    if rows:
                        df = pd.DataFrame(rows)
                        df.to_sql("positions", conn, if_exists="append", index=False)
                        log(f"✅ Записано {len(df)} строк за {date}, nm_id={nm_id}")
                        conn.commit()
                    else:
                        log(f"⚠️ Пустой ответ для nm_id={nm_id}, дата {date}")

                time.sleep(21)

        # Удалим дубликаты
        cursor.execute('''
            DELETE FROM positions WHERE rowid NOT IN (
                SELECT MAX(rowid) FROM positions GROUP BY article, date, search_query
            )
        ''')
        conn.commit()
        log("🧹 Удалены дубликаты в таблице positions")

        conn.close()

    except Exception as e:
        log(f"❌ Ошибка в positions_parser: {e}")
