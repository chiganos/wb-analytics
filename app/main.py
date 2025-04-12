from fastapi import FastAPI, HTTPException
import os
import sqlite3

from app.parsers.funnel_parser import parse_funnel
from app.parsers.ads_parser import parse_ads
from app.parsers.positions_parser import parse_positions

app = FastAPI()

DB_PATH = "data/wb.db"

@app.get("/")
def root():
    return {"message": "WB Analytics API is running"}

@app.get("/articles")
def read_articles():
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=500, detail="❌ База данных не найдена!")

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT article, date, orders FROM funnel LIMIT 100")
        rows = cursor.fetchall()
        return [{"article": r[0], "date": r[1], "orders": r[2]} for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ Ошибка чтения БД: {e}")
    finally:
        conn.close()


@app.post("/parse/funnel")
def run_funnel_parser():
    try:
        parse_funnel()
        return {"status": "✅ Funnel парсинг завершён"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ Funnel parser error: {e}")


@app.post("/parse/ads")
def run_ads_parser():
    try:
        parse_ads()
        return {"status": "✅ Ads парсинг завершён"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ Ads parser error: {e}")


@app.post("/parse/positions")
def run_positions_parser():
    try:
        parse_positions()
        return {"status": "✅ Positions парсинг завершён"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ Positions parser error: {e}")


@app.get("/tables")
def get_tables():
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=500, detail="❌ База данных не найдена!")

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        return {"tables": [t[0] for t in tables]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ Ошибка при получении таблиц: {e}")
    finally:
        conn.close()
