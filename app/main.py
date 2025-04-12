from fastapi import FastAPI, HTTPException
import sqlite3
import os

app = FastAPI()

DB_PATH = "data/wb.db"

@app.get("/articles")
def read_articles():
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=500, detail="❌ База данных не найдена!")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT article, date, orders FROM funnel LIMIT 100")
        rows = cursor.fetchall()
        return [{"article": r[0], "date": r[1], "orders": r[2]} for r in rows]
    except sqlite3.OperationalError as e:
        raise HTTPException(status_code=500, detail=f"❌ Ошибка чтения БД: {e}")
    finally:
        conn.close()
