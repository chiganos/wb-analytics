from fastapi import FastAPI, HTTPException
import sqlite3
import os

app = FastAPI()

DB_PATH = "data/wb.db"

def fetch_rows(query, fields):
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=500, detail="❌ База данных не найдена!")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        return [dict(zip(fields, r)) for r in rows]
    except sqlite3.OperationalError as e:
        raise HTTPException(status_code=500, detail=f"❌ Ошибка чтения БД: {e}")
    finally:
        conn.close()

@app.get("/")
def root():
    return {"message": "🎉 WB Analytics API запущен успешно!"}

@app.get("/funnel")
def read_funnel():
    return fetch_rows(
        "SELECT article, date, orders, revenue, price FROM funnel LIMIT 100",
        ["article", "date", "orders", "revenue", "price"]
    )

@app.get("/ads")
def read_ads():
    return fetch_rows(
        "SELECT article, date, shows, clicks, cost, ctr, cpc FROM ads LIMIT 100",
        ["article", "date", "shows", "clicks", "cost", "ctr", "cpc"]
    )

@app.get("/positions")
def read_positions():
    return fetch_rows(
        "SELECT article, date, search_query, avg_position, visibility, open_card FROM positions LIMIT 100",
        ["article", "date", "search_query", "avg_position", "visibility", "open_card"]
    )

@app.get("/upload")
def read_upload():
    return fetch_rows(
        "SELECT article, upload_date, name, cost_price, wb_fee_percent, logistics, stock FROM upload LIMIT 100",
        ["article", "upload_date", "name", "cost_price", "wb_fee_percent", "logistics", "stock"]
    )

@app.get("/calculated")
def read_calculated():
    return fetch_rows(
        "SELECT article, date, orders, revenue, profit, roi, margin_percent FROM calculated LIMIT 100",
        ["article", "date", "orders", "revenue", "profit", "roi", "margin_percent"]
    )
