# app/main.py
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import sqlite3
import pandas as pd
import os

from app.blocks import sma_block  # Импортируем SMA-блок

app = FastAPI()
templates = Jinja2Templates(directory="templates")

DB_PATH = os.path.join(os.path.dirname(__file__), "../wb.db")


def get_articles():
    conn = sqlite3.connect(DB_PATH)
    articles = pd.read_sql("SELECT DISTINCT article FROM funnel", conn)['article'].tolist()
    conn.close()
    return articles


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    articles = get_articles()
    return templates.TemplateResponse("index.html", {"request": request, "articles": articles})


@app.get("/api/analytics/sma/{article}", response_class=HTMLResponse)
def sma_analysis(article: int):
    html = sma_block.analyze_sma(DB_PATH, article)
    return HTMLResponse(content=html)

