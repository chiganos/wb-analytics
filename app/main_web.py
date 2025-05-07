print("🚀 main_web.py ЗАПУСТИЛСЯ")
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import sqlite3
import pandas as pd
import os

from app.blocks import (
    sma_block,
    ads_baskets_block,
    ads_impact_block,
    price_orders_block,
    price_optimization_block,
    last_date_block,
    price_conversion_block,
    ads_shows_to_baskets_block,
    cpm_to_shows_block,
    keyword_analysis_block,
    income_summary_block,
    price_metric_block,  # для ручного маршрута
)
from app.blocks.price_metric_summary_block import router as price_metric_summary_block
from app.routes import gpt_router, combined
from app.routes.init_context import init_context_data  # импорт функции

app = FastAPI()

# Подключаем объединённый и GPT роутеры
app.include_router(combined.router)
app.include_router(gpt_router.router)

# Ручные маршруты для старых блоков
app.include_router(price_metric_summary_block)

templates = Jinja2Templates(directory="templates")

# Путь к базе
DB_PATH = os.path.join(os.path.dirname(__file__), "../data/wb.db")

# Инициализация контекста
init_context_data(DB_PATH)

def get_articles():
    conn = sqlite3.connect(DB_PATH)
    articles = pd.read_sql("SELECT DISTINCT article FROM funnel", conn)['article'].tolist()
    conn.close()
    return articles

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    articles = get_articles()
    return templates.TemplateResponse("index.html", {"request": request, "articles": articles})

@app.get("/api/analytics/sma/{article}")
def sma_analysis(article: str):
    return sma_block.analyze_sma(DB_PATH, article)

@app.get("/api/analytics/ads-baskets/{article}", response_class=HTMLResponse)
def ads_baskets_analysis(article: int):
    return ads_baskets_block.analyze_ads_baskets(DB_PATH, article)

@app.get("/api/analytics/ads-orders/{article}", response_class=HTMLResponse)
def ads_orders_analysis(article: int):
    return ads_impact_block.analyze_ads_impact(DB_PATH, article)

@app.get("/api/analytics/price-orders/{article}", response_class=HTMLResponse)
def price_orders_analysis(article: int):
    return price_orders_block.analyze_price_vs_orders(DB_PATH, article)

@app.get("/api/analytics/price-optimization/{article}", response_class=HTMLResponse)
def price_optimization_analysis(article: int):
    return price_optimization_block.analyze_price_optimization(DB_PATH, article)

@app.get("/api/analytics/price-conversion/{article}", response_class=HTMLResponse)
def price_conversion_analysis(article: int):
    return price_conversion_block.analyze_price_conversion(DB_PATH, article)

@app.get("/api/analytics/ads-shows-baskets/{article}", response_class=HTMLResponse)
def ads_shows_baskets_analysis(article: int):
    return ads_shows_to_baskets_block.analyze_ads_shows_to_baskets(DB_PATH, article)

@app.get("/api/analytics/cpm-shows/{article}", response_class=HTMLResponse)
def cpm_to_shows_analysis(article: int):
    return cpm_to_shows_block.analyze_cpm_vs_shows(DB_PATH, article)

@app.get("/api/analytics/keywords/{article}", response_class=HTMLResponse)
def keyword_analysis(article: int):
    return keyword_analysis_block.analyze_keywords(DB_PATH, article)

@app.get("/api/analytics/price-metric/{article}", response_class=HTMLResponse)
def price_metric_analysis(article: int):
    return price_metric_block.render_price_metric_block(article, DB_PATH)

@app.get("/api/analytics/last-date/{article}", response_class=HTMLResponse)
def last_date_analysis(article: int):
    return last_date_block.render_last_date_block(article, DB_PATH)

@app.get("/api/analytics/income-summary/{article}", response_class=HTMLResponse)
def income_summary_analysis(article: int):
    return income_summary_block.render_income_summary_block(DB_PATH)
