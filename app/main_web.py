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
    price_conversion_block,
    ads_shows_to_baskets_block,
    cpm_to_shows_block,
    keyword_analysis_block,
)
from app.routes import gpt_router, combined
from app.routes.init_context import init_context_data  # импорт функции

app = FastAPI()
app.include_router(combined.router)
app.include_router(gpt_router.router)

templates = Jinja2Templates(directory="templates")

# ВАЖНО: сначала путь к базе
DB_PATH = os.path.join(os.path.dirname(__file__), "../data/wb.db")

# Затем инициализация контекста
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

@app.get("/api/analytics/sma/{article}", response_class=HTMLResponse)
def sma_analysis(article: int):
    html = sma_block.analyze_sma(DB_PATH, article)
    return HTMLResponse(content=html)

@app.get("/api/analytics/ads-baskets/{article}", response_class=HTMLResponse)
def ads_baskets_analysis(article: int):
    html = ads_baskets_block.analyze_ads_baskets(DB_PATH, article)
    return HTMLResponse(content=html)

@app.get("/api/analytics/ads-orders/{article}", response_class=HTMLResponse)
def ads_orders_analysis(article: int):
    html = ads_impact_block.analyze_ads_impact(DB_PATH, article)
    return HTMLResponse(content=html)

@app.get("/api/analytics/price-orders/{article}", response_class=HTMLResponse)
def price_orders_analysis(article: int):
    html = price_orders_block.analyze_price_impact(DB_PATH, article)
    return HTMLResponse(content=html)

@app.get("/api/analytics/price-optimization/{article}", response_class=HTMLResponse)
def price_optimization_analysis(article: int):
    html = price_optimization_block.analyze_price_optimization(DB_PATH, article)
    return HTMLResponse(content=html)

@app.get("/api/analytics/price-conversion/{article}", response_class=HTMLResponse)
def price_conversion_analysis(article: int):
    html = price_conversion_block.analyze_price_conversion(DB_PATH, article)
    return HTMLResponse(content=html)

@app.get("/api/analytics/ads-shows-baskets/{article}", response_class=HTMLResponse)
def ads_shows_baskets_analysis(article: int):
    html = ads_shows_to_baskets_block.analyze_ads_shows_to_baskets(DB_PATH, article)
    return HTMLResponse(content=html)

@app.get("/api/analytics/cpm-shows/{article}", response_class=HTMLResponse)
def cpm_to_shows_analysis(article: int):
    html = cpm_to_shows_block.analyze_cpm_to_shows(DB_PATH, article)
    return HTMLResponse(content=html)

@app.get("/api/analytics/keywords/{article}", response_class=HTMLResponse)
def keyword_analysis(article: int):
    html = keyword_analysis_block.analyze_keywords(DB_PATH, article)
    return HTMLResponse(content=html)

# ==== API ROUTES FROM main_api.py ====
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, FileResponse
from parsers.funnel_parser import parse_funnel
from parsers.positions_parser import parse_positions
from parsers.ads_parser import parse_ads
from utils.logger import get_logs, clear_logs
import os

@app.get("/")
def root():
    return {"msg": "WB Analytics API is working!"}
@app.get("/run/funnel")
def run_funnel():
    parse_funnel()
    return {"status": "🔄 Funnel parsing started"}
@app.get("/run/positions")
def run_positions():
    parse_positions()
    return {"status": "🔄 Positions parsing started"}
@app.get("/run/ads")
def run_ads():
    parse_ads()
    return {"status": "🔄 Ads parsing started"}
@app.get("/logs", response_class=PlainTextResponse)
def view_logs():
    return get_logs()
@app.get("/logs/clear")
def clear_log_memory():
    clear_logs()
    return {"status": "🧹 Логи очищены"}
@app.get("/download-db")
def download_db():
    db_path = "../data/wb.db"
    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail="Файл базы данных не найден")
    return FileResponse(path=db_path, filename="wb.db", media_type="application/octet-stream")