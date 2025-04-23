from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, FileResponse
from parsers.funnel_parser import parse_funnel
from parsers.positions_parser import parse_positions
from parsers.ads_parser import parse_ads
from utils.logger import get_logs, clear_logs
import os

app = FastAPI()

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
