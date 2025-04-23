from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
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
from app.blocks import gpt_block
from app.routes import init_context

import os

router = APIRouter()
templates = Jinja2Templates(directory="templates")
DB_PATH = os.path.join(os.path.dirname(__file__), "../../data/wb.db")

@router.get("/analyze/{article}", response_class=HTMLResponse)
def combined_analysis(request: Request, article: int):
    context_parts = []

    html1, ctx1 = sma_block.analyze_sma(DB_PATH, article)
    html2, ctx2 = ads_baskets_block.analyze_ads_baskets(DB_PATH, article)
    html3, ctx3 = ads_impact_block.analyze_ads_impact(DB_PATH, article)
    html4, ctx4 = price_orders_block.analyze_price_impact(DB_PATH, article)
    html5, ctx5 = price_optimization_block.analyze_price_optimization(DB_PATH, article)
    html6, ctx6 = price_conversion_block.analyze_price_conversion(DB_PATH, article)
    html7, ctx7 = ads_shows_to_baskets_block.analyze_ads_shows_to_baskets(DB_PATH, article)
    html8, ctx8 = cpm_to_shows_block.analyze_cpm_to_shows(DB_PATH, article)
    html9, ctx9 = keyword_analysis_block.analyze_keywords(DB_PATH, article)
    html10, ctx10 = gpt_block.analyze_with_gpt(DB_PATH, article)

    # Собираем контекст
    context_text = "\n".join([ctx for ctx in [ctx1, ctx2, ctx3, ctx4, ctx5, ctx6, ctx7, ctx8, ctx9] if ctx])

    # Сохраняем в context_by_article
    init_context.context_by_article[article] = context_text

    return templates.TemplateResponse("combined.html", {
        "request": request,
        "article": article,
        "block1": html1,
        "block2": html2,
        "block3": html3,
        "block4": html4,
        "block5": html5,
        "block6": html6,
        "block7": html7,
        "block8": html8,
        "block9": html9,
        "block10": html10,
    })
