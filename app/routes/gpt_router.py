from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from app.blocks import gpt_block

router = APIRouter()

@router.get("/api/analytics/gpt/{article}", response_class=HTMLResponse)
def gpt_analysis(article: int):
    result = gpt_block.analyze_article_with_gpt(article)
    return HTMLResponse(content=result)
