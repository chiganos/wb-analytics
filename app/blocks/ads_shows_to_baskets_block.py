import sqlite3
import pandas as pd
import plotly.graph_objects as go
from fastapi.responses import HTMLResponse
import numpy as np

def analyze_ads_shows_to_baskets(db_path: str, article: str):
    conn = sqlite3.connect(db_path)
    ads = pd.read_sql("SELECT * FROM ads WHERE article = ?", conn, params=(article,))
    conn.close()

    if ads.empty or "shows" not in ads.columns or "baskets" not in ads.columns:
        return HTMLResponse("<p>❌ Недостаточно данных для анализа.</p>", status_code=200)

    ads["date"] = pd.to_datetime(ads["date"])
    ads = ads.sort_values("date")

    filtered = ads[(ads["shows"] > 200) & (ads["baskets"] > 0)]
    corr = filtered["shows"].corr(filtered["baskets"]) if len(filtered) > 1 else 0
    corr = round(corr, 2)

    if corr > 0.6:
        comment = "✅ <b>Сильная положительная связь</b> — показы ведут к росту корзин"
    elif corr > 0.3:
        comment = "🟢 Умеренная положительная связь"
    elif corr > 0.1:
        comment = "🟡 Слабая связь"
    elif corr > -0.1:
        comment = "🟡 <b>Связь почти отсутствует</b>"
    elif corr > -0.3:
        comment = "🟠 Слабая отрицательная связь"
    elif corr > -0.6:
        comment = "🔻 Умеренная отрицательная связь"
    else:
        comment = "❌ <b>Сильная отрицательная связь</b> — увеличение показов уменьшает корзины"

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=ads["date"], y=ads["shows"],
        mode="lines+markers",
        name="Показы",
        line=dict(color="royalblue")
    ))
    fig.add_trace(go.Scatter(
        x=ads["date"], y=ads["baskets"],
        mode="lines+markers",
        name="Корзины",
        yaxis="y2",
        line=dict(color="orangered")
    ))

    fig.update_layout(
        title=f"Влияние показов на корзины / артикул {article}",
        xaxis=dict(title="Дата"),
        yaxis=dict(title="Показы", side="left"),
        yaxis2=dict(title="Корзины", overlaying="y", side="right", showgrid=False),
        legend=dict(x=0.01, y=0.99),
        hovermode="x unified"
    )

    html_comment = f"""
    <p>📉 <b>Корреляция между показами и корзинами</b>: {corr}</p>
    <p>{comment}</p>
    <p>Фильтрация: показы > 200 и корзины > 0</p>
    """

    return HTMLResponse(content=html_comment + fig.to_html(full_html=False, include_plotlyjs='cdn'))
