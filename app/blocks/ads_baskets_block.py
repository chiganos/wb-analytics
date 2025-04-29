import sqlite3
import pandas as pd
import plotly.graph_objects as go
from fastapi.responses import HTMLResponse
import numpy as np

def analyze_ads_baskets(db_path: str, article: str):
    conn = sqlite3.connect(db_path)
    ads = pd.read_sql("SELECT * FROM ads WHERE article = ?", conn, params=(article,))
    funnel = pd.read_sql("SELECT * FROM funnel WHERE article = ?", conn, params=(article,))
    conn.close()

    ads["date"] = pd.to_datetime(ads["date"])
    funnel["date"] = pd.to_datetime(funnel["date"])

    merged = pd.merge(ads, funnel, on=["article", "date"], how="inner")
    merged_sorted = merged.sort_values("date")

    # Статистика по фильтрации
    filtered = merged_sorted[(merged_sorted["shows"] > 200) & (merged_sorted["baskets"] > 0)]
    n_days = len(filtered)

    # Корреляция baskets → cart_add
    correlation = np.corrcoef(filtered["baskets"], filtered["cart_add"])[0, 1] if n_days > 1 else 0
    correlation = round(correlation, 2)

    # Интерпретация
    if correlation > 0.6:
        interp = "✅ <b>Сильная положительная связь</b> — реклама сильно влияет на добавления в корзину"
    elif correlation > 0.3:
        interp = "ℹ️ Умеренная положительная связь"
    elif correlation > 0:
        interp = "⚠️ Слабая положительная связь"
    else:
        interp = "❌ Связь не обнаружена"

    # Plotly график baskets vs cart_add по дате
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=merged_sorted['date'], y=merged_sorted['baskets'],
                             mode='lines+markers', name='По рекламе',
                             line=dict(color='orange')))
    fig.add_trace(go.Scatter(x=merged_sorted['date'], y=merged_sorted['cart_add'],
                             mode='lines+markers', name='Общие добавления',
                             line=dict(color='red')))

    fig.update_layout(
        title=f"Влияние рекламы на общие добавления в корзину / артикул {article}",
        xaxis_title="Дата",
        yaxis_title="Количество",
        hovermode="x unified"
    )

    html_summary = f"""
    <p>📉 <b>Корреляция</b>: {correlation}</p>
    <p>{interp}</p>
    <p>Дней с показами > 200: {n_days}</p>
    """

    full_html = html_summary + fig.to_html(full_html=False, include_plotlyjs='cdn')
    return HTMLResponse(content=full_html)
