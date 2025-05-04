
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

    # Корреляция по всем дням
    if len(merged_sorted) > 1:
        full_corr = round(np.corrcoef(merged_sorted["baskets"], merged_sorted["cart_add"])[0, 1], 2)
    else:
        full_corr = 0

    # Корреляция только по дням с рекламой
    filtered = merged_sorted[(merged_sorted["shows"] > 200) & (merged_sorted["baskets"] > 0)]
    n_days = len(filtered)
    if n_days > 1:
        filtered_corr = round(np.corrcoef(filtered["baskets"], filtered["cart_add"])[0, 1], 2)
    else:
        filtered_corr = 0

    # Интерпретация по фильтрованной корреляции
    if filtered_corr > 0.6:
        interp = "✅ <b>Сильная положительная связь</b> — реклама сильно влияет на добавления в корзину"
    elif filtered_corr > 0.3:
        interp = "ℹ️ Умеренная положительная связь"
    elif filtered_corr > 0:
        interp = "⚠️ Слабая положительная связь"
    else:
        interp = "❌ Связь не обнаружена"

    # График
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=merged_sorted['date'], y=merged_sorted['baskets'],
                             mode='lines+markers', name='По рекламе', line=dict(color='orange')))
    fig.add_trace(go.Scatter(x=merged_sorted['date'], y=merged_sorted['cart_add'],
                             mode='lines+markers', name='Общие добавления', line=dict(color='red')))

    fig.update_layout(
        title=f"Влияние рекламы на общие добавления в корзину / артикул {article}",
        xaxis_title="Дата",
        yaxis_title="Количество",
        template="plotly_white"
    )

    html = (
        f"<p>📉 <b>Корреляция по всем дням:</b> {full_corr}</p>"
        f"<p>📈 <b>Корреляция по дням с рекламой:</b> {filtered_corr}</p>"
        f"<p>{interp}</p>"
        f"<p>Дней с показами > 200 и корзинами > 0: {n_days}</p>"
        + fig.to_html(full_html=False, include_plotlyjs='cdn')
    )

    return HTMLResponse(content=html)
