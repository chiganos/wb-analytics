import sqlite3
import pandas as pd
import plotly.graph_objects as go
from fastapi.responses import HTMLResponse
import numpy as np

def analyze_price_conversion(db_path: str, article: str):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql("SELECT * FROM funnel WHERE article = ?", conn, params=(article,))
    conn.close()

    if df.empty or "price" not in df.columns or "orders" not in df.columns or "cart_to_order_conv" not in df.columns:
        return HTMLResponse("<p>❌ Недостаточно данных для анализа.</p>", status_code=200)

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    # Расчёт корреляции между ценой и заказами
    corr = df["price"].corr(df["orders"])
    corr = round(corr, 2)

    # Интерпретация
    if corr > 0.6:
        comment = "✅ <b>Сильная положительная связь</b> — при росте цены заказы тоже растут"
    elif corr > 0.3:
        comment = "🟢 Умеренная связь — цена влияет на заказы"
    elif corr > 0.1:
        comment = "🟡 Слабая связь"
    elif corr > -0.1:
        comment = "🟡 <b>Связь почти отсутствует</b>"
    elif corr > -0.3:
        comment = "🟠 Слабая отрицательная связь"
    elif corr > -0.6:
        comment = "🔻 Умеренная отрицательная связь — чем выше цена, тем меньше заказов"
    else:
        comment = "❌ <b>Сильная отрицательная связь</b> — высокая цена негативно влияет на заказы"

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["price"],
        mode="lines+markers",
        name="Цена",
        line=dict(color="blue")
    ))
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["cart_to_order_conv"],
        mode="lines+markers",
        name="Конверсия корзина → заказ",
        yaxis="y2",
        line=dict(color="green")
    ))

    fig.update_layout(
        title=f"Влияние цены на конверсию / артикул {article}",
        xaxis=dict(title="Дата"),
        yaxis=dict(title="Цена", side="left"),
        yaxis2=dict(title="Конверсия", overlaying="y", side="right", showgrid=False),
        legend=dict(x=0.01, y=0.99),
        hovermode="x unified"
    )

    html_comment = f"""
    <p>📉 <b>Корреляция между ценой и заказами</b>: {corr}</p>
    <p>{comment}</p>
    """

    return HTMLResponse(content=html_comment + fig.to_html(full_html=False, include_plotlyjs='cdn'))