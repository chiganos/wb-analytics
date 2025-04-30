import sqlite3
import pandas as pd
import plotly.graph_objects as go
from fastapi.responses import HTMLResponse

def analyze_price_vs_orders(db_path: str, article: str):
    try:
        conn = sqlite3.connect(db_path)
        query = f"SELECT date, price, orders FROM funnel WHERE article = '{article}'"
        df = pd.read_sql(query, conn)
        conn.close()

        if df.empty:
            return HTMLResponse("<p>❌ Нет данных по артикулу.</p>", status_code=200)

        # Приведение типов
        df["orders"] = pd.to_numeric(df["orders"], errors="coerce")
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["date"] = pd.to_datetime(df["date"])

        df = df[["date", "price", "orders"]].dropna()
        df = df[df["orders"] > 0]
        df = df.sort_values("date")

        if len(df) < 3:
            return HTMLResponse("<p>❗ Недостаточно данных для анализа (менее 3 строк с заказами).</p>", status_code=200)

        corr = round(df["price"].corr(df["orders"]), 2)

        if corr < -0.6:
            comment = "🔻 <b>Сильная отрицательная связь</b> — снижение цены приводит к росту заказов"
        elif corr < -0.3:
            comment = "🟠 <b>Умеренная отрицательная связь</b> — цена влияет на заказы"
        elif corr < -0.1:
            comment = "⚠️ <b>Слабая отрицательная связь</b> — возможно влияние, но слабое"
        else:
            comment = "⚪ <b>Связь почти отсутствует</b> — цена не влияет на заказы"

        # График с двумя Y-осями
        fig = go.Figure()

        # Левая ось Y — заказы
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["orders"],
            name="Заказы", mode="lines+markers", line=dict(color="red"),
            yaxis="y1"
        ))

        # Правая ось Y — цена
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["price"],
            name="Цена", mode="lines+markers", line=dict(color="orange"),
            yaxis="y2"
        ))

        fig.update_layout(
            title="📊 Динамика цены и количества заказов по времени",
            xaxis=dict(title="Дата"),
            yaxis=dict(title="Заказы", side="left"),
            yaxis2=dict(title="Цена", overlaying="y", side="right"),
            height=400
        )

        html = f"""
        <h3>📦 Влияние цены на количество заказов</h3>
        <p>📈 <b>Корреляция между ценой и заказами:</b> {corr}</p>
        <p>{comment}</p>
        """

        return HTMLResponse(html + fig.to_html(include_plotlyjs=False, full_html=False), status_code=200)

    except Exception as e:
        return HTMLResponse(f"<p>❌ Ошибка выполнения: {e}</p>", status_code=500)
