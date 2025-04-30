import sqlite3
import pandas as pd
import plotly.graph_objects as go
from fastapi.responses import HTMLResponse

def analyze_ads_impact(db_path: str, article: int) -> str:
    try:
        article = str(article).strip().replace("'", "")
        conn = sqlite3.connect(db_path)
        ads = pd.read_sql(f"SELECT date, orders FROM ads WHERE article = '{article}'", conn)
        funnel = pd.read_sql(f"SELECT date, orders FROM funnel WHERE article = '{article}'", conn)
        conn.close()

        if ads.empty or funnel.empty:
            return HTMLResponse("<p>❌ Нет данных по артикулу.</p>", status_code=200)

        ads["date"] = pd.to_datetime(ads["date"])
        funnel["date"] = pd.to_datetime(funnel["date"])

        df = pd.merge(funnel, ads, on="date", how="outer", suffixes=("_total", "_ads"))
        df["orders_total"] = pd.to_numeric(df["orders_total"], errors="coerce").fillna(0)
        df["orders_ads"] = pd.to_numeric(df["orders_ads"], errors="coerce").fillna(0)
        df = df.dropna(subset=["date"])
        df = df.sort_values("date")

        # Разделение
        with_ads = df[df["orders_ads"] > 0]
        without_ads = df[df["orders_ads"] == 0]

        avg_with = round(with_ads["orders_total"].mean(), 2) if not with_ads.empty else 0.0
        avg_without = round(without_ads["orders_total"].mean(), 2) if not without_ads.empty else 0.0

        total_days = len(df)
        days_with_ads = len(with_ads)
        days_without_ads = len(without_ads)

        corr = round(df["orders_ads"].corr(df["orders_total"]), 2)

        # Комментарий
        if days_with_ads < 3:
            effect_text = "⚠️ <b>Мало дней с активной рекламой — выводы условны.</b>"
        elif avg_with > avg_without * 1.5:
            effect_text = "✅ <b>Реклама заметно увеличивает продажи.</b>"
        elif avg_with > avg_without * 1.1:
            effect_text = "➕ <b>Реклама даёт умеренный рост.</b>"
        else:
            effect_text = "❌ <b>Реклама почти не влияет на продажи.</b>"

        # График
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["orders_total"],
            name="Общие заказы", mode="lines+markers", line=dict(color="blue")
        ))
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["orders_ads"],
            name="Рекламные заказы", mode="lines+markers", line=dict(color="green")
        ))

        fig.update_layout(
            title="📊 Сравнение рекламных и общих заказов",
            xaxis_title="Дата",
            yaxis_title="Количество заказов",
            height=400
        )

        html = f"""
        <h3>📢 Влияние рекламных заказов на общие</h3>
        <p>
            📦 Всего дней: <b>{total_days}</b><br>
            📣 С рекламой: <b>{days_with_ads}</b>, 🚫 Без рекламы: <b>{days_without_ads}</b><br>
            📈 Ср. заказы без рекламы: <b>{avg_without}</b><br>
            📊 Ср. заказы с рекламой: <b>{avg_with}</b><br>
            📉 Корреляция между рекламными и общими заказами: <b>{corr}</b><br>
            {effect_text}
        </p>
        {fig.to_html(include_plotlyjs='cdn', full_html=False)}
        """

        return HTMLResponse(html, status_code=200)

    except Exception as e:
        return HTMLResponse(f"<p>❌ Ошибка выполнения: {e}</p>", status_code=500)
