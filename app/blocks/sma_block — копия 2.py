
import pandas as pd
import sqlite3
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import io
import base64
from datetime import timedelta
import plotly.graph_objects as go
from fastapi.responses import HTMLResponse

def analyze_sma(db_path, article):
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql(f"SELECT * FROM funnel WHERE article = {article}", conn, parse_dates=["date"])
        conn.close()

        if df.empty:
            return HTMLResponse("<p>⚠️ Нет данных по данному артикулу.</p>", status_code=200)

        df = df.sort_values("date").copy()
        df["SMA_3"] = df["orders"].rolling(window=3).mean()
        df["SMA_7"] = df["orders"].rolling(window=7).mean()

        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(df["date"], df["orders"], marker="o", label="Заказы", color="black")
        ax.plot(df["date"], df["SMA_3"], label="SMA 3 дня", color="orange")
        ax.plot(df["date"], df["SMA_7"], label="SMA 7 дней", color="blue")

        for i in range(1, len(df)):
            prev = df.iloc[i - 1]
            curr = df.iloc[i]
            if pd.notna(curr["SMA_3"]) and pd.notna(curr["SMA_7"]):
                color = "#d1f7c4" if curr["SMA_3"] > curr["SMA_7"] else "#ffd6d6"
                ax.axvspan(prev["date"], curr["date"], color=color, alpha=0.3)

        ax.set_title("Скользящее среднее заказов + зоны роста/спада")
        ax.set_xlabel("Дата")
        ax.set_ylabel("Количество заказов")
        ax.grid(True)
        ax.legend()
        fig.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        plt.close(fig)
        img_base64 = base64.b64encode(buf.getvalue()).decode("utf-8")

        fig_plotly = go.Figure()
        fig_plotly.add_trace(go.Scatter(
            x=df["date"], y=df["orders"], mode="lines+markers",
            name="Продажи", hovertemplate="Дата: %{x|%Y-%m-%d}<br>Продажи: %{y}<extra></extra>"
        ))
        fig_plotly.add_trace(go.Scatter(
            x=df["date"], y=df["SMA_3"], mode="lines", name="SMA 3 дня", line=dict(dash="dash")
        ))
        fig_plotly.add_trace(go.Scatter(
            x=df["date"], y=df["SMA_7"], mode="lines", name="SMA 7 дней", line=dict(dash="dot")
        ))
        fig_plotly.update_layout(
            title="Интерактивный график продаж",
            xaxis_title="Дата", yaxis_title="Заказы",
            hovermode="x unified"
        )
        interactive_html = fig_plotly.to_html(full_html=False, include_plotlyjs="cdn")

        last_date = df["date"].max()
        this_week = df[df["date"] > last_date - timedelta(days=7)]["orders"].sum()
        prev_week = df[(df["date"] > last_date - timedelta(days=14)) & (df["date"] <= last_date - timedelta(days=7))]["orders"].sum()

        if prev_week > 0:
            growth = round((this_week - prev_week) / prev_week * 100, 1)
            sign = "+" if growth >= 0 else "−"
            growth_text = f"{sign}{abs(growth)}%"
        else:
            growth_text = "недостаточно данных"

        html = f"""
        <h3>📈 Скользящее среднее</h3>
        <p>🟩 Зелёная зона — рост (SMA_3 > SMA_7)<br>
           🟥 Красная зона — падение (SMA_3 < SMA_7)</p>
        <img src="data:image/png;base64,{img_base64}" style="max-width:100%; margin-bottom:20px;">
        <h4>📊 Интерактивный график</h4>
        <div style='margin-bottom:20px;'>{interactive_html}</div>
        <h4>📆 Сравнение заказов</h4>
        <ul>
            <li>За последние 7 дней: <b>{this_week}</b> заказов</li>
            <li>Неделей ранее: <b>{prev_week}</b> заказов</li>
            <li>📊 Прирост: <b>{growth_text}</b></li>
        </ul>
        """

        return HTMLResponse(content=html, status_code=200)

    except Exception as e:
        import traceback
        error_text = traceback.format_exc()
        print("🔥 SMA BLOCK ERROR:", error_text)
        return HTMLResponse(
            content=f"<h3 style='color:red;'>🔥 Ошибка в SMA-блоке:</h3><pre>{error_text}</pre>",
            status_code=500
        )
