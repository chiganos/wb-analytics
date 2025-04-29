import pandas as pd
import sqlite3
import plotly.graph_objects as go
from fastapi.responses import HTMLResponse

def analyze_sma(db_path, article):
    try:
        conn = sqlite3.connect(db_path)
        query = f"SELECT date, orders FROM funnel WHERE article = '{article}'"
        df = pd.read_sql(query, conn)
        conn.close()

        if df.empty:
            return HTMLResponse("<h2>Нет данных для отображения</h2>", status_code=200)

        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')

        # Скользящая средняя
        df['sma'] = df['orders'].rolling(window=7, min_periods=1).mean()

        # Расчёт прироста за последние 7 дней vs предыдущие 7
        last_7 = df[df["date"] >= df["date"].max() - pd.Timedelta(days=6)]["orders"].sum()
        prev_7 = df[(df["date"] < df["date"].max() - pd.Timedelta(days=6)) & (df["date"] >= df["date"].max() - pd.Timedelta(days=13))]["orders"].sum()

        if pd.isna(prev_7) or prev_7 == 0:
            growth = float("inf")
        else:
            growth = (last_7 - prev_7) / prev_7 * 100

        growth_str = f"{growth:.1f}%" if growth != float("inf") else "∞"

        summary_html = f"""
        <h3>📅 Сравнение заказов</h3>
        <ul>
          <li>За последние 7 дней: <b>{last_7}</b> заказов</li>
          <li>Неделей ранее: <b>{prev_7}</b> заказов</li>
          <li>📊 Прирост: <b>{growth_str}</b></li>
        </ul>
        """

        # Построение графика через Plotly
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df['date'], y=df['orders'], mode='lines+markers', name='Продажи'))
        fig.add_trace(go.Scatter(x=df['date'], y=df['sma'], mode='lines', name='7-дневное скользящее среднее'))

        fig.update_layout(
            title=f"SMA по артикулу {article}",
            xaxis_title="Дата",
            yaxis_title="Продажи",
            hovermode="x unified"
        )

        html = summary_html + fig.to_html(full_html=False, include_plotlyjs='cdn')
        return HTMLResponse(content=html)

    except Exception as e:
        return HTMLResponse(content=f"<h2>Ошибка: {str(e)}</h2>", status_code=500)