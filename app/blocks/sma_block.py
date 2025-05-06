import pandas as pd
import sqlite3
import plotly.graph_objects as go
from fastapi.responses import HTMLResponse

def analyze_sma(db_path, article):
    try:
        conn = sqlite3.connect(db_path)
        query = f"SELECT date, orders, cart_add FROM funnel WHERE article = '{article}'"
        df = pd.read_sql(query, conn)
        conn.close()

        if df.empty:
            return HTMLResponse("<h2>Нет данных для отображения</h2>", status_code=200)

        # Обработка дат
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')

        # Расчёт прироста за последние 7 дней vs предыдущие 7 дней
        last_7 = df[df['date'] >= df['date'].max() - pd.Timedelta(days=6)]['orders'].sum()
        prev_7 = df[(df['date'] < df['date'].max() - pd.Timedelta(days=6))
                    & (df['date'] >= df['date'].max() - pd.Timedelta(days=13))]['orders'].sum()
        if pd.isna(prev_7) or prev_7 == 0:
            growth = float('inf')
        else:
            growth = (last_7 - prev_7) / prev_7 * 100
        growth_str = f"{growth:.1f}%" if growth != float('inf') else "∞"

        # Расчёт корреляции
        corr = df['orders'].corr(df['cart_add'])
        corr_str = f"{corr:.2f}" if not pd.isna(corr) else "–"

        summary_html = f"""
        <h3>📅 Сравнение заказов</h3>
        <ul>
          <li>За последние 7 дней: <b>{int(last_7)}</b> заказов</li>
          <li>Неделей ранее: <b>{int(prev_7)}</b> заказов</li>
          <li>📊 Прирост: <b>{growth_str}</b></li>
        </ul>
        <p>🔗 Корреляция между заказами и добавлениями в корзину: <b>{corr_str}</b></p>
        """

        # Построение графика через Plotly
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df['date'], y=df['orders'],
            mode='lines+markers', name='Продажи', line=dict(color='red')
        ))
        fig.add_trace(go.Scatter(
            x=df['date'], y=df['cart_add'],
            mode='lines+markers', name='Добавления в корзину', line=dict(color='orange'), yaxis='y2'
        ))

        fig.update_layout(
            xaxis_title="Дата",
            yaxis=dict(
                title_text="Продажи",
                title_font_color="red",
                tickfont_color="red"
            ),
            yaxis2=dict(
                title_text="Добавления в корзину",
                title_font_color="orange",
                tickfont_color="orange",
                overlaying="y",
                side="right"
            ),
            hovermode="x unified",
            legend=dict(x=0.01, y=0.99)
        )

        html = summary_html + fig.to_html(full_html=False, include_plotlyjs='cdn')
        return HTMLResponse(content=html)

    except Exception as e:
        return HTMLResponse(content=f"<h2>Ошибка: {str(e)}</h2>", status_code=500)