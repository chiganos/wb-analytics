import sqlite3
import pandas as pd
from fastapi.responses import HTMLResponse
import plotly.graph_objects as go
import numpy as np

def analyze_keywords(db_path: str, article: str):
    conn = sqlite3.connect(db_path)
    positions = pd.read_sql("SELECT * FROM positions WHERE article = ?", conn, params=(article,))
    funnel = pd.read_sql("SELECT * FROM funnel WHERE article = ?", conn, params=(article,))
    conn.close()

    if positions.empty or funnel.empty:
        return HTMLResponse("<p>❌ Недостаточно данных.</p>")

    positions["date"] = pd.to_datetime(positions["date"])
    funnel["date"] = pd.to_datetime(funnel["date"])

    pos_grouped = positions.groupby(["date", "search_query"]).agg({"open_card": "sum", "avg_position": "mean"}).reset_index()
    funnel_grouped = funnel.groupby("date").agg({"cart_add": "sum"}).reset_index()

    merged = pd.merge(pos_grouped, funnel_grouped, on="date", how="inner")

    total_open = merged.groupby("search_query")["open_card"].sum().reset_index()
    total_open = total_open[total_open["open_card"] > 100]
    filtered = merged[merged["search_query"].isin(total_open["search_query"])]

    if filtered.empty:
        return HTMLResponse("<p>⚠️ Нет ключей с >100 переходами</p>")

    correlations = filtered.groupby("search_query").apply(
        lambda g: g["open_card"].corr(g["cart_add"])
    ).reset_index(name="correlation")

    total_transitions = filtered.groupby("search_query")["open_card"].sum().reset_index()
    result = pd.merge(correlations, total_transitions, on="search_query")
    result = result.dropna().sort_values("open_card", ascending=False).head(20)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=result["search_query"],
        y=result["open_card"],
        name="Переходы",
        marker_color="orange"
    ))
    fig.add_trace(go.Scatter(
        x=result["search_query"],
        y=result["correlation"],
        name="Корреляция",
        mode="lines+markers",
        yaxis="y2",
        line=dict(color="green")
    ))

    fig.update_layout(
        title=f"Ключевые слова / артикул {article}",
        xaxis_title="Ключевое слово",
        yaxis=dict(title="Переходы"),
        yaxis2=dict(title="Корреляция", overlaying="y", side="right", showgrid=False, range=[-1, 1]),
        legend=dict(x=0.01, y=0.99),
        hovermode="x unified"
    )

    html_blocks = []
    top_keyword = result.sort_values("correlation", ascending=False).iloc[0]["search_query"]
    html_top = f"<p>🔍 <b>Самое эффективное ключевое слово</b>: {top_keyword}</p>"

    html_blocks.append(html_top)
    html_blocks.append(fig.to_html(full_html=False, include_plotlyjs='cdn'))
    html_blocks.append("<h3>📊 Детальный анализ по каждому ключевому слову</h3>")

    for _, row in result.iterrows():
        keyword = row["search_query"]
        corr = round(row["correlation"], 3)
        total = int(row["open_card"])
        subset = filtered[filtered["search_query"] == keyword]

        fig_kw1 = go.Figure()
        fig_kw1.add_trace(go.Scatter(x=subset["date"], y=subset["open_card"], name="Переходы", mode="lines+markers"))
        fig_kw1.add_trace(go.Scatter(x=subset["date"], y=subset["cart_add"], name="Добавления в корзину", mode="lines+markers"))
        fig_kw1.update_layout(title="Динамика переходов и корзины", xaxis_title="Дата", hovermode="x unified")

        fig_kw2 = go.Figure()
        fig_kw2.add_trace(go.Scatter(x=subset["date"], y=subset["avg_position"], name="Позиция", mode="lines+markers"))
        fig_kw2.update_layout(title="Динамика позиции в выдаче", xaxis_title="Дата", yaxis_title="Позиция", hovermode="x unified")

        fig_kw3 = go.Figure()
        fig_kw3.add_trace(go.Scatter(x=subset["avg_position"], y=subset["open_card"], mode="markers", name="Позиция vs Переходы"))
        fig_kw3.update_layout(title="Позиция vs Переходы", xaxis_title="avg_position", yaxis_title="open_card")

        html_blocks.append(f"""
            <details style='margin-bottom:1em;'>
              <summary>🔑 {keyword} — Корреляция: {corr}, Переходов: {total}</summary>
              {fig_kw1.to_html(full_html=False, include_plotlyjs=False)}
              {fig_kw2.to_html(full_html=False, include_plotlyjs=False)}
              {fig_kw3.to_html(full_html=False, include_plotlyjs=False)}
            </details>
        """)

    return HTMLResponse(content="".join(html_blocks))
