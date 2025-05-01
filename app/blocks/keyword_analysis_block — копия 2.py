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

    # Группировка по дате и ключевым словам для open_card
    pos_grouped = positions.groupby(["date", "search_query"]).agg({"open_card": "sum"}).reset_index()
    funnel_grouped = funnel.groupby("date").agg({"cart_add": "sum"}).reset_index()

    merged = pd.merge(pos_grouped, funnel_grouped, on="date", how="inner")

    # Фильтр: только ключи с >100 переходов суммарно
    total_open = merged.groupby("search_query")["open_card"].sum().reset_index()
    total_open = total_open[total_open["open_card"] > 100]
    filtered = merged[merged["search_query"].isin(total_open["search_query"])]

    if filtered.empty:
        return HTMLResponse("<p>⚠️ Нет ключей с >100 переходами</p>")

    correlations = filtered.groupby("search_query").apply(
        lambda g: g["open_card"].corr(g["cart_add"])
    ).reset_index(name="correlation")

    # Добавим суммарные переходы для отображения
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

    top_keyword = result.sort_values("correlation", ascending=False).iloc[0]["search_query"]
    html_top = f"<p>🔍 <b>Самое эффективное ключевое слово</b>: {top_keyword}</p>"

    return HTMLResponse(content=html_top + fig.to_html(full_html=False, include_plotlyjs='cdn'))
