import sqlite3
import pandas as pd
import plotly.graph_objects as go
from fastapi.responses import HTMLResponse

def analyze_cpm_vs_shows(db_path: str, article: str):
    conn = sqlite3.connect(db_path)
    ads = pd.read_sql("SELECT * FROM ads WHERE article = ?", conn, params=(article,))
    calculated = pd.read_sql("SELECT * FROM calculated WHERE article = ?", conn, params=(article,))
    conn.close()

    if ads.empty or calculated.empty:
        return HTMLResponse("<p>❌ Недостаточно данных для анализа.</p>")

    ads["date"] = pd.to_datetime(ads["date"])
    calculated["date"] = pd.to_datetime(calculated["date"])

    merged = pd.merge(ads, calculated[["date", "CPM"]], on="date", how="inner")

    if merged.empty or merged["shows"].sum() == 0:
        return HTMLResponse("<p>⚠️ Недостаточно пересечений по дате между ads и calculated.</p>")

    correlation = merged["CPM"].corr(merged["shows"])

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=merged["date"],
        y=merged["CPM"],
        name="CPM",
        mode="lines+markers",
        line=dict(color="red")
    ))
    fig.add_trace(go.Scatter(
        x=merged["date"],
        y=merged["shows"],
        name="Показы",
        mode="lines+markers",
        yaxis="y2",
        line=dict(color="blue")
    ))

    fig.update_layout(
        title=f"Зависимость CPM от показов / артикул {article}",
        xaxis_title="Дата",
        yaxis=dict(title=dict(text="CPM", font=dict(color="red")), tickfont=dict(color="red")),
        yaxis2=dict(title=dict(text="Показы", font=dict(color="blue")), tickfont=dict(color="blue"), overlaying="y", side="right"),
        legend=dict(x=0.01, y=0.99),
        hovermode="x unified"
    )

    comment = ""
    if correlation > 0.5:
        comment = "✅ Сильная положительная связь — при росте показов увеличивается CPM"
    elif correlation < -0.5:
        comment = "🔻 Сильная отрицательная связь — CPM падает при увеличении показов"
    else:
        comment = "🟡 Связь слабая или отсутствует"

    summary = f"<p>📈 <b>Корреляция CPM и показов:</b> {round(correlation, 2)}</p><p>{comment}</p>"

    return HTMLResponse(content=summary + fig.to_html(full_html=False, include_plotlyjs='cdn'))
