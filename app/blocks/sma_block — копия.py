
import pandas as pd
import sqlite3
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import io
import base64
from datetime import timedelta

def analyze_sma(db_path, article):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(f"SELECT * FROM funnel WHERE article = {article}", conn, parse_dates=['date'])
    conn.close()

    if df.empty:
        return "<p>⚠️ Нет данных по данному артикулу.</p>"

    df = df.sort_values("date").copy()
    df["SMA_3"] = df["orders"].rolling(window=3).mean()
    df["SMA_7"] = df["orders"].rolling(window=7).mean()

    # Цветные зоны: рост (оранжевый > синий), спад (оранжевый < синий)
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

    # Сравнение с предыдущей неделей
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
    <h4>📆 Сравнение заказов</h4>
    <ul>
        <li>За последние 7 дней: <b>{this_week}</b> заказов</li>
        <li>Неделей ранее: <b>{prev_week}</b> заказов</li>
        <li>📊 Прирост: <b>{growth_text}</b></li>
    </ul>
    """

    return html
