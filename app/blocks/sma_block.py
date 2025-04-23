
import pandas as pd
import sqlite3
import matplotlib
matplotlib.use("Agg")  # Обязательно для macOS, чтобы не крашилось!
import matplotlib.pyplot as plt
import io
import base64


def analyze_sma(db_path, article):
    # Загружаем данные
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(f"SELECT * FROM funnel WHERE article = {article}", conn, parse_dates=['date'])
    conn.close()

    if df.empty:
        return "<p>⚠️ Нет данных по данному артикулу.</p>"

    df = df.sort_values("date")
    df["SMA_3"] = df["orders"].rolling(window=3).mean()
    df["SMA_7"] = df["orders"].rolling(window=7).mean()

    # Генерация графика
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(df["date"], df["orders"], marker="o", label="Заказы")
    ax.plot(df["date"], df["SMA_3"], label="SMA 3 дня")
    ax.plot(df["date"], df["SMA_7"], label="SMA 7 дней")
    ax.set_title("Скользящее среднее заказов")
    ax.set_xlabel("Дата")
    ax.set_ylabel("Количество заказов")
    ax.grid(True)
    ax.legend()
    fig.tight_layout()

    # Сохраняем график в base64
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    image_base64 = base64.b64encode(buf.read()).decode("utf-8")

    # Генерация текста-резюме
    sma_3 = df["orders"].tail(3).mean()
    avg = df["orders"].mean()
    delta = sma_3 - avg
    trend = "⬆️ рост" if delta > 0 else "⬇️ падение" if delta < 0 else "↔️ без изменений"

    summary = (
        f"<b>📊 Анализ скользящего среднего заказов:</b><br>"
        f"- Последнее SMA (3 дня): <b>{sma_3:.2f}</b><br>"
        f"- Среднее за период: <b>{avg:.2f}</b><br>"
        f"- Разница: <b>{delta:+.2f}</b> ({trend})"
    )

    html = (
        f"<p>{summary}</p>"
        f"<img src='data:image/png;base64,{image_base64}' style='max-width:100%; height:auto;'>"
    )

    return html
