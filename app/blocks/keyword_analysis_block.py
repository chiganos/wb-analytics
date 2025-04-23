import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import io
import base64

def analyze_keywords(db_path: str, article: int) -> str:
    article = int(article)

    conn = sqlite3.connect(db_path)
    positions = pd.read_sql_query("SELECT * FROM positions", conn)
    funnel = pd.read_sql_query("SELECT * FROM funnel", conn)
    conn.close()

    positions['date'] = pd.to_datetime(positions['date'])
    funnel['date'] = pd.to_datetime(funnel['date'])

    pos = positions[positions['article'].astype(str) == str(article)]
    fun = funnel[funnel['article'].astype(str) == str(article)]

    if pos.empty or fun.empty:
        return "<p>❗ Нет данных для выбранного артикула.</p>"

    popular_keywords = (
        pos.groupby('search_query')['open_card']
        .sum().reset_index()
        .query("open_card > 100")['search_query']
    )

    results = []
    details_html = ""

    for keyword in popular_keywords:
        pos_kw = pos[pos['search_query'] == keyword]
        daily_open = pos_kw.groupby('date')['open_card'].sum().reset_index()
        merged = pd.merge(daily_open, fun[['date', 'cart_add']], on='date', how='inner')
        if len(merged) >= 5:
            corr = merged['open_card'].corr(merged['cart_add'])
            total_opens = daily_open['open_card'].sum()
            results.append({
                'Ключевое слово': keyword,
                'Корреляция': round(corr, 3),
                'Переходов всего': int(total_opens)
            })

            images = []

            # График 1: open_card vs cart_add
            fig1, ax1 = plt.subplots(figsize=(10, 4))
            sns.lineplot(data=merged, x='date', y='open_card', label='Переходы (open_card)', ax=ax1)
            sns.lineplot(data=merged, x='date', y='cart_add', label='Добавления в корзину (cart_add)', ax=ax1)
            ax1.set_title(f"Влияние ключевого слова '{keyword}' на корзины")
            ax1.grid(True)
            ax1.legend()
            fig1.tight_layout()
            buf1 = io.BytesIO()
            fig1.savefig(buf1, format="png")
            plt.close(fig1)
            images.append(base64.b64encode(buf1.getvalue()).decode("utf-8"))

            # График 2: avg_position vs open_card
            merged2 = pos_kw[['date', 'avg_position', 'open_card']].dropna()
            merged2 = merged2.groupby('date').agg({'avg_position': 'mean', 'open_card': 'sum'}).reset_index()
            fig2, ax2 = plt.subplots(figsize=(7, 4))
            sns.scatterplot(data=merged2, x='avg_position', y='open_card', ax=ax2)
            ax2.set_title(f"Позиция vs Переходы по слову '{keyword}'")
            ax2.grid(True)
            fig2.tight_layout()
            buf2 = io.BytesIO()
            fig2.savefig(buf2, format="png")
            plt.close(fig2)
            images.append(base64.b64encode(buf2.getvalue()).decode("utf-8"))

            # График 3: динамика позиции
            fig3, ax3 = plt.subplots(figsize=(10, 4))
            sns.lineplot(data=merged2, x='date', y='avg_position', ax=ax3)
            ax3.set_title(f"Динамика позиции в выдаче по слову '{keyword}'")
            ax3.grid(True)
            fig3.tight_layout()
            buf3 = io.BytesIO()
            fig3.savefig(buf3, format="png")
            plt.close(fig3)
            images.append(base64.b64encode(buf3.getvalue()).decode("utf-8"))

            image_tags = "".join(
                f"<img src='data:image/png;base64,{img}' style='max-width:100%; margin-bottom:20px;'>" for img in images
            )

            details_html += (
                f"<details style='margin-bottom:20px;'>"
                f"<summary style='font-weight:bold;'>🔑 {keyword} — Корреляция: {round(corr, 3)}, Переходов: {total_opens}</summary>"
                f"<div style='margin-top:10px;'>{image_tags}</div>"
                f"</details>"
            )

    result_df = pd.DataFrame(results).sort_values(by='Корреляция', ascending=False)
    if result_df.empty:
        return "<p>❗ Нет подходящих ключевых слов с open_card > 100</p>"

    top_kw = result_df.iloc[0]['Ключевое слово']
    top_corr = result_df.iloc[0]['Корреляция']
    top_open = result_df.iloc[0]['Переходов всего']

    table_html = "<table border='1' cellspacing='0' cellpadding='6'><tr><th>Ключевое слово</th><th>Корреляция</th><th>Переходов всего</th></tr>"
    for _, row in result_df.iterrows():
        table_html += f"<tr><td>{row['Ключевое слово']}</td><td>{row['Корреляция']}</td><td>{row['Переходов всего']}</td></tr>"
    table_html += "</table>"

    html = (
        f"<h3>🔍 Анализ ключевых слов в органике</h3>"
        f"<p>📦 Артикул: <b>{article}</b><br>"
        f"🔠 Топ-ключевое слово: <b>{top_kw}</b><br>"
        f"📈 Корреляция: <b>{top_corr}</b><br>"
        f"👁️ Всего переходов: <b>{top_open}</b></p>"
        f"<hr><h4>📋 Все ключевые слова с >100 переходами</h4>{table_html}"
        f"<hr><h4>📊 Детальный анализ по каждому ключевому слову</h4>{details_html}"
    )

    return html
