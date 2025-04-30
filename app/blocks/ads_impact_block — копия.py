
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64

def analyze_ads_impact(db_path: str, article: int) -> str:
    article = int(article)

    conn = sqlite3.connect(db_path)
    ads = pd.read_sql_query("SELECT * FROM ads", conn)
    funnel = pd.read_sql_query("SELECT * FROM funnel", conn)
    conn.close()

    ads['date'] = pd.to_datetime(ads['date'])
    funnel['date'] = pd.to_datetime(funnel['date'])

    df_ads = ads[ads['article'].astype(str) == str(article)].copy()
    df_funnel = funnel[funnel['article'].astype(str) == str(article)].copy()
    df = df_ads.merge(df_funnel, on=['article', 'date'], how='inner')

    if df.empty:
        return "<p>❗ Нет данных по выбранному артикулу.</p>"

    df = df.sort_values(by='date')
    df['orders_ads'] = df['orders_x']
    df['orders_total'] = df['orders_y']

    ad_days = df[df['orders_ads'] > 0]
    no_ad_days = df[df['orders_ads'] == 0]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(df['date'], df['orders_total'], label='Общие заказы (funnel)', linewidth=2)
    ax.plot(df['date'], df['orders_ads'], label='Рекламные заказы (ads)', linewidth=2)
    ax.set_xlabel("Дата")
    ax.set_ylabel("Количество заказов")
    ax.set_title(f"Сравнение продаж: общие vs реклама\nАртикул {article}")
    ax.legend()
    ax.grid(True)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    image_base64 = base64.b64encode(buf.getvalue()).decode("utf-8")
    img_tag = f"<img src='data:image/png;base64,{image_base64}' style='max-width:100%; height:auto;'>"

    total_days = len(df)
    days_with_ads = len(ad_days)
    days_without_ads = len(no_ad_days)
    avg_orders_with_ads = ad_days['orders_total'].mean() if not ad_days.empty else 0
    avg_orders_without_ads = no_ad_days['orders_total'].mean() if not no_ad_days.empty else 0

    if days_with_ads < 5:
        comment = "⚠️ Мало дней с активной рекламой — выводы условны."
    elif avg_orders_with_ads > avg_orders_without_ads * 1.5:
        comment = "✅ Реклама заметно увеличивает продажи."
    elif avg_orders_with_ads > avg_orders_without_ads * 1.1:
        comment = "➕ Реклама даёт умеренный рост."
    else:
        comment = "❌ Реклама почти не влияет на продажи."

    html = f'''
    <h3>📊 Анализ влияния рекламы на общие заказы</h3>
    <p>
        📦 Всего дней: <b>{total_days}</b><br>
        📢 С рекламой: <b>{days_with_ads}</b>, 🚫 Без рекламы: <b>{days_without_ads}</b><br>
        📊 Ср. заказы без рекламы: <b>{avg_orders_without_ads:.2f}</b><br>
        📊 Ср. заказы с рекламой: <b>{avg_orders_with_ads:.2f}</b><br>
        <b>{comment}</b>
    </p>
    {img_tag}
    '''

    return html
