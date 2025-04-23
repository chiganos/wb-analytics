
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
from sklearn.linear_model import LinearRegression

def analyze_ads_baskets(db_path: str, article: int) -> str:
    article = int(article)

    conn = sqlite3.connect(db_path)
    ads = pd.read_sql_query("SELECT * FROM ads", conn)
    funnel = pd.read_sql_query("SELECT * FROM funnel", conn)
    conn.close()

    ads['date'] = pd.to_datetime(ads['date'])
    funnel['date'] = pd.to_datetime(funnel['date'])

    df_ads = ads[ads['article'].astype(str) == str(article)]
    df_funnel = funnel[funnel['article'].astype(str) == str(article)]
    df = df_ads.merge(df_funnel, on=['article', 'date'], how='inner')

    if df.empty:
        return "<p>❗ Нет данных после объединения по article и date.</p>"

    df['shows'] = pd.to_numeric(df['shows'], errors='coerce')
    df = df[df['shows'] > 200]

    if 'shows' not in df or 'baskets' not in df or 'cart_add' not in df:
        return "<p>❗ Не найдены нужные колонки.</p>"

    if len(df) < 3:
        return f"<p>❗ Недостаточно данных (дней с показами > 200): {len(df)}</p>"

    corr = df['baskets'].corr(df['cart_add'])

    X = df[['baskets']].values
    y = df['cart_add'].values
    model = LinearRegression().fit(X, y)
    line = model.predict(X)

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.scatter(df['baskets'], df['cart_add'], alpha=0.6, label='Данные')
    ax.plot(df['baskets'], line, color='red', label='Линейная регрессия')
    ax.set_title(f'Корреляция baskets → cart_add\nАртикул {article}, r = {corr:.2f}')
    ax.set_xlabel("Добавления в корзину по рекламе (baskets)")
    ax.set_ylabel("Общие добавления в корзину (cart_add)")
    ax.legend()
    ax.grid(True)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    img_data = base64.b64encode(buf.getvalue()).decode('utf-8')
    img_tag = f'<img src="data:image/png;base64,{img_data}" style="max-width:100%;">'

    if corr > 0.6:
        msg = "✅ Сильная положительная связь — реклама сильно влияет на добавления"
    elif corr > 0.3:
        msg = "➕ Умеренная связь — реклама влияет, но не решающий фактор"
    elif corr > 0:
        msg = "⚠️ Слабая связь — влияние есть, но слабое"
    else:
        msg = "❌ Нет связи или она отрицательная"

    html_output = (
        f"<h3>📊 Анализ рекламы: зависимость общих добавлений в корзину от добавления в корзину по рекламе → cart_add</h3>"
        f"<p><b>📈 Корреляция:</b> {corr:.2f}</p>"
        f"<p>{msg}</p>"
        f"<p>Дней с показами &gt; 200: {len(df)}</p>"
        f"{img_tag}"
    )

    return html_output
