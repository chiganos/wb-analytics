
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
from sklearn.linear_model import LinearRegression

def analyze_ads_shows_to_baskets(db_path: str, article: int) -> str:
    article = int(article)

    conn = sqlite3.connect(db_path)
    ads = pd.read_sql_query("SELECT * FROM ads", conn)
    conn.close()

    ads['date'] = pd.to_datetime(ads['date'])
    df = ads[ads['article'].astype(str) == str(article)][['shows', 'baskets']].dropna()
    df = df[df['shows'] > 200]

    if len(df) < 5:
        return f"<p>❗ Недостаточно данных (показов > 200): найдено {len(df)}.</p>"

    corr = df['shows'].corr(df['baskets'])

    X = df[['shows']]
    y = df['baskets']
    model = LinearRegression()
    model.fit(X, y)
    y_pred = model.predict(X)

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.scatter(df['shows'], df['baskets'], alpha=0.6, label="Фактические данные")
    ax.plot(df['shows'], y_pred, color='green', label="Линейная регрессия")
    ax.set_xlabel("Показы рекламы (shows)")
    ax.set_ylabel("Добавления в корзину (baskets)")
    ax.set_title(f"Показы рекламы → baskets\nКорреляция: {corr:.2f}")
    ax.grid(True)
    ax.legend()
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    img_data = base64.b64encode(buf.getvalue()).decode('utf-8')
    img_tag = f"<img src='data:image/png;base64,{img_data}' style='max-width:100%; height:auto;'>"

    if corr > 0.6:
        comment = "✅ Сильная положительная связь — показы хорошо влияют на добавления"
    elif corr > 0.3:
        comment = "➕ Умеренная положительная связь"
    elif corr > 0:
        comment = "⚠️ Слабая положительная связь"
    elif corr < -0.3:
        comment = "❌ Отрицательная связь — больше показов → меньше корзин"
    else:
        comment = "ℹ️ Связь практически отсутствует"

    html = f'''
    <h3>📊 Влияние показов рекламы на добавления в корзину по рекламе</h3>
    <p>
        📈 Корреляция shows → baskets: <b>{corr:.2f}</b><br>
        <b>{comment}</b>
    </p>
    {img_tag}
    '''
    return html
