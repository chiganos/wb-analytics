
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
from sklearn.linear_model import LinearRegression

def analyze_cpm_to_shows(db_path: str, article: int) -> str:
    article = int(article)

    conn = sqlite3.connect(db_path)
    ads = pd.read_sql_query("SELECT * FROM ads", conn)
    conn.close()

    ads['date'] = pd.to_datetime(ads['date'])

    # Расчёт CPM, если его нет
    if 'CPM' not in ads.columns or ads['CPM'].isnull().all():
        ads['CPM'] = (ads['cost'] / ads['shows']) * 1000

    df = ads[ads['article'].astype(str) == str(article)][['CPM', 'shows']].dropna()
    df = df[df['shows'] > 200]

    if len(df) < 5:
        return f"<p>❗ Недостаточно данных (показов > 200): найдено {len(df)}.</p>"

    corr = df['CPM'].corr(df['shows'])

    X = df[['CPM']]
    y = df['shows']
    model = LinearRegression()
    model.fit(X, y)
    y_pred = model.predict(X)

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.scatter(df['CPM'], df['shows'], alpha=0.6, label="Фактические данные")
    ax.plot(df['CPM'], y_pred, color='orange', label="Линейная регрессия")
    ax.set_xlabel("CPM (стоимость за 1000 показов)")
    ax.set_ylabel("Показы (shows)")
    ax.set_title(f"CPM → Показы\nКорреляция: {corr:.2f}")
    ax.grid(True)
    ax.legend()
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    img_data = base64.b64encode(buf.getvalue()).decode('utf-8')
    img_tag = f"<img src='data:image/png;base64,{img_data}' style='max-width:100%; height:auto;'>"

    if corr > 0.6:
        comment = "✅ Высокая CPM действительно увеличивает количество показов"
    elif corr > 0.3:
        comment = "➕ Умеренная зависимость — CPM влияет"
    elif corr > 0:
        comment = "⚠️ Слабая положительная связь"
    elif corr < -0.3:
        comment = "❌ Отрицательная связь — выше CPM, меньше показов"
    else:
        comment = "ℹ️ Связь практически отсутствует"

    html = f'''
    <h3>📊 Влияние CPM на количество показов</h3>
    <p>
        📈 Корреляция CPM → shows: <b>{corr:.2f}</b><br>
        <b>{comment}</b>
    </p>
    {img_tag}
    '''
    return html
