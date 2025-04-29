
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import io
import base64

def analyze_price_impact(db_path: str, article: int) -> str:
    article = int(article)

    conn = sqlite3.connect(db_path)
    funnel = pd.read_sql_query("SELECT * FROM funnel", conn)
    conn.close()

    funnel['date'] = pd.to_datetime(funnel['date'])

    df = funnel[funnel['article'].astype(str) == str(article)].copy()
    df = df[['price', 'orders']].dropna()
    df = df[df['orders'] > 0]

    if len(df) < 3:
        return "<p>❗ Недостаточно данных для анализа (менее 3 строк с заказами).</p>"

    corr = df['price'].corr(df['orders'])

    X = df[['price']].values
    y = df['orders'].values
    model = LinearRegression().fit(X, y)
    predicted = model.predict(X)

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.scatter(df['price'], df['orders'], color='blue', alpha=0.6, label="Фактические данные")
    ax.plot(df['price'], predicted, color='red', label="Линейная регрессия")
    ax.set_xlabel("Цена")
    ax.set_ylabel("Количество заказов")
    ax.set_title(f"Зависимость цены от количества заказов\nАртикул {article}, Корреляция = {corr:.2f}")
    ax.legend()
    ax.grid(True)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    img_base64 = base64.b64encode(buf.getvalue()).decode('utf-8')
    img_tag = f"<img src='data:image/png;base64,{img_base64}' style='max-width:100%; height:auto;'>"

    # Комментарий
    if corr < -0.6:
        comment = "✅ Сильная отрицательная связь — снижение цены увеличивает заказы"
    elif corr < -0.3:
        comment = "➖ Умеренная отрицательная связь — есть влияние, но не абсолютное"
    elif corr < 0:
        comment = "⚠️ Слабая отрицательная связь — возможно влияние, но слабое"
    elif corr > 0.3:
        comment = "❌ Положительная корреляция — заказы растут при росте цены (редкость)"
    else:
        comment = "🟡 Связь почти отсутствует"

    html = f'''
    <h3>📦 Влияние цены на количество заказов</h3>
    <p>
        📈 Корреляция между ценой и заказами: <b>{corr:.2f}</b><br>
        <b>{comment}</b>
    </p>
    {img_tag}
    '''
    return html
