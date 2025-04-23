
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
from sklearn.linear_model import LinearRegression

def analyze_price_conversion(db_path: str, article: int) -> str:
    article = int(article)

    conn = sqlite3.connect(db_path)
    funnel = pd.read_sql_query("SELECT * FROM funnel", conn)
    conn.close()

    funnel['date'] = pd.to_datetime(funnel['date'])
    df = funnel[funnel['article'].astype(str) == str(article)][['price', 'cart_to_order_conv']].dropna()

    if len(df) < 5:
        return "<p>❗ Недостаточно данных для анализа (нужно хотя бы 5 точек).</p>"

    corr = df['price'].corr(df['cart_to_order_conv'])

    X = df[['price']]
    y = df['cart_to_order_conv']
    model = LinearRegression()
    model.fit(X, y)
    y_pred = model.predict(X)

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.scatter(df['price'], df['cart_to_order_conv'], alpha=0.7, label='Фактические данные')
    ax.plot(df['price'], y_pred, color='red', label='Линейная регрессия')
    ax.set_xlabel("Цена")
    ax.set_ylabel("Конверсия из корзины в заказ")
    ax.set_title(f"Цена → cart_to_order_conv\nКорреляция: {corr:.2f}")
    ax.grid(True)
    ax.legend()
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    img_data = base64.b64encode(buf.getvalue()).decode('utf-8')
    img_tag = f"<img src='data:image/png;base64,{img_data}' style='max-width:100%; height:auto;'>"

    # Комментарий
    if corr > 0.6:
        comment = "✅ Сильная положительная связь — высокая цена увеличивает конверсию"
    elif corr < -0.6:
        comment = "❌ Сильная отрицательная связь — высокая цена снижает конверсию"
    elif corr > 0.3:
        comment = "➕ Умеренная положительная связь"
    elif corr < -0.3:
        comment = "➖ Умеренная отрицательная связь"
    elif abs(corr) < 0.1:
        comment = "⚠️ Связь почти отсутствует"
    else:
        comment = "ℹ️ Слабая связь"

    html = f'''
    <h3>🧩 Влияние цены на конверсию из корзины в заказ</h3>
    <p>
        📊 Корреляция между ценой и cart_to_order_conv: <b>{corr:.2f}</b><br>
        <b>{comment}</b>
    </p>
    {img_tag}
    '''
    return html
