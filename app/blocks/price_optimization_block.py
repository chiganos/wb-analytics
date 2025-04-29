import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline
import numpy as np
import io
import base64

def get_optimal_price(df, y_col):
    if df["price"].nunique() <= 1:
        return f"<div style='font-family: sans-serif; font-size: 15px; color: #555;'>Недостаточно данных: только {df['price'].nunique()} уникальная цена для построения графика.</div>"
    df = df[["price", y_col]].dropna()
    df = df.groupby("price")[y_col].mean().reset_index()
    if len(df) < 3:
        return None, None, None

    X = df[["price"]]
    y = df[y_col]

    poly_model = make_pipeline(PolynomialFeatures(2), LinearRegression())
    poly_model.fit(X, y)

    price_range = np.linspace(df["price"].min(), df["price"].max(), 200).reshape(-1, 1)
    y_pred = poly_model.predict(price_range)

    best_price = price_range[np.argmax(y_pred)][0]

    return df, y_pred, best_price

def analyze_price_optimization(db_path: str, article: int) -> str:
    article = int(article)
    conn = sqlite3.connect(db_path)
    funnel = pd.read_sql_query("SELECT * FROM funnel", conn)

    try:
        calculated = pd.read_sql_query("SELECT * FROM calculated", conn)
    except:
        calculated = None

    conn.close()

    funnel["date"] = pd.to_datetime(funnel["date"])
    df_f = funnel[funnel["article"].astype(str) == str(article)].copy()
    df_c = calculated[calculated["article"].astype(str) == str(article)].copy() if calculated is not None else None

    if len(df_f) < 3:
        return "<p>❗ Недостаточно данных по funnel для анализа.</p>"

    result = get_optimal_price(df_f, "orders")
    if isinstance(result, str):
        return result
    if result is not None:
        if len(result) == 4:
            df_orders, y_orders, price_orders, _ = result
        elif len(result) == 3:
            df_orders, y_orders, price_orders = result
        else:
            return "<div style='font-family:sans-serif; font-size:15px; color:#b00;'>Ошибка: неожиданный формат данных при оптимизации цены (orders).</div>"
    else:
        df_orders = y_orders = price_orders = None

    result_profit = get_optimal_price(df_c, "profit") if df_c is not None and "profit" in df_c else None
    if isinstance(result_profit, str):
        return result_profit
    if result_profit is not None:
        if len(result_profit) == 4:
            df_profit, y_profit, price_profit, _ = result_profit
        elif len(result_profit) == 3:
            df_profit, y_profit, price_profit = result_profit
        else:
            return "<div style='font-family:sans-serif; font-size:15px; color:#b00;'>Ошибка: неожиданный формат данных при оптимизации цены (profit).</div>"
    else:
        df_profit = y_profit = price_profit = None

    fig, axs = plt.subplots(1, 2, figsize=(14, 5))

    if df_orders is not None:
        axs[0].scatter(df_orders["price"], df_orders["orders"], color="blue", label="Фактические заказы")
        axs[0].plot(np.linspace(df_orders["price"].min(), df_orders["price"].max(), 200), y_orders, color="green", label="Модель")
        axs[0].axvline(price_orders, color="red", linestyle="--", label=f"Оптимальная цена: {price_orders:.0f}")
        axs[0].set_title("Оптимизация по заказам")
        axs[0].set_xlabel("Цена")
        axs[0].set_ylabel("Среднее количество заказов")
        axs[0].legend()
        axs[0].grid(True)

    if df_profit is not None:
        axs[1].scatter(df_profit["price"], df_profit["profit"], color="purple", label="Фактическая прибыль")
        axs[1].plot(np.linspace(df_profit["price"].min(), df_profit["price"].max(), 200), y_profit, color="orange", label="Модель")
        axs[1].axvline(price_profit, color="red", linestyle="--", label=f"Оптимальная цена: {price_profit:.0f}")
        axs[1].set_title("Оптимизация по прибыли")
        axs[1].set_xlabel("Цена")
        axs[1].set_ylabel("Средняя прибыль")
        axs[1].legend()
        axs[1].grid(True)

    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    img_base64 = base64.b64encode(buf.read()).decode("utf-8")

    html = f"""
    <div style='text-align:center;'>
        <h3 style='font-family:sans-serif;'>Оптимизация цены</h3>
        <img src="data:image/png;base64,{img_base64}" style='max-width:100%; height:auto;'>
    </div>
    """
    return html
