import sqlite3
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline
from fastapi.responses import HTMLResponse
import plotly.graph_objects as go


def get_optimal_price(df, y_col):
    if df["price"].nunique() <= 1:
        return f"<p>Недостаточно уникальных цен для анализа</p>"
    df = df[["price", y_col]].dropna()
    df = df.groupby("price")[y_col].agg(["mean", "count"]).reset_index()
    df.columns = ["price", y_col, "count"]
    if len(df) < 3:
        return None

    X = df[["price"]]
    y = df[y_col]

    poly_model = make_pipeline(PolynomialFeatures(2), LinearRegression())
    poly_model.fit(X, y)

    price_range = pd.DataFrame(np.linspace(df["price"].min(), df["price"].max(), 200), columns=["price"])
    y_pred = poly_model.predict(price_range)

    best_price = price_range.iloc[np.argmax(y_pred)][0]

    return df, y_pred, best_price


def analyze_price_optimization(db_path: str, article: int) -> HTMLResponse:
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
        return HTMLResponse("<p>❗ Недостаточно данных по funnel для анализа.</p>")

    result = get_optimal_price(df_f, "orders")
    if isinstance(result, str):
        return HTMLResponse(result)
    if result is not None:
        df_orders, y_orders, price_orders = result
    else:
        df_orders = y_orders = price_orders = None

    result_profit = get_optimal_price(df_c, "profit") if df_c is not None and "profit" in df_c else None
    if isinstance(result_profit, str):
        return HTMLResponse(result_profit)
    if result_profit is not None:
        df_profit, y_profit, price_profit = result_profit
    else:
        df_profit = y_profit = price_profit = None

    fig = go.Figure()

    if df_orders is not None:
        fig.add_trace(go.Scatter(
            x=df_orders["price"],
            y=df_orders["orders"],
            mode="markers+lines",
            name="Заказы",
            text=[f"Дней: {c}" for c in df_orders["count"]],
            hoverinfo="text+x+y",
            line=dict(color="blue")
        ))
        fig.add_trace(go.Scatter(
            x=pd.DataFrame(np.linspace(df_orders["price"].min(), df_orders["price"].max(), 200), columns=["price"])["price"],
            y=y_orders,
            mode="lines",
            name=f"Оптимальная цена по заказам (модель): {int(price_orders)}",
            line=dict(color="royalblue", dash="dash")
        ))

    if df_profit is not None:
        fig.add_trace(go.Scatter(
            x=df_profit["price"],
            y=df_profit["profit"],
            mode="markers+lines",
            name="Прибыль",
            text=[f"Дней: {c}" for c in df_profit["count"]],
            hoverinfo="text+x+y",
            line=dict(color="green")
        ))
        fig.add_trace(go.Scatter(
            x=pd.DataFrame(np.linspace(df_profit["price"].min(), df_profit["price"].max(), 200), columns=["price"])["price"],
            y=y_profit,
            mode="lines",
            name=f"Оптимальная цена по прибыли (модель): {int(price_profit)}",
            line=dict(color="darkgreen", dash="dash")
        ))

    fig.update_layout(
        title=f"Оптимизация цены / артикул {article}",
        xaxis_title="Цена",
        yaxis_title="Метрики",
        hovermode="x unified",
        legend=dict(x=0.01, y=0.99)
    )

    return HTMLResponse(content=fig.to_html(full_html=False, include_plotlyjs='cdn'))
