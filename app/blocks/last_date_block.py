
import pandas as pd
import sqlite3
from fastapi.responses import HTMLResponse

def render_last_date_block(article_id: int, db_path: str):
    conn = sqlite3.connect(db_path)

    funnel_df = pd.read_sql_query("SELECT * FROM funnel WHERE article = ?", conn, params=(article_id,))
    ads_df = pd.read_sql_query("SELECT * FROM ads WHERE article = ?", conn, params=(article_id,))
    calculated_df = pd.read_sql_query(
        "SELECT date, article, CPM, margin_percent, roi, profit_per_order FROM calculated WHERE article = ?",
        conn,
        params=(article_id,),
    )
    conn.close()

    funnel_df["date"] = pd.to_datetime(funnel_df["date"])
    ads_df["date"] = pd.to_datetime(ads_df["date"])
    calculated_df["date"] = pd.to_datetime(calculated_df["date"])

    df = funnel_df.merge(calculated_df, on="date", how="left")
    df = df.merge(ads_df[["date", "cost"]], on="date", how="left")
    df["cost"] = df["cost"].fillna(0)

    table_html = df[["date", "price", "orders", "cost"]].sort_values("date").to_html(index=False)

    if df.empty or "price" not in df.columns:
        return HTMLResponse("<p>❌ Нет данных для отображения.</p>", status_code=200)

    min_price = int(df["price"].min())
    avg_price = int(df["price"].mean())
    max_price = int(df["price"].max())

    last_date = df["date"].max()
    latest_row = df[df["date"] == last_date].iloc[0]

    latest_block = f"""
    <h4>📊 Метрики за последний день ({last_date.date()}):</h4>
    <ul>
        <li>📦 Заказы: <b>{round(latest_row["orders"], 1)}</b></li>
        <li>💰 Цена: <b>{round(latest_row["price"], 1)}</b></li>
        <li>📦 Остаток на WB: <b>{round(latest_row["stocks_wb"], 1)}</b></li>
        <li>💸 Затраты на рекламу: <b>{round(latest_row["cost"], 1)}</b></li>
        <li>📈 Прибыль с заказа: <b>{round(latest_row["profit_per_order"], 1)}</b></li>
        <li>📉 Маржа %: <b>{round(latest_row["margin_percent"], 1)}</b></li>
        <li>📊 ROI %: <b>{round(latest_row["roi"], 1)}</b></li>
        <li>📢 CPM: <b>{round(latest_row["CPM"], 1)}</b></li>
    </ul>
    """

    return HTMLResponse(content=f"""
    {latest_block}
</ul>
""")