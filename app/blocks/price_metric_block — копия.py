
import pandas as pd
import sqlite3

def render_price_metric_block(article_id: int, db_path: str) -> str:
    conn = sqlite3.connect(db_path)

    funnel_df = pd.read_sql_query("SELECT * FROM funnel WHERE article = ?", conn, params=(article_id,))
    ads_df = pd.read_sql_query("SELECT * FROM ads WHERE article = ?", conn, params=(article_id,))
    calculated_df = pd.read_sql_query(
        "SELECT date, article, CPM, margin_percent, roi, profit_per_order FROM calculated WHERE article = ?",
        conn,
        params=(article_id,),
    )
    cost_df = pd.read_sql_query("SELECT cost_price FROM upload WHERE article = ?", conn, params=(article_id,))
    stock_df = pd.read_sql_query("SELECT stocks_wb FROM funnel WHERE article = ? ORDER BY date DESC LIMIT 1", conn, params=(article_id,))
    buyout_df = pd.read_sql_query("SELECT avg_buyout_percent FROM upload WHERE article = ?", conn, params=(article_id,))
    conn.close()

    funnel_df["date"] = pd.to_datetime(funnel_df["date"])
    ads_df["date"] = pd.to_datetime(ads_df["date"])
    calculated_df["date"] = pd.to_datetime(calculated_df["date"])

    df = funnel_df.merge(calculated_df, on="date", how="left")
    df = df.merge(ads_df[["date", "cost"]], on="date", how="left")
    df["cost"] = df["cost"].fillna(0)

    if df["profit_per_order"].isna().all():
        return "<div style='font-family: sans-serif; font-size: 15px; color: #555;'>Недостаточно данных: отсутствует информация о прибыли на заказ (profit_per_order).</div>"

    df = df.dropna(subset=["price", "orders", "profit_per_order"])

    min_price = df["price"].min()
    max_price = df["price"].max()
    bins = pd.interval_range(start=int(min_price) - 1, end=int(max_price) + 100, freq=100, closed="right")
    df["price_range"] = pd.cut(df["price"], bins)

    grouped = df.groupby("price_range", observed=True).agg(
        avg_profit=("profit_per_order", "mean"),
        avg_orders=("orders", "mean"),
        days=("date", "nunique"),
        avg_cost=("cost", "mean"),
        avg_cpm=("CPM", "mean"),
        avg_margin=("margin_percent", "mean"),
        avg_roi=("roi", "mean")
    ).reset_index()

    grouped = grouped[grouped["days"] >= 3]
    grouped["price_range"] = grouped["price_range"].apply(lambda x: f"{int(x.left+1)}–{int(x.right)} ₽")

    cost_price = float(cost_df.iloc[0]["cost_price"]) if not cost_df.empty else 90
    stock = int(stock_df.iloc[0]["stocks_wb"]) if not stock_df.empty else 500
    avg_buyout = float(buyout_df.iloc[0]["avg_buyout_percent"]) if not buyout_df.empty else 1.0

    grouped["avg_sales"] = grouped["avg_orders"] * avg_buyout
    grouped["days_to_sell"] = stock / grouped["avg_sales"]
    grouped["nominal_profit"] = grouped["avg_profit"] * grouped["avg_sales"] * grouped["days_to_sell"]

    inflation_rate = 0.20
    grouped["npv_profit"] = grouped["nominal_profit"] / ((1 + inflation_rate) ** (grouped["days_to_sell"] / 365))

    storage_cost_per_day = 0.5
    grouped["storage_cost"] = (grouped["days_to_sell"] / 2) * stock * storage_cost_per_day
    grouped["net_profit"] = grouped["npv_profit"] - grouped["storage_cost"]
    grouped["profit_per_day"] = grouped["nominal_profit"] / grouped["days_to_sell"]
    grouped["avg_spend_per_day"] = grouped["avg_cost"]

    grouped = grouped[[
        "price_range", "avg_profit", "avg_orders", "avg_sales", "days", "days_to_sell", "avg_spend_per_day",
        "avg_cpm", "avg_margin", "avg_roi", "profit_per_day", "nominal_profit",
        "npv_profit", "storage_cost", "net_profit"
    ]]

    grouped.columns = [
        "Диапазон цен", "Средняя прибыль с заказа", "Средние заказы в день", "Средние продажи в день",
        "Дней в диапазоне", "Дней до распродажи", "Средний расход рекламы в день",
        "CPM", "Маржа", "ROI", "Номинальная прибыль в день", "Номинальная прибыль всего",
        "Реальная прибыль (с учетом инфляции)", "Стоимость хранения", "Чистая прибыль (- инфляция и хранение)"
    ]

    for col in grouped.columns:
        if col in ["Средняя прибыль с заказа", "Средние заказы в день", "Средние продажи в день", "Маржа"]:
            grouped[col] = grouped[col].map(lambda x: f"{x:.1f}".rstrip("0").rstrip(".") if isinstance(x, float) else x)
        elif grouped[col].dtype.kind in "f":
            grouped[col] = grouped[col].round(0).astype(int)

    return """
<style>
table.dataframe {
    border-collapse: collapse;
    width: 100%;
    font-family: sans-serif;
    font-size: 13px;
}
table.dataframe th {
    background-color: #f4f4f4;
    font-weight: bold;
    text-align: center;
    padding: 8px;
    border: 1px solid #ccc;
}
table.dataframe td {
    padding: 6px;
    text-align: center;
    border: 1px solid #e0e0e0;
}
table.dataframe tbody tr:nth-child(even) {
    background-color: #fafafa;
}
</style>
""" + "<h3 style='font-family: sans-serif;'>📊 Анализ по диапазонам цен</h3>" + grouped.to_html(index=False, border=1, justify="center", classes="dataframe")
