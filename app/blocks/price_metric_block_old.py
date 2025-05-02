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
    conn.close()

    funnel_df["date"] = pd.to_datetime(funnel_df["date"])
    ads_df["date"] = pd.to_datetime(ads_df["date"])
    calculated_df["date"] = pd.to_datetime(calculated_df["date"])

    df = funnel_df.merge(calculated_df, on="date", how="left")
    df = df.merge(ads_df[["date", "cost"]], on="date", how="left")

    df["cost"] = df["cost"].fillna(0)

    # Если отсутствует прибыль на заказ, возвращаем пояснение
    if df["profit_per_order"].isna().all():
        return "<div style='font-family: sans-serif; font-size: 15px; color: #555;'>Недостаточно данных: отсутствует информация о прибыли на заказ (profit_per_order).</div>"

    df = df.dropna(subset=["price", "orders", "profit_per_order"])

    min_price = df["price"].min()
    max_price = df["price"].max()
    avg_price = df["price"].mean()

    bins = pd.interval_range(start=int(min_price) - 1, end=int(max_price) + 100, freq=100, closed="right")
    df["price_range"] = pd.cut(df["price"], bins)

    grouped = df.groupby("price_range").agg(
        avg_profit=("profit_per_order", "mean"),
        avg_orders=("orders", "mean"),
        days=("date", "nunique"),
        avg_cost=("cost", "mean"),
        avg_cpm=("CPM", "mean"),
        avg_margin=("margin_percent", "mean"),
        avg_roi=("roi", "mean")
    ).reset_index()

    grouped = grouped[grouped["days"] >= 3]
    grouped["metric"] = grouped["avg_profit"] * grouped["avg_orders"]

    grouped["range_start"] = grouped["price_range"].apply(lambda x: x.left)
    grouped = grouped.sort_values("range_start").drop(columns=["range_start"])

    grouped["price_range"] = grouped["price_range"].apply(lambda x: f"{int(x.left + 1)}–{int(x.right)} ₽")
    grouped["avg_profit"] = grouped["avg_profit"].round(0).astype(int).astype(str) + " ₽"
    grouped["avg_orders"] = grouped["avg_orders"].round(1)
    grouped["avg_cost"] = grouped["avg_cost"].round(1)
    grouped["avg_cpm"] = grouped["avg_cpm"].fillna(0).round(0).astype(int).astype(str)
    grouped["avg_margin"] = grouped["avg_margin"].round(0).astype(int).astype(str) + " %"
    grouped["avg_roi"] = grouped["avg_roi"].round(0).astype(int).astype(str) + " %"
    grouped["metric"] = grouped["metric"].round(0).astype(int).astype(str)

    grouped["metric"] = grouped["metric"].str.replace(" 🔥", "", regex=False)
    if not grouped.empty:
        idx = grouped["metric"].astype(int).idxmax()
        grouped.loc[idx, "metric"] += " 🔥"

    grouped.columns = [
        "Диапазон цен", "Средняя прибыль", "Средние заказы", "Дней в диапазон", "Средний расход в день",
        "CPM", "Маржа", "ROI", "Потенц. прибыль"
    ]

    summary_html = f"""
    <div style='margin-bottom: 10px; font-size: 15px; font-family: sans-serif; color: #333;'>
        <div><b>Минимальная цена:</b> {int(min_price)} ₽</div>
        <div><b>Средняя цена:</b> {int(avg_price)} ₽</div>
        <div><b>Максимальная цена:</b> {int(max_price)} ₽</div>
    </div>
    """

    table_html = grouped.to_html(index=False, classes="table", border=1, escape=False)
    return summary_html + table_html


def analyze_price_metric(db_path: str, article_id: int) -> tuple[str, dict]:
    html = render_price_metric_block(article_id, db_path)
    return html, {}