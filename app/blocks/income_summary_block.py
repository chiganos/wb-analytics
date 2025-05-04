import pandas as pd
import sqlite3
from datetime import datetime, timedelta

def render_income_summary_block(db_path: str) -> str:
    conn = sqlite3.connect(db_path)

    # Определим дату 30 дней назад
    start_date = datetime(2025, 4, 1)
    end_date = datetime(2025, 4, 30)
    start_date_str = start_date.strftime("%Y-%m-%d")

    # Загружаем данные
    funnel_df = pd.read_sql_query(f"SELECT article, date, orders, price FROM funnel WHERE date >= '{start_date_str}'", conn)
    upload_df = pd.read_sql_query("SELECT article, avg_buyout_percent FROM upload", conn)
    calculated_df = pd.read_sql_query(f"SELECT article, date, profit_per_order, additional, cost_price, wb_fee, logistics, storage, returns, taxes FROM calculated WHERE date >= '{start_date_str}'", conn)
    ads_df = pd.read_sql_query(f"SELECT article, date, cost FROM ads WHERE date >= '{start_date_str}'", conn)

    # Преобразование типов
    funnel_df["date"] = pd.to_datetime(funnel_df["date"])
    calculated_df["date"] = pd.to_datetime(calculated_df["date"])
    ads_df["date"] = pd.to_datetime(ads_df["date"])

    funnel_df["article"] = funnel_df["article"].astype(str)
    upload_df["article"] = upload_df["article"].astype(str)
    calculated_df["article"] = calculated_df["article"].astype(str)
    ads_df["article"] = ads_df["article"].astype(str)

    # Исправление процентов
    upload_df.loc[upload_df["avg_buyout_percent"] > 1, "avg_buyout_percent"] /= 100

    # Объединение и расчёты по дням
    df = funnel_df.merge(upload_df, on="article", how="left")
    df = df.merge(calculated_df, on=["article", "date"], how="left")
    df = df.merge(ads_df, on=["article", "date"], how="left")
    df.fillna(0, inplace=True)

    # Расчёты
    df["sales"] = df["orders"] * df["avg_buyout_percent"]
    df = df[df["sales"] > 0].copy()
    df["profit"] = df["sales"] * df["profit_per_order"]
    df["additional_total"] = df["sales"] * df["additional"]
    df["total_cost"] = df["sales"] * df["cost_price"]
    df["sales_revenue"] = df["sales"] * df["price"]
    df["wb_fee_total"] = df["sales"] * df["wb_fee"]
    df["logistics_total"] = df["sales"] * df["logistics"]
    df["storage_total"] = df["sales"] * df["storage"]
    df["returns_total"] = df["sales"] * df["returns"]
    df["taxes_total"] = df["sales"] * df["taxes"]
    df["ads_cost"] = df["cost"]
    df["daily_transfer"] = ((df["price"] - df["wb_fee"] - df["logistics"] - df["storage"] - df["returns"]) * df["sales"] - df["cost"])

    # Группировка по артикулу
    final_df = df.groupby("article").agg({
        "price": "mean",
        "orders": "sum",
        "sales": "sum",
        "sales_revenue": "sum",
        "profit": "sum",
        "additional_total": "sum",
        "total_cost": "sum",
        "wb_fee_total": "sum",
        "logistics_total": "sum",
        "storage_total": "sum",
        "returns_total": "sum",
        "taxes_total": "sum",
        "ads_cost": "sum",
        "daily_transfer": "sum"
    }).reset_index()

    conn.close()

    # Форматирование
    for col in final_df.columns:
        if col != "article":
            final_df[col] = final_df[col].astype(float).round(0).astype(int)

    rename_cols = {
        "article": "Артикул",
        "price": "Средняя цена",
        "orders": "Заказы",
        "sales": "Продажи",
        "sales_revenue": "Выручка",
        "profit": "Прибыль",
        "additional_total": "Доп. метрика",
        "total_cost": "Себестоимость",
        "wb_fee_total": "Комиссия WB",
        "logistics_total": "Логистика",
        "storage_total": "Хранение",
        "returns_total": "Возвраты",
        "taxes_total": "Налоги",
        "ads_cost": "Реклама",
        "daily_transfer": "Перевод на р/с"
    }
    final_df.rename(columns=rename_cols, inplace=True)

    # Добавим строку ИТОГО
    total_row = final_df.drop(columns=["Артикул"]).sum(numeric_only=True)
    total_row["Артикул"] = "<b>ИТОГО:</b>"
    total_df = pd.DataFrame([total_row])
    final_df = pd.concat([final_df, total_df], ignore_index=True)

    html_range = f"<p style='font-family: sans-serif; font-size: 14px;'><b>Период:</b> {start_date.strftime('%d.%m.%Y')} – {end_date.strftime('%d.%m.%Y')}</p>"
    return html_range + """
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
table.dataframe tbody tr:last-child {
    font-weight: bold;
    background-color: #e9f5e9;
}
</style>
<h3 style='font-family: sans-serif;'>💰 Финансовая сводка по артикулам</h3>
""" + final_df.to_html(index=False, escape=False, border=1, justify="center", classes="dataframe")
