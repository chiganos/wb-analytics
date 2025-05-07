import os
from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
import sqlite3
import pandas as pd
import numpy as np

router = APIRouter()
BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.normpath(os.path.join(BASE_DIR, "../../data/wb.db"))

# Получение метрик по ценовым диапазонам
def get_price_metric_table(article_id: int) -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    funnel_df = pd.read_sql_query(
        "SELECT date, price, orders, stocks_wb FROM funnel WHERE article = ?", conn,
        params=(article_id,)
    )
    calc_df = pd.read_sql_query(
        "SELECT date, profit_per_order, roi AS roi_nominal, margin_percent, CPM FROM calculated WHERE article = ?", conn,
        params=(article_id,)
    )
    ads_df = pd.read_sql_query(
        "SELECT date, cost FROM ads WHERE article = ?", conn,
        params=(article_id,)
    )
    upload_df = pd.read_sql_query(
        "SELECT stock, cost_price, avg_buyout_percent FROM upload WHERE article = ?", conn,
        params=(article_id,)
    )
    conn.close()
    if funnel_df.empty:
        return pd.DataFrame()

    df = funnel_df[['date', 'price', 'orders']].merge(calc_df, on='date', how='left')
    df = df.merge(ads_df.groupby('date', as_index=False)['cost'].mean(), on='date', how='left')

    wb_stock = int(funnel_df['stocks_wb'].iat[-1])
    up_stock = int(upload_df['stock'].iat[0]) if not upload_df.empty else 0
    total_stock = wb_stock + up_stock
    cost_price = float(upload_df['cost_price'].iat[0]) if not upload_df.empty else 0.0
    buyout_pct = float(upload_df['avg_buyout_percent'].iat[0]) if not upload_df.empty else 0.0

    price_min = int(df['price'].min())
    price_max = int(df['price'].max())
    bins = list(range(price_min, price_max + 100, 100))
    df['price_range'] = pd.cut(df['price'], bins=bins, right=False).astype(str)
    df['price_range'] = (
        df['price_range']
            .str.replace(r"[\[\]\(\)]", "", regex=True)
            .str.replace(", ", "–")
        + ' ₽'
    )

    metrics = df.groupby('price_range', as_index=False).agg(
        avg_profit=('profit_per_order', 'mean'),
        avg_orders=('orders', 'mean'),
        avg_ad_cost=('cost', 'mean'),
        avg_CPM=('CPM', 'mean'),
        avg_margin=('margin_percent', 'mean'),
        roi_nominal=('roi_nominal', 'mean'),
        days_count=('date', 'count')
    )
    metrics = metrics[metrics['days_count'] >= 3]
    for col in ['avg_profit', 'avg_orders', 'avg_ad_cost', 'avg_CPM', 'avg_margin', 'roi_nominal']:
        metrics[col] = metrics[col].round(1)

    metrics['avg_sales'] = (metrics['avg_orders'] * buyout_pct).round(1).fillna(0)
    metrics['stock_total'] = total_stock
    metrics['days_to_clear'] = (
        metrics['stock_total'] / metrics['avg_sales']
    ).replace([np.inf, -np.inf], np.nan).round().fillna(0).astype(int)
    metrics['nominal_total_profit'] = (
        metrics['avg_profit'] * metrics['avg_sales'] * metrics['days_to_clear']
    ).round().fillna(0).astype(int)
    metrics['real_profit'] = (
        metrics['nominal_total_profit'] / ((1 + 0.20) ** (metrics['days_to_clear'] / 365))
    ).round().fillna(0).astype(int)
    metrics['storage_cost'] = (
        (metrics['days_to_clear'] / 2) * total_stock * 0.5
    ).round().fillna(0).astype(int)
    metrics['net_profit'] = (metrics['real_profit'] - metrics['storage_cost']).round().fillna(0).astype(int)
    inv = total_stock * cost_price if cost_price > 0 else 1
    metrics['roi_net'] = ((metrics['net_profit'] / inv) * 100).round(1).fillna(0)
    # Линейная годовая рентабельность вместо экспоненциальной
    metrics['annual_return'] = (metrics['roi_net'] * (365 / metrics['days_to_clear'])).round(1).fillna(0)
    return metrics

# Метрики за последние 7 дней
def compute_current_metrics(article_id: int, conn: sqlite3.Connection) -> dict | None:
    funnel = pd.read_sql_query(
        "SELECT date, orders, stocks_wb FROM funnel WHERE article = ? AND date >= date('now','-7 day')", conn,
        params=(article_id,)
    )
    if funnel.empty:
        return None
    upload = pd.read_sql_query(
        "SELECT stock, cost_price, avg_buyout_percent FROM upload WHERE article = ?", conn,
        params=(article_id,)
    )
    wb_stock = int(funnel['stocks_wb'].iat[-1])
    up_stock = int(upload['stock'].iat[0]) if not upload.empty else 0
    total_stock = wb_stock + up_stock

    calc = pd.read_sql_query(
        "SELECT date, profit_per_order, roi AS roi_nominal, margin_percent, CPM FROM calculated WHERE article = ? AND date >= date('now','-7 day')", conn,
        params=(article_id,)
    )
    ads = pd.read_sql_query(
        "SELECT date, cost FROM ads WHERE article = ? AND date >= date('now','-7 day')", conn,
        params=(article_id,)
    )
    df = funnel[['date', 'orders']].merge(calc, on='date', how='left')
    df = df.merge(ads.groupby('date', as_index=False)['cost'].mean(), on='date', how='left')
    avg_profit = df['profit_per_order'].mean()
    avg_orders = df['orders'].mean()
    buyout_pct = float(upload['avg_buyout_percent'].iat[0]) if not upload.empty else 0
    avg_sales = avg_orders * buyout_pct
    days_to_clear = int(total_stock / avg_sales) if avg_sales > 0 else 0
    nominal_total = avg_profit * avg_sales * days_to_clear
    real_profit = nominal_total / ((1 + 0.20) ** (days_to_clear / 365))
    storage_cost = (days_to_clear / 2) * total_stock * 0.5
    net_profit = real_profit - storage_cost
    cost_price = float(upload['cost_price'].iat[0]) if not upload.empty else 1
    roi_net = (net_profit / (total_stock * cost_price)) * 100 if total_stock * cost_price > 0 else 0
    annual_return = (roi_net * (365 / days_to_clear)) if days_to_clear > 0 else 0
    return {
        'article': article_id,
        'Дней до распродажи': days_to_clear,
        'ROI чистая (%)': round(roi_net, 1),
        'Чистая годовая рентабельность (%)': round(annual_return, 1)
    }

# Формирование HTML
def render_html_table(df: pd.DataFrame) -> str:
    styles = """
    <style>
      table {border-collapse: collapse; width: 100%;}
      th, td {border: 1px solid #ddd; padding: 8px; text-align: center;}
      th {background: #f4f4f4;}
    </style>
    """
    return styles + df.to_html(index=False, float_format='{:,.1f}'.format)

# Эндпоинт для одного артикула
@router.get("/api/analytics/price-metric-summary/{article}", response_class=HTMLResponse)
def summary_single(article: int):
    conn = sqlite3.connect(DB_PATH)
    curr = compute_current_metrics(article, conn)
    best_df = get_price_metric_table(article)
    conn.close()
    if curr is None or best_df.empty:
        raise HTTPException(status_code=404, detail="Недостаточно данных для артикула")
    best = best_df.loc[best_df['roi_net'].idxmax()]
    data = {**curr, 'Лучший ROI чистая (%)': best['roi_net'], 'Лучшая годовая рентабельность (%)': best['annual_return']}
    return HTMLResponse(render_html_table(pd.DataFrame([data])))

# Эндпоинт для всех артикулов
@router.get("/api/analytics/price-metric-summary", response_class=HTMLResponse)
@router.get("/api/analytics/price-metric-summary/", response_class=HTMLResponse)
def summary_all():
    conn = sqlite3.connect(DB_PATH)
    articles = pd.read_sql_query("SELECT DISTINCT article FROM funnel", conn)["article"].tolist()
    rows = []
    for aid in articles:
        curr = compute_current_metrics(aid, conn)
        if curr is None:
            continue
        best_df = get_price_metric_table(aid)
        if best_df.empty:
            continue
        best = best_df.loc[best_df['roi_net'].idxmax()]
        rows.append({**curr, 'Лучший ROI чистая (%)': best['roi_net'], 'Лучшая годовая рентабельность (%)': best['annual_return']})
    conn.close()
    if not rows:
        raise HTTPException(status_code=404, detail="Нет данных для отображения")
    return HTMLResponse(render_html_table(pd.DataFrame(rows)))
