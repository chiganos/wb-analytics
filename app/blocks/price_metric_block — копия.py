import pandas as pd
import numpy as np
import sqlite3

def render_price_metric_block(article_id: int, db_path: str) -> str:
    """
    Построение HTML-блока с метриками по ценовым диапазонам для заданного артикула.
    :param article_id: ID артикула
    :param db_path: путь к sqlite-базе wb.db
    :return: HTML-блок с таблицей метрик
    """
    # Открываем соединение с переданной базой
    conn = sqlite3.connect(db_path)

    # Читаем данные из таблиц
    funnel_df = pd.read_sql_query(
        "SELECT date, price, orders, stocks_wb FROM funnel WHERE article = ?", conn,
        params=(article_id,)
    )
    calc_df = pd.read_sql_query(
        "SELECT date, profit_per_order, roi AS roi_nominal, margin_percent, CPM FROM calculated WHERE article = ?", conn,
        params=(article_id,)
    )
    ads_df = pd.read_sql_query(
        "SELECT date, cost, shows FROM ads WHERE article = ?", conn,
        params=(article_id,)
    )
    upload_df = pd.read_sql_query(
        "SELECT stock, cost_price, avg_buyout_percent FROM upload WHERE article = ?", conn,
        params=(article_id,)
    )
    conn.close()

    # Объединяем по дате
    df = funnel_df.merge(calc_df, on='date', how='left')
    df = df.merge(
        ads_df.groupby('date', as_index=False)['cost'].mean(),
        on='date', how='left'
    )
    # Добавляем показы рекламы для подсчета рекламных дней
    shows_df = ads_df.groupby('date', as_index=False)['shows'].sum()
    df = df.merge(shows_df, on='date', how='left').fillna({'shows': 0})

    # Параметры склада
    wb_stock = int(funnel_df['stocks_wb'].iloc[-1]) if not funnel_df.empty else 0
    up_stock = int(upload_df.iloc[0]['stock']) if not upload_df.empty else 0
    cost_price = float(upload_df.iloc[0]['cost_price']) if not upload_df.empty else 0.0
    buyout_pct = float(upload_df.iloc[0]['avg_buyout_percent']) if not upload_df.empty else 0.0
    total_stock = wb_stock + up_stock

    # Категоризация по ценовым диапазонам
    bins = list(range(int(df['price'].min()), int(df['price'].max()) + 100, 100))
    df['Диапазон цен'] = (
        pd.cut(df['price'], bins=bins, right=False)
          .astype(str)
          .str.replace(r"\[|\]|\(|\)", "", regex=True)
          .str.replace(", ", "–") + ' ₽'
    )

    # Агрегация метрик по диапазону
    grouped = df.groupby('Диапазон цен', as_index=False).agg(
        Средняя_прибыль_с_заказа=('profit_per_order', 'mean'),
        Средние_заказы_в_день=('orders', 'mean'),
        Средний_расход_рекламы_в_день=('cost', 'mean'),
        CPM=('CPM', 'mean'),
        Маржа=('margin_percent', 'mean'),
        ROI_nominal=('roi_nominal', 'mean'),
        Дней_в_диапазоне=('date', 'count')
    )
    grouped.rename(columns={
        'Средняя_прибыль_с_заказа': 'Средняя прибыль с заказа',
        'Средние_заказы_в_день': 'Средние заказы в день',
        'Средний_расход_рекламы_в_день': 'Средний расход рекламы в день',
        'ROI_nominal': 'ROI номинальный',
        'Дней_в_диапазоне': 'Дней в диапазоне'
    }, inplace=True)

    # Фильтрация и очистка
    grouped = grouped[~grouped['Диапазон цен'].str.contains('nan')].reset_index(drop=True)
    grouped = grouped[grouped['Дней в диапазоне'] >= 3].reset_index(drop=True)
    # Округление ключевых показателей
    fmt1 = lambda x: round(x, 1)
    for col in ['Средняя прибыль с заказа', 'Средние заказы в день', 'Средний расход рекламы в день', 'CPM', 'Маржа', 'ROI номинальный']:
        grouped[col] = grouped[col].apply(fmt1)

    # Средние продажи в день с учетом выкупа
    grouped['Средние продажи в день'] = (grouped['Средние заказы в день'] * buyout_pct).round(1)

    # Количество рекламных дней (shows > 200)
    ad_days = df[df['shows'] > 200].groupby('Диапазон цен')['date'].nunique()
    grouped.insert(
        grouped.columns.get_loc('Дней в диапазоне') + 1,
        'Рекламных дней',
        grouped['Диапазон цен'].map(ad_days).fillna(0).astype(int)
    )

    # Остаток и дни до распродажи
    grouped.insert(
        grouped.columns.get_loc('Дней в диапазоне') + 2,
        'Остаток (вб+склад)',
        total_stock
    )
    ds = (total_stock / grouped['Средние продажи в день'].replace(0, np.nan))
    grouped['Дней до распродажи'] = ds.round().fillna(0).astype(int)

    # Расчет прибылей
    grouped['Номинальная прибыль всего'] = (
        grouped['Средняя прибыль с заказа'] * grouped['Средние продажи в день'] * grouped['Дней до распродажи']
    ).round().astype(int)
    grouped['Реальная прибыль (с учетом инфляции)'] = (
        grouped['Номинальная прибыль всего'] / ((1 + 0.20) ** (grouped['Дней до распродажи'] / 365))
    ).round().astype(int)
    grouped['Стоимость хранения'] = (
        (grouped['Дней до распродажи'] / 2) * total_stock * 0.5
    ).round().astype(int)
    grouped['Чистая прибыль (- инфляция и хранение)'] = (
        grouped['Реальная прибыль (с учетом инфляции)'] - grouped['Стоимость хранения']
    ).round().astype(int)
    grouped['Номинальная прибыль в день'] = (
        grouped['Номинальная прибыль всего'] / grouped['Дней до распродажи'].replace(0, np.nan)
    ).round().fillna(0).astype(int)
    inv = total_stock * cost_price
    grouped['ROI чистая (%)'] = (grouped['Чистая прибыль (- инфляция и хранение)'] / inv * 100).round(1)
    grouped['Чистая годовая рентабельность (%)'] = (
        (1 + grouped['ROI чистая (%)'] / 100) ** (365 / grouped['Дней до распродажи']) - 1
    ) * 100
    grouped['Чистая годовая рентабельность (%)'] = grouped['Чистая годовая рентабельность (%)'].round(1)

    # Подсветка лучшего диапазона
    best_idx = grouped['Чистая годовая рентабельность (%)'].idxmax()
    styler = grouped.style.format({c: '{:.1f}' for c in grouped.select_dtypes(float).columns}) \
        .apply(lambda row: ['background-color:#d9f2d9' if row.name == best_idx else '' for _ in row], axis=1)

    # HTML-оформление
    style = '''<style>
    .price-metric-block{border:1px solid #e0e0e0;border-radius:6px;padding:12px;background:#fff;font-size:12px;max-width:100%;overflow-x:auto;}
    .price-metric-block table.dataframe{width:100%;border-collapse:collapse;font-family:sans-serif;font-size:12px;}
    .price-metric-block th{background:#f0f0f0;font-weight:bold;text-align:center;padding:4px;border:1px solid #ccc;}
    .price-metric-block td{padding:4px;text-align:center;border:1px solid #e0e0e0;}
    .price-metric-block tbody tr:nth-child(even){background:#fafafa;}
    .price-metric-block thead th:first-child,.price-metric-block tbody th{display:none;}
    </style>'''
    header = "<h3 style='font-family:sans-serif;'>📊 Анализ по диапазонам цен</h3>"
    return style + f"<div class='price-metric-block'>{header}" + styler.to_html() + "</div>"
