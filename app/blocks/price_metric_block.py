import pandas as pd
import numpy as np
import sqlite3

def render_price_metric_block(article_id: int, db_path: str) -> str:
    """
    Построение HTML-блока с метриками по ценовым диапазонам для заданного артикула.
    Выводит две таблицы: первая — только рекламные дни (shows > 200), вторая — только дни без рекламы (shows <= 200).
    :param article_id: ID артикула
    :param db_path: путь к sqlite-базе wb.db
    :return: HTML-блок с таблицами
    """
    # Загрузка данных
    conn = sqlite3.connect(db_path)
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

    # Подготовка общего DataFrame
    df = funnel_df.merge(calc_df, on='date', how='left')
    df = df.merge(
        ads_df.groupby('date', as_index=False)['cost'].mean(),
        on='date', how='left'
    )
    shows_df = ads_df.groupby('date', as_index=False)['shows'].sum()
    df = df.merge(shows_df, on='date', how='left').fillna({'shows': 0})

    # Параметры склада и цены
    wb_stock = int(funnel_df['stocks_wb'].iloc[-1]) if not funnel_df.empty else 0
    up_stock = int(upload_df.loc[0, 'stock']) if not upload_df.empty else 0
    cost_price = float(upload_df.loc[0, 'cost_price']) if not upload_df.empty else 0.0
    buyout_pct = float(upload_df.loc[0, 'avg_buyout_percent']) if not upload_df.empty else 0.0
    total_stock = wb_stock + up_stock

    def build_group(df_subset):
        # Пустая таблица
        if df_subset.empty:
            cols = [
                'Диапазон цен','Средняя прибыль с заказа','Средние заказы в день',
                'Средний расход рекламы в день','CPM','Маржа','ROI номинальный',
                'Дней в диапазоне','Остаток (вб+склад)','Средние продажи в день',
                'Дней до распродажи','Номинальная прибыль всего',
                'Реальная прибыль (с учетом инфляции)','ROI чистая (%)',
                'Чистая годовая рентабельность (%)'
            ]
            return pd.DataFrame(columns=cols)

        # Категоризация
        bins = list(range(int(df_subset['price'].min()), int(df_subset['price'].max()) + 100, 100))
        df_subset = df_subset.copy()
        df_subset['Диапазон цен'] = (
            pd.cut(df_subset['price'], bins=bins, right=False)
              .astype(str)
              .str.replace(r"\[|\]|\(|\)", "", regex=True)
              .str.replace(", ", "–") + ' ₽'
        )
        grouped = df_subset.groupby('Диапазон цен', as_index=False).agg(
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
        grouped = grouped[~grouped['Диапазон цен'].str.contains('nan')]
        grouped = grouped[grouped['Дней в диапазоне'] >= 3].reset_index(drop=True)

        # Общие расчеты
        grouped['Средние продажи в день'] = grouped['Средние заказы в день'] * buyout_pct
        grouped.insert(grouped.columns.get_loc('Дней в диапазоне') + 1,
                       'Остаток (вб+склад)', total_stock)
        grouped['Дней до распродажи'] = (
            total_stock / grouped['Средние продажи в день'].replace(0, np.nan)
        ).fillna(0)
        grouped['Номинальная прибыль всего'] = (
            grouped['Средняя прибыль с заказа'] * grouped['Средние продажи в день'] * grouped['Дней до распродажи']
        )
        grouped['Реальная прибыль (с учетом инфляции)'] = (
            grouped['Номинальная прибыль всего'] / ((1 + 0.20) ** (grouped['Дней до распродажи'] / 365))
        )
        inv = total_stock * cost_price
        grouped['ROI чистая (%)'] = grouped['Реальная прибыль (с учетом инфляции)'] / inv * 100
        grouped['Чистая годовая рентабельность (%)'] = (
            (1 + grouped['ROI чистая (%)'] / 100) ** (365 / grouped['Дней до распродажи']) - 1
        ) * 100

                # Округление
        one_decimal = [
            'Средние заказы в день', 'Средний расход рекламы в день', 'Маржа', 'Средние продажи в день'
        ]
        # Округляем с одним десятичным
        for col in one_decimal:
            grouped[col] = grouped[col].round(1)
        # Округляем остальные числовые и приводим к int, заполняя NaN нулями
        numeric_cols = grouped.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if col not in one_decimal and col != 'Дней в диапазоне':
                grouped[col] = grouped[col].round(0).fillna(0).astype(int)
        # Приводим 'Дней в диапазоне' к int
        grouped['Дней в диапазоне'] = grouped['Дней в диапазоне'].round(0).fillna(0).astype(int)

        return grouped

    # Таблицы для рекламных и без рекламных дней
    grouped_ads = build_group(df[df['shows'] > 200])
    grouped_no_ads = build_group(df[df['shows'] <= 200])

    # Форматирование
    style_fmt = {col: '{:.1f}' for col in ['Средние заказы в день', 'Средний расход рекламы в день', 'Маржа', 'Средние продажи в день']}
    html_ads = (
        grouped_ads.style.format(style_fmt).to_html() if not grouped_ads.empty
        else "<p style='font-family:sans-serif;'>Нет данных за рекламные дни</p>"
    )
    html_no_ads = (
        grouped_no_ads.style.format(style_fmt).to_html() if not grouped_no_ads.empty
        else "<p style='font-family:sans-serif;'>Нет данных за дни без рекламы</p>"
    )

    # Итоговый HTML
    style = '''<style>
    .price-metric-block{border:1px solid #e0e0e0;border-radius:6px;padding:12px;background:#fff;font-size:12px;max-width:100%;overflow-x:auto;}
    .price-metric-block table{width:100%;border-collapse:collapse;font-family:sans-serif;font-size:12px;}
    .price-metric-block th{background:#f0f0f0;font-weight:bold;text-align:center;padding:4px;border:1px solid #ccc;}
    .price-metric-block td{padding:4px;text-align:center;border:1px solid #e0e0e0;}
    .price-metric-block tbody tr:nth-child(even){background:#fafafa;}
    .price-metric-block thead th:first-child,.price-metric-block tbody th{display:none;}
    </style>'''
    header1 = "<h3 style='font-family:sans-serif;'>📊 Анализ по диапазонам цен — рекламные дни</h3>"
    header2 = "<h3 style='font-family:sans-serif;'>📊 Анализ по диапазонам цен — дни без рекламы</h3>"

    return (
        style +
        f"<div class='price-metric-block'>{header1}" + html_ads +
        header2 + html_no_ads +
        "</div>"
    )
