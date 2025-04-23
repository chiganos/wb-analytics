import sqlite3
import os
import numpy as np
from datetime import datetime, timedelta

context_by_article = {}

def average(values):
    cleaned = [v for v in values if v is not None]
    return round(sum(cleaned) / len(cleaned), 2) if cleaned else 0

def calculate_correlation(x, y):
    x, y = list(x), list(y)
    min_len = min(len(x), len(y))
    x, y = x[:min_len], y[:min_len]
    if len(x) >= 2 and len(y) >= 2:
        return round(np.corrcoef(x, y)[0, 1], 2)
    return None

def init_context_data(db_path, only_article: str = None):
    if not os.path.exists(db_path):
        print("❌ База данных не найдена.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    if only_article:
        articles = [only_article]
    else:
        cursor.execute("SELECT DISTINCT article FROM funnel")
        articles = [row[0] for row in cursor.fetchall()]

    for article in articles:
        context_text = f"📦 Артикул: {article}\n"

        cursor.execute("""
            SELECT date, orders, price, cart_conversion, cart_to_order_conv FROM funnel
            WHERE article = ? ORDER BY date DESC LIMIT 30
        """, (article,))
        funnel_data = cursor.fetchall()

        cursor.execute("""
            SELECT date, shows, baskets, ctr FROM ads
            WHERE article = ? ORDER BY date DESC LIMIT 30
        """, (article,))
        ads_data = cursor.fetchall()

        cursor.execute("""
            SELECT date, search_query, open_card FROM positions
            WHERE article = ? ORDER BY date DESC LIMIT 30 * 10
        """, (article,))
        positions_data = cursor.fetchall()

        if len(funnel_data) < 3:
            continue

        def extract_column(data, index):
            return [row[index] for row in data if row[index] is not None]

        orders = extract_column(funnel_data, 1)
        prices = extract_column(funnel_data, 2)
        cart_conv = extract_column(funnel_data, 3)
        cart_to_order_conv = extract_column(funnel_data, 4)

        shows = extract_column(ads_data, 1)
        baskets = extract_column(ads_data, 2)
        ctr = extract_column(ads_data, 3)

        for period in [3, 7, 14, 30]:
            context_text += f"\n📊 Средние значения за {period} дней:\n"
            context_text += f"- Заказы: {average(orders[:period])}\n"
            context_text += f"- Цена: {average(prices[:period])}\n"
            context_text += f"- Конверсия в корзину: {average(cart_conv[:period])}\n"
            context_text += f"- Конверсия из корзины в заказ: {average(cart_to_order_conv[:period])}\n"
            context_text += f"- Показы в рекламе: {average(shows[:period])}\n"
            context_text += f"- Корзины из рекламы: {average(baskets[:period])}\n"
            context_text += f"- CTR: {average(ctr[:period])}\n"

        context_text += "\n📈 Корреляции (за 30 дней):\n"
        context_text += f"- Цена → Заказы: {calculate_correlation(prices, orders)}\n"
        context_text += f"- Показы → Заказы: {calculate_correlation(shows, orders)}\n"
        context_text += f"- CTR → Заказы: {calculate_correlation(ctr, orders)}\n"
        context_text += f"- Корзины из рекламы → Заказы: {calculate_correlation(baskets, orders)}\n"
        context_text += f"- Конверсия в корзину → Заказы: {calculate_correlation(cart_conv, orders)}\n"
        context_text += f"- Конверсия из корзины в заказ → Заказы: {calculate_correlation(cart_to_order_conv, orders)}\n"

        # Топ ключи по open_card (30 дней)
        key_stats = {}
        for date, query, open_card in positions_data:
            if open_card is not None:
                key_stats[query] = key_stats.get(query, 0) + open_card

        top_queries = [q for q, oc_sum in key_stats.items() if oc_sum > 100]

        if top_queries:
            context_text += "\n🔍 Ключевые запросы с >100 переходами:\n"
            for q in top_queries:
                daily_opens = [row[2] for row in positions_data if row[1] == q and row[2] is not None]
                context_text += f"- {q}: средн. переходов в день = {average(daily_opens)}\n"

        os.makedirs("context_texts", exist_ok=True)
        path = f"context_texts/{article}.txt"
        with open(path, "w", encoding="utf-8") as f:
            f.write(context_text)
        print(f"💾 Сохранено: {path}")

        context_by_article[str(article)] = context_text

    conn.close()
    print("✅ Контекст для GPT инициализирован")
    print(f"🔢 Всего артикулов в context_by_article: {len(context_by_article)}")
    print(f"📦 Примеры артикулов: {list(context_by_article.keys())[:2]}")