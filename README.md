
# 🟣 wb-analytics

AI-консультант и система аналитики для маркетплейса Wildberries.

📊 Поддерживает автоматический парсинг данных:
- `positions` — поисковые позиции по артикулу
- `ads` — рекламные показы, клики, заказы
- `funnel` — воронка продаж

📂 Хранит данные в SQLite и строит визуализации через FastAPI.

---

## ⚙️ Установка и запуск

```bash
git clone https://github.com/chiganos/wb-analytics.git
cd wb-analytics
pip install -r requirements.txt
```

---

## 🚀 Запуск парсинга

Файл `articles.csv` содержит список артикулов и ID кампаний.

**Пример структуры:**
```csv
article,campaign_id
241733698,21138230
221271976,
```

Запуск парсинга позиций:
```bash
python parsers/positions_parser.py
```

---

## 🌐 Веб-интерфейс

```bash
uvicorn app.main_web:app --reload
```

---

## 📁 Структура проекта

- `articles.csv` — список артикулов для парсинга
- `parsers/` — скрипты парсинга
- `app/` — FastAPI-интерфейс с HTML и блоками аналитики
- `data/wb.db` — SQLite-база данных
- `utils/logger.py` — логгер действий
