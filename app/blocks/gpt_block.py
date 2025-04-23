import os
from openai import OpenAI
from fastapi import HTTPException
from app.routes.init_context import context_by_article
from datetime import datetime, timedelta

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DB_PATH = os.path.join(BASE_DIR, "data", "wb.db")

# Используем клиента OpenAI с переменной окружения
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def format_gpt_response(text: str) -> str:
    lines = text.strip().split("\n")
    html = ""
    for line in lines:
        line = line.strip()
        if not line:
            continue
        elif line.startswith(("1.", "2.", "3.", "4.", "5.")):
            html += f"<p style='margin-left:10px;'>{line}</p>"
        elif line.startswith(("-", "•", "*")):
            html += f"<p style='margin-left:20px;'>• {line[1:].strip()}</p>"
        else:
            html += f"<p>{line}</p>"
    return html

def analyze_article_with_gpt(article: int) -> str:
    article_str = str(article).strip()
    context = context_by_article.get(article_str)
    if not context:
        return "⚠️ Нет context_text для данного артикула."

    prompt = f"""
Ты — аналитик Wildberries. Проанализируй артикул: {article}

Данные:
{context}

Формат ответа:
Покажи что происходит с продажами за последние 7 дней в динамике с прошлыми периодами, падают они, растут или плюс минус ровно. Сравни динамику с другими показателями и определи что больше всего влияет на изменение заказов. Используй цифры в качестве доказательства. Отвечай кратко и по делу в стиле сноба.
"""

    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.4,
        )
        raw_text = response.choices[0].message.content
        return format_gpt_response(raw_text)
    except Exception as e:
        return f"❌ Ошибка GPT: {str(e)}"
