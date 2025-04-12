import sqlite3

db_path = "data/wb.db"
table_name = "funnel"

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(f"PRAGMA table_info({table_name});")
    columns = cursor.fetchall()

    if columns:
        print(f"📋 Колонки таблицы '{table_name}':")
        for col in columns:
            print(f" - {col[1]} ({col[2]})")  # col[1] — имя колонки, col[2] — тип
    else:
        print(f"⚠️ Таблица '{table_name}' не найдена в базе.")
except Exception as e:
    print(f"❌ Ошибка при подключении к БД: {e}")
finally:
    conn.close()
