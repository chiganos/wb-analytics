# utils/logger.py
from datetime import datetime

_log_buffer = []

def log(msg: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    _log_buffer.append(f"[{timestamp}] {msg}")
    if len(_log_buffer) > 500:
        _log_buffer.pop(0)

def get_logs():
    return "\n".join(_log_buffer)

def clear_logs():
    _log_buffer.clear()
