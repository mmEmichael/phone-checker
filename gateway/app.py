"""
Gateway-сервис для приёма задач проверки телефонов и выдачи результатов.

Принимает списки номеров через POST /process, кладёт задачу в Redis-очередь.
Phone-service забирает задачи, обрабатывает номера и пишет результат в Redis.
Клиент получает результат через GET /result?task_id=...
"""

import os
from pathlib import Path

import redis.asyncio as redis
import uuid
import yaml
from fastapi import FastAPI

# -----------------------------------------------------------------------------
# Загрузка конфигурации
# -----------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"


def load_config() -> dict:
    """
    Читает конфигурацию из config.yaml в папке gateway.
    Если файла нет или он пустой — возвращаются значения по умолчанию.
    """
    defaults = {
        "redis": {"host": "localhost", "port": 6379},
        "queue": {"name": "tasks"},
        "app": {"title": "Phone Checker Gateway"},
    }
    if not CONFIG_PATH.exists():
        return defaults
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if not data:
            return defaults
        # мержим с defaults, чтобы отсутствующие ключи подтянулись
        for key, value in defaults.items():
            if key not in data:
                data[key] = value
        return data
    except Exception:
        return defaults


config = load_config()

# -----------------------------------------------------------------------------
# Подключение к Redis и приложение FastAPI
# -----------------------------------------------------------------------------

# Пытаемся взять адрес из переменной окружения (которую мы прописали в yaml)
# Если её нет (запуск без докера), используем localhost
REDIS_ADDR = os.getenv("REDIS_HOST", "redis")

redis_client = redis.Redis(
    host=REDIS_ADDR,
    port=config["redis"]["port"],
    decode_responses=True,
)

app = FastAPI(title=config["app"].get("title", "Phone Checker Gateway"))

# Имя очереди для task_id (должно совпадать с phone-service)
QUEUE_NAME = config["queue"].get("name", "tasks")


# -----------------------------------------------------------------------------
# API: создание задачи
# -----------------------------------------------------------------------------


@app.post("/process")
async def start_task(phones: list[str]):
    """
    Принимает список телефонов и создаёт задачу на проверку.

    В Redis сохраняются:
    - статус задачи (accepted),
    - хеш с номерами (значения пока 0),
    - task_id добавляется в очередь 'tasks' для воркера.

    Возвращает task_id — по нему потом запрашивают результат через GET /result.
    """
    task_id = str(uuid.uuid4())
    await create_task(task_id, phones)
    return {"message": "Task created", "task_id": task_id}


# -----------------------------------------------------------------------------
# API: получение результата
# -----------------------------------------------------------------------------


@app.get("/result")
async def get_result(task_id: str):
    """
    Возвращает статус или результат задачи по task_id.

    - accepted / processing — задача ещё в работе, возвращается строка со статусом.
    - processed — результат (словарь номер -> "страна: оператор") возвращается клиенту,
      после чего данные задачи удаляются из Redis.
    - иначе — задача не найдена (неверный id или уже получена).
    """
    status = await redis_client.get(f"task:{task_id}:status")
    if status in ["accepted", "processing"]:
        return f"Task ID: {task_id} {status}"
    elif status == "processed":
        result = await redis_client.hgetall(f"task:{task_id}:phones")
        await delete_task(task_id)
        return result
    else:
        return "Task not found"


# -----------------------------------------------------------------------------
# Внутренние функции работы с Redis
# -----------------------------------------------------------------------------


async def create_task(task_id: str, phones: list[str]) -> None:
    """
    Создаёт задачу в Redis: статус, хеш номеров и запись в очередь.

    Используется pipeline — все команды отправляются одним round-trip,
    чтобы минимизировать задержки при большом количестве номеров.
    """
    pipe = redis_client.pipeline()
    pipe.set(f"task:{task_id}:status", "accepted")
    for phone in phones:
        pipe.hset(f"task:{task_id}:phones", phone, 0)
    pipe.lpush(QUEUE_NAME, task_id)
    await pipe.execute()


async def delete_task(task_id: str) -> None:
    """
    Удаляет данные задачи из Redis после выдачи результата клиенту.

    Удаляются ключи task:{task_id}:phones и task:{task_id}:status.
    Выполняется одним round-trip через pipeline.
    """
    pipe = redis_client.pipeline()
    pipe.delete(f"task:{task_id}:phones")
    pipe.delete(f"task:{task_id}:status")
    await pipe.execute()
