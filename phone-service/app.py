"""
Воркер проверки телефонов: забирает задачи из Redis, определяет страну и оператора
по каждому номеру (phonenumbers), пишет результат обратно в Redis.

Работает в бесконечном цикле: brpop по очереди 'tasks', обрабатывает номера
параллельно (asyncio.gather + to_thread), обновляет статус задачи.
"""

import os
import asyncio
from pathlib import Path

import phonenumbers
from phonenumbers import carrier, geocoder
import redis.asyncio as redis
import yaml

# -----------------------------------------------------------------------------
# Загрузка конфигурации
# -----------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"


def load_config() -> dict:
    """
    Читает конфигурацию из config.yaml в папке phone-service.
    Если файла нет или он пустой — возвращаются значения по умолчанию.
    """
    defaults = {
        "redis": {"host": "localhost", "port": 6379},
        "queue": {"name": "tasks"},
    }
    if not CONFIG_PATH.exists():
        return defaults
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if not data:
            return defaults
        for key, value in defaults.items():
            if key not in data:
                data[key] = value
        return data
    except Exception:
        return defaults


config = load_config()

# -----------------------------------------------------------------------------
# Подключение к Redis
# -----------------------------------------------------------------------------

# Пытаемся взять адрес из переменной окружения (которую мы прописали в yaml)
# Если её нет (запуск без докера), используем localhost
REDIS_ADDR = os.getenv("REDIS_HOST", "redis")

redis_client = redis.Redis(
    host=REDIS_ADDR,
    port=config["redis"]["port"],
    decode_responses=True,
)

# Имя очереди, из которой забираем task_id (должно совпадать с gateway)
QUEUE_NAME = config["queue"].get("name", "tasks")


# -----------------------------------------------------------------------------
# Обработка одного номера (страна + оператор)
# -----------------------------------------------------------------------------

def _parse_phone(phone: str) -> str:
    """
    Синхронно определяет страну и оператора по номеру через библиотеку phonenumbers.

    Номер может быть с '+' или без; возвращается строка вида "Country: Operator".
    Вызывается из asyncio.to_thread, чтобы не блокировать event loop.
    """
    plus_phone = phone if phone.startswith("+") else "+" + phone
    parsed = phonenumbers.parse(plus_phone, None)
    country = geocoder.country_name_for_number(parsed, "en")
    operator = carrier.name_for_number(parsed, "en")
    return f"{country}: {operator}"


async def _process_one_phone(phone: str, semaphore: asyncio.Semaphore) -> tuple[str, str]:
    """
    Обрабатывает один номер в отдельном потоке (asyncio.to_thread).

    Парсинг phonenumbers — CPU-bound, поэтому выполняем в пуле потоков,
    чтобы не блокировать цикл событий и обрабатывать много номеров параллельно.
    Возвращает кортеж (номер, строка "страна: оператор") или (номер, "Error: ...")
    при исключении.
    """
    async with semaphore:
        try:
            result = await asyncio.to_thread(_parse_phone, phone)
            return (phone, result)
        except Exception as e:
            return (phone, f"Error: {e}")

# -----------------------------------------------------------------------------
# Основной цикл воркера
# -----------------------------------------------------------------------------


async def phone_service() -> None:
    """
    Бесконечный цикл: ожидание задачи из Redis, параллельная обработка номеров,
    запись результата и обновление статуса.

    Шаги:
    1. brpop(QUEUE_NAME) — блокирующее ожидание task_id.
    2. Статус задачи -> "processing".
    3. Сбор всех номеров из task:{task_id}:phones через hscan_iter.
    4. Параллельная обработка номеров (asyncio.gather + to_thread).
    5. Запись результатов в Redis одним pipeline.
    6. Статус задачи -> "processed".
    """

    # Ограничиваем количество потоков
    MAX_CONCURRENT_THREADS = os.cpu_count() or 2
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_THREADS)

    while True:
        res = await redis_client.brpop(QUEUE_NAME, 0)

        if not res:
            continue

        _queue_name, task_id = res
        print(task_id)

        await redis_client.set(f"task:{task_id}:status", "processing")

        # Собираем все номера из хеша (итератор по полям, без полной загрузки в память)
        phones = []
        async for phone, _ in redis_client.hscan_iter(f"task:{task_id}:phones"):
            phones.append(phone)

        # Обрабатываем номера параллельно
        results = await asyncio.gather(
            *[_process_one_phone(phone, semaphore=semaphore) for phone in phones],
            return_exceptions=True,
        )

        # Записываем результаты одним pipeline
        pipe = redis_client.pipeline()
        for result in results:
            # Проверяем, не является ли результат объектом исключения
            if isinstance(result, Exception):
                print(f"Критическая ошибка при обработке номера: {result}")
                continue
            
            # Теперь распаковка безопасна
            phone, data = result
            pipe.hset(f"task:{task_id}:phones", phone, data)
            
        await pipe.execute()

        await redis_client.set(f"task:{task_id}:status", "processed")


# -----------------------------------------------------------------------------
# Точка входа
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        asyncio.run(phone_service())
    except KeyboardInterrupt:
        print("\nService STOP")
