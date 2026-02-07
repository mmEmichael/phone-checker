import redis.asyncio as redis
import asyncio
import phonenumbers
from phonenumbers import carrier, geocoder

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)


def _parse_phone(phone: str) -> str:
    """Определение региона и оператора по номеру (синхронная часть)."""
    plus_phone = phone if phone.startswith("+") else "+" + phone
    parsed = phonenumbers.parse(plus_phone, None)
    country = geocoder.country_name_for_number(parsed, "en")
    operator = carrier.name_for_number(parsed, "en")
    return f"{country}: {operator}"


async def _process_one_phone(phone: str) -> tuple[str, str]:
    """Обработка одного номера; отдаём CPU в event loop для параллелизма."""
    result = await asyncio.to_thread(_parse_phone, phone)
    return (phone, result)


async def phone_service():
    while True:
        res = await redis_client.brpop("tasks", 0)

        if not res:
            continue

        _queue_name, task_id = res
        print(task_id)

        await redis_client.set(f"task:{task_id}:status", "processing")

        # Собираем все номера
        phones = []
        async for phone, _ in redis_client.hscan_iter(f"task:{task_id}:phones"):
            phones.append(phone)

        # Обрабатываем номера параллельно
        results = await asyncio.gather(
            *[_process_one_phone(phone) for phone in phones],
            return_exceptions=True
        )

        # Записываем результаты одним pipeline
        pipe = redis_client.pipeline()
        for item in results:
            if isinstance(item, Exception):
                print(f"Task {task_id}: error processing phone: {item}")
                continue
            phone, data = item
            pipe.hset(f"task:{task_id}:phones", phone, data)
        await pipe.execute()

        await redis_client.set(f"task:{task_id}:status", "processed")
try:
    asyncio.run(phone_service())
except KeyboardInterrupt:
    print("\n Service STOP")