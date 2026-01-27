import redis.asyncio as redis
import asyncio

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

async def phone_service():
    while True:
        res = await redis_client.brpop("tasks", 0)
        queue_name, task_id = res # Распаковываем кортеж
        print(task_id)
        await get_region(task_id)


# функция заглушка
async def get_region(task_id):
    async for phone, reg in redis_client.hscan_iter(f"task:{task_id}:phones"):
        await redis_client.hset(f"task:{task_id}:phones", phone, "rus")
        print(await redis_client.hget(f"task:{task_id}:phones", phone))
    await redis_client.set(f"task:{task_id}:status", "processed")


try:
    asyncio.run(phone_service())
except KeyboardInterrupt:
    print("\n Service STOP")