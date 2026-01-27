import redis.asyncio as redis
import asyncio

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

async def phone_service():
    while True:
        res = await redis_client.brpop("tasks", 0)
        
        queue_name, task_id = res # Распаковываем кортеж
        print(task_id)

        await redis_client.set(f"task:{task_id}:status", "processing") # статус: Начали обработку

        async for phone, reg in redis_client.hscan_iter(f"task:{task_id}:phones"): # переделать это место!!! Здесь вызывать функцию определения региона и оператора
            pass

        await redis_client.set(f"task:{task_id}:status", "processed") # статус: Закончили обработку

try:
    asyncio.run(phone_service())
except KeyboardInterrupt:
    print("\n Service STOP")