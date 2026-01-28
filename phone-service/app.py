import redis.asyncio as redis
import asyncio
import phonenumbers
from phonenumbers import carrier, geocoder

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

async def phone_service():
    while True:
        res = await redis_client.brpop("tasks", 0)
        
        queue_name, task_id = res # Распаковываем кортеж
        print(task_id)

        await redis_client.set(f"task:{task_id}:status", "processing") # статус: Начали обработку

        async for phone, data in redis_client.hscan_iter(f"task:{task_id}:phones"): # переделать это место!!! Здесь вызывать функцию определения региона и оператора
            # если номер без плюча, добавляем
            if phone[0] != "+":
                plus_phone = "+" + phone
            else:
                plus_phone = phone
            
            parsed_phone = phonenumbers.parse(plus_phone, None)
            country = geocoder.country_name_for_number(parsed_phone, "en")
            operator = carrier.name_for_number(parsed_phone, "en")
            data = country + ": " + operator

            #записываем данные 
            await redis_client.hset(f"task:{task_id}:phones", phone, data)

        await redis_client.set(f"task:{task_id}:status", "processed") # статус: Закончили обработку

try:
    asyncio.run(phone_service())
except KeyboardInterrupt:
    print("\n Service STOP")