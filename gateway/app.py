from fastapi import FastAPI
import redis.asyncio as redis
import uuid

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

#проверка подключения к редису

app = FastAPI()

@app.post("/process")
async def start_task(phones: list[str]):
    # валидация

    # создаем id
    task_id = str(uuid.uuid4())
    # создаем задачу
    await create_task(task_id, phones)
    
    #возвращаем id задачи
    return(f"Task reated. ID: {task_id}")

# обрабатываем get запрос выдаем номера
@app.get("/result")
async def get_result(task_id: str):
    status = await redis_client.get(f"task:{task_id}:status")
    if status in ["accepted", "processing"]:
       return f"Task ID: {task_id} {status}"
    if status == "processed":
        result = await redis_client.hgetall(f"task:{task_id}:phones")
        await delete_task(task_id)
        return result
    else:
        return "Task not wound"

async def create_task(task_id: str, phones: list[str]):
    #status
    await redis_client.set(f"task:{task_id}:status", "accepted")
    # запись с номерами
    for i in range(len(phones)):
        await redis_client.hset(f"task:{task_id}:phones", phones[i], 0)
    # добавляем id в очередь
    await redis_client.lpush("tasks", task_id)

async def delete_task(task_id: str):
    await redis_client.delete(f"task:{task_id}:phones")
    await redis_client.delete(f"task:{task_id}:status")