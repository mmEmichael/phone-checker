from fastapi import FastAPI
import redis.asyncio as redis
import uuid

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

app = FastAPI()

@app.post("/process")
async def start_task(phones: list[str]):
    # создаем id
    task_id = str(uuid.uuid4())
    # создаем задачу
    await create_task(task_id, phones)
    
    #возвращаем id задачи
    return {"message": "Task created", "task_id": task_id}

# обрабатываем get запрос выдаем номера
@app.get("/result")
async def get_result(task_id: str):
    status = await redis_client.get(f"task:{task_id}:status")
    if status in ["accepted", "processing"]:
       return f"Task ID: {task_id} {status}"
    elif status == "processed":
        result = await redis_client.hgetall(f"task:{task_id}:phones")
        await delete_task(task_id)
        return result
    else:
        return "Task not found"

# Создаём задачу: загружаем номера и добавляем задачу в очередь (один round-trip через pipeline)
async def create_task(task_id: str, phones: list[str]):
    pipe = redis_client.pipeline()
    pipe.set(f"task:{task_id}:status", "accepted")
    for phone in phones:
        pipe.hset(f"task:{task_id}:phones", phone, 0)
    pipe.lpush("tasks", task_id)
    await pipe.execute()

# Удаляем статус и номера задачи (один round-trip через pipeline)
async def delete_task(task_id: str):
    pipe = redis_client.pipeline()
    pipe.delete(f"task:{task_id}:phones")
    pipe.delete(f"task:{task_id}:status")
    await pipe.execute()