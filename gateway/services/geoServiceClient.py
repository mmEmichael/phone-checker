import requests
import json
import config

#Запрос данных о регионе
def send_geo_request(phones):
    json_data = json.dumps({"phones": phones})
    
    try:
        response = requests.post(config.GEO_SERVICE_URL, json=json_data)
        response.raise_for_status() # Выдаст ошибку, если статус 4xx или 5xx
        return response.json()
    except requests.exceptions.ConnectionError:
        # Сюда попадем, если сервис выключен (ваш случай)
        print("Ошибка: Гео-сервис недоступен (Connection Refused)")
        return {"error": "Geo-service is down"}, 503
    except requests.exceptions.Timeout:
        # Сюда попадем, если сервис завис и не отвечает
        print("Ошибка: Время ожидания истекло")
        return {"error": "Service timeout"}, 504
    except Exception as e:
        print(f"Неизвестная ошибка: {e}")
        return {"error": "Internal gateway error"}, 500