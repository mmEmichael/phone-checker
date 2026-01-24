from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import config
from services.geoServiceClient import send_geo_request

class HTTPHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        status_code = 200 # by default
        bytes_data = "Сообщение принято".encode("utf-8") # by default
        path = self.path

        # Получаем размер данных из заголовка
        content_length = int(self.headers["Content-Length"])
         
        # Читаем сами данные
        post_data = self.rfile.read(content_length)

        # Декодируем и выводим в консоль сервера
        print(f"Получен POST запрос: {post_data.decode("utf-8")}")

        if path == "/numbers":
            phones = self.parse_phone_numbers(post_data)
            print(f"Номера: {phones}")
            response = send_geo_request(phones)
            bytes_data = response.text.encode("utf-8")
        else:
            status_code = 404
            bytes_data = "404".encode("utf-8")       
 
        # Отправляем ответ клиенту
        self.send_response(status_code)
        self.end_headers()
        self.wfile.write(bytes_data)

    # Парсим номера из POST запроса
    def parse_phone_numbers(self, raw_data):
        data = json.loads(raw_data.decode("utf-8"))
        return data.get("phones", [])  # массив номеров


# Инициализация сервера
webServer = HTTPServer((config.hostName, config.serverPort), HTTPHandler)
print(f"Сервер запущен: http://{config.hostName}:{config.serverPort}")

# Бесконечный цикл прослушивания порта
try: webServer.serve_forever()
except KeyboardInterrupt: pass
 
webServer.server_close()
print("Сервер остановлен...")