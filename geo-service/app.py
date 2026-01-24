from http.server import HTTPServer, BaseHTTPRequestHandler
import config

class MyHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        # Получаем размер данных из заголовка
        content_length = int(self.headers["Content-Length"])
         
        # Читаем сами данные
        post_data = self.rfile.read(content_length)
         
        # Декодируем и выводим в консоль сервера
        print(f"Получен POST запрос: {post_data.decode("utf-8")}")
 
        # Отправляем ответ клиенту
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"geo service!!!")

# Инициализация сервера
webServer = HTTPServer((config.hostName, config.serverPort), MyHandler)
print(f"Сервер запущен: http://{config.hostName}:{config.serverPort}")
 
# Бесконечный цикл прослушивания порта
try: webServer.serve_forever()
except KeyboardInterrupt: pass
 
webServer.server_close()
print("Сервер остановлен...")