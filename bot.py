import config
import requests


class Bot:
    def __init__(self): # при инициализации объекта класса Bot ему передаются атрибуты
        self._api_url = 'https://api.telegram.org/' # URL API - ссылка на АПИ бота
        self._token = config.TG_BOT_TOKEN # - токен для доступа к боту - формируется в модуле config из данных, хранящихся в .env
        self._admin_ids = config.TG_ADMIN_ID # - список ID пользователей в формате INT -
                                             # формируется в модуле config из данных, хранящихся в .env

    # Метод ничего не возвращает, только отправляет Get-запрос боту для очистки накопленных входящих сообщений.
    # По сути мы не ожидаем входящих сообщений - эта функция - просто страховка. При ручном запросе возвращает False
    def _get_updates(self) -> None: # -> аннотация параметра метода, показывает что метод ничего не передает
        url = f'{self._api_url}bot{self._token}/getUpdates'
        requests.get(url)


    def send_message(self, message_text: str) -> None: # -> аннотация параметра метода, показывает что метод ничего не передает
        self._get_updates() # вызываем метод для очистки входящих
        url = f'{self._api_url}bot{self._token}/sendMessage' # формируем URL для отправки данных
        for admin_id in self._admin_ids: # для всех ID  в списке админов
            data = {'chat_id': admin_id,
                    'text': message_text} # формируем словарь (фигурные скобки) по сигнатуре Telegram
                                            # c последующей передачей в requests
            requests.post(url, data=data) # сигнатура метода Python: requests.post('https://httpbin.org/post', data={'key':'value'})
            #

