import config
import requests


class Bot:
    """
    Класс реализует взаимодействие с телеграм-ботом @dodozvon_bot.
    Поддерживает метод send_message для отправки простого текстового сообщения.
    """
    def __init__(self):

        # URL API - ссылка на АПИ бота
        self._api_url = 'https://api.telegram.org/'

        # токен для доступа к боту - формируется в модуле config из данных, хранящихся в .env
        self._token = config.TG_BOT_TOKEN

        # - список ID пользователей в формате INT - формируется в модуле config из данных, хранящихся в .env
        self._admin_ids = config.TG_ADMIN_ID

    def _get_updates(self) -> None:
        """
        Метод отправляет Get-запрос боту для получения накопленных входящих сообщений и ничего с ними не делает.
        Таким образом реализуется "очистка кеша".
        По сути мы не ожидаем входящих сообщений - эта функция - просто страховка.
        :return: None
        """
        url = f'{self._api_url}bot{self._token}/getUpdates'

        # отправляем GET-запрос
        requests.get(url)

    def send_message(self, message_text: str) -> None:
        """
        Отправляет сообщение админам. Список админов в .env в переменной TG_ADMIN_ID
        :param message_text: строка с текстом сообщения. Максимальная длина - 4096 байтов (см. АПИ телеграм-ботов).
        :return: None
        """

        # вызываем метод для очистки входящих
        self._get_updates()

        # формируем URL для отправки данных
        url = f'{self._api_url}bot{self._token}/sendMessage'

        # для всех ID в списке админов
        for admin_id in self._admin_ids:
            # формируем словарь (фигурные скобки) согласно АПИ телеграм-ботов c последующей передачей в requests
            data = {'chat_id': admin_id,
                    'text': message_text}
            # сигнатура метода Python: requests.post('https://httpbin.org/post', data={'key':'value'})
            requests.post(url, data=data)
