import config
import requests


class Bot:
    def __init__(self):
        self._api_url = 'https://api.telegram.org/'
        self._token = config.TG_BOT_TOKEN
        self._admin_id = config.TG_ADMIN_ID

    def send_message(self, message_text: str):
        url = f'{self._api_url}bot{self._token}/sendMessage?chat_id={self._admin_id}text={message_text}'
        requests.get(url)
