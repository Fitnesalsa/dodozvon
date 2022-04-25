from urllib.parse import quote

import config
import requests


class Bot:
    def __init__(self):
        self._api_url = 'https://api.telegram.org/'
        self._token = config.TG_BOT_TOKEN
        self._admin_id = config.TG_ADMIN_ID

    def _get_updates(self):
        url = f'{self._api_url}bot{self._token}/getUpdates'
        requests.get(url)

    def send_message(self, message_text: str):
        self._get_updates()
        url = f'{self._api_url}bot{self._token}/sendMessage'
        data = {'chat_id': self._admin_id,
                'text': message_text}
        requests.post(url, data=data)

