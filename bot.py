import config
import requests


class Bot:
    def __init__(self):
        self._api_url = 'https://api.telegram.org/'
        self._token = config.TG_BOT_TOKEN
        self._admin_ids = config.TG_ADMIN_ID

    def _get_updates(self) -> None:
        url = f'{self._api_url}bot{self._token}/getUpdates'
        requests.get(url)

    def send_message(self, message_text: str):
        self._get_updates()
        url = f'{self._api_url}bot{self._token}/sendMessage'
        for admin_id in self._admin_ids:
            data = {'chat_id': admin_id,
                    'text': message_text}
            requests.post(url, data=data)
