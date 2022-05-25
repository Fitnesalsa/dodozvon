from typing import Dict

import requests

from parser import DatabaseWorker
from postgresql import Database


class DodoOpenAPIParser:
    """
    Метод обращается к публичному API Dodo (https://publicapi.dodois.io/ru/api/v1/unitinfo)
    и возвращает результаты в виде словаря.
    """

    def __init__(self):
        """
        Инициализация DodoOpenAPIParser(). Без параметров.
        """
        # создаём новую сессию
        self._session = requests.Session()
        # сохраняем адрес API
        self._public_api_address = 'https://publicapi.dodois.io/ru/api/v1/unitinfo'

    def parse(self) -> Dict:
        """
        Парсинг.
        :return: словарь со значениями
        """
        # отправляем get-запрос на сервер и сохраняем ответ в переменную result
        result = self._session.get(self._public_api_address)
        # закрываем сессию (чтобы не выдавались ошибки)
        self._session.close()
        # Читаем значение json-объекта
        return result.json()


class DodoOpenAPIStorer(DatabaseWorker):
    """
    Метод ...
    """
    def __init__(self, db: Database = None):
        super().__init__(db)

    def store(self, json_: dict):
        params = []
        for unit in json_:
            # unit - это словарь
            if unit['Approve'] and not unit['IsTemporarilyClosed']:
                params.append(('ru', unit['Id'], unit['UUId'], unit['Name'], unit['TimeZoneShift']))
        query = """INSERT INTO units (country_code, unit_id, uuid, unit_name, tz_shift) VALUES %s
                   ON CONFLICT (country_code, unit_id) DO UPDATE
                   SET (uuid, unit_name, tz_shift) = (EXCLUDED.uuid, EXCLUDED.unit_name, EXCLUDED.tz_shift);"""
        # И отправляем всё одним запросом на сервер, иначе это занимает очень много времени
        self._db.execute(query, params)

        self._db_close()
