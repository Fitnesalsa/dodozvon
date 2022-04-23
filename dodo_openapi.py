import requests

from postgresql import Database


class DodoOpenAPIParser:
    def __init__(self):
        self._session = requests.Session()
        self._public_api_address = 'https://publicapi.dodois.io/ru/api/v1/unitinfo'

    def parse(self) -> dict:
        result = self._session.get(self._public_api_address)
        # Читаем значение json-объекта
        return result.json()


class DodoOpenAPIStorer:
    def __init__(self, db: Database = None):
        if not db:
            self._db = Database()
            self._db.connect()
            self._external_db = False
        else:
            self._db = db
            self._external_db = True

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

        if not self._external_db:
            self._db.close()
