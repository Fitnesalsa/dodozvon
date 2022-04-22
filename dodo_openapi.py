import requests

from postgresql import Database


class DodoOpenAPIParser:
    def __init__(self):
        self._session = requests.Session()
        self._public_api_address = 'https://publicapi.dodois.io/ru/api/v1/unitinfo'

    def parse_unit_info(self) -> dict:
        result = self._session.get(self._public_api_address)
        # Читаем значение json-объекта
        return result.json()


class DodoOpenAPIStorer:
    def __init__(self):
        self._db = Database()
        self._db.connect()

    def store_unit_info(self, json_: dict):
        for unit in json_:
            # unit - это словарь
            if unit['Approve'] and not unit['IsTemporarilyClosed']:
                query = """INSERT INTO units (country_code, unit_id, uuid, unit_name, tz_shift) 
                VALUES (%(code)s, %(id)s, %(uuid)s, %(name)s, %(tz_shift)s)
                ON CONFLICT (country_code, unit_id) DO UPDATE
                SET uuid = %(uuid)s, unit_name = %(name)s, tz_shift = %(tz_shift)s;"""
                params = {'code': 'ru', 'id': unit['Id'], 'uuid': unit['UUId'], 'name': unit['Name'],
                          'tz_shift': unit['TimeZoneShift']}
                self._db.execute(query, params)
        self._db.close()
