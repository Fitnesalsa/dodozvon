import io
import random
import re
from datetime import datetime

import pandas as pd
import requests

from pandas import CategoricalDtype

from bot import Bot
from config import CONNECT_TIMEOUT
from parser import DatabaseWorker
from postgresql import Database


class DodoAuthError(Exception):
    def __init__(self, message: str = 'Ошибка авторизации'):
        self.message = message
        super().__init__(self.message)


class DodoISParser:
    """
    Класс для сбора данных из ДОДО ИС с заданными параметрами
    """

    def __init__(self, unit_id: int, unit_name: str, login: str, password: str,
                 start_date: datetime, end_date: datetime):
        self._user_agents = [
            'Mozilla/5.0 (Windows NT 6.3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 YaBrowser/17.6.1.749 Yowser/2.5 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 YaBrowser/18.3.1.1232 Yowser/2.5 Safari/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 10_0 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 YaBrowser/17.4.3.195.10 Mobile/14A346 Safari/E7FBAF',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36']
        self._headers_auth = {'origin': 'https://auth.dodopizza.ru',
                              'referer': 'https://auth.dodopizza.ru/Authenticate/LogOn',
                              'User-Agent': random.choice(self._user_agents)}
        self._auth_payload = {'State': '',
                              'fromSiteId': '',
                              'CountryCode': 'Ru',
                              'login': login,
                              'password': [password, 'ltr']}
        self._authorized = False
        self._session = requests.Session()
        self._response = None
        self._unit_id = unit_id
        self._unit_name = unit_name
        self._start_date = start_date
        self._end_date = end_date

    def _auth(self) -> None:
        if not self._authorized:
            response = self._session.post('https://auth.dodopizza.ru/Authenticate/LogOn',
                                          data=self._auth_payload,
                                          headers=self._headers_auth,
                                          allow_redirects=True, timeout=CONNECT_TIMEOUT)
            if response.ok and response.url != 'https://auth.dodopizza.ru/Authenticate/LogOn':
                self._authorized = True

            elif response.url == 'https://auth.dodopizza.ru/Authenticate/LogOn':
                raise DodoAuthError(f'{self._unit_name}: ошибка авторизации. Проверьте правильность данных.')

    def _parse_report(self) -> None:
        if not self._authorized:
            self._auth()
        self._response = self._session.post('https://officemanager.dodopizza.ru/Reports/ClientsStatistic/Export',
                                            data={
                                                'unitsIds': self._unit_id,
                                                'beginDate': self._start_date.strftime('%d.%m.%Y'),
                                                'endDate': self._end_date.strftime('%d.%m.%Y'),
                                                'hidePhoneNumbers': 'false'})

    def _read_response(self) -> pd.DataFrame:
        result = io.BytesIO(self._response.content)
        return pd.read_excel(result, skiprows=10, dtype='object')

    def _process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:

        # Добавляем категорийный столбец first_order_types, который будет хранить значения Направления первого заказа
        order_type = CategoricalDtype(categories=['Доставка', 'Самовывоз', 'Ресторан'], ordered=True)
        df['first_order_type'] = df['Направление первого заказа'].astype(order_type).cat.codes

        # Дата первого заказа лежит в переданном диапазоне, который совпадает с диапазоном выгрузки
        df = df.drop(df[df['Дата первого заказа'] < self._start_date].index)
        df = df.drop(df[df['Дата последнего заказа'] >= self._end_date].index)

        # Отдел соответствует отделу первого И последнего заказа
        city_name = re.match(r'([А-Яа-я -]+)[ -][0-9 -]+', self._unit_name).group(1)
        df = df.drop(df[~df['Отдел первого заказа'].str.startswith(city_name)].index)
        df = df.drop(df[~df['Отдел последнего заказа'].str.startswith(city_name)].index)

        # Номер начинается на +79
        df = df.drop(df[~df['№ телефона'].str.startswith('+79')].index)

        return df

    def parse(self) -> pd.DataFrame:
        self._parse_report()
        df = self._read_response()
        self._session.close()
        return self._process_dataframe(df)


class DodoISStorer(DatabaseWorker):
    def __init__(self, unit_id: int, db: Database = None):
        super().__init__(db)
        self._unit_id = unit_id

    def store(self, df: pd.DataFrame):
        if len(df) > 0:
            params = []
            for row in df.iterrows():
                params.append(('ru', self._unit_id, row[1]['№ телефона'], row[1]['Дата первого заказа'],
                               row[1]['Отдел первого заказа'], row[1]['Дата последнего заказа'],
                               row[1]['Отдел последнего заказа'], row[1]['first_order_type'], '', '', ''))
            query = """INSERT INTO clients (country_code, unit_id, phone, first_order_datetime,
                       first_order_city, last_order_datetime, last_order_city, first_order_type, sms_text,
                       sms_text_city, ftp_path_city) VALUES %s
                       ON CONFLICT (country_code, unit_id, phone) DO UPDATE
                       SET (last_order_datetime, last_order_city) = 
                       (EXCLUDED.last_order_datetime, EXCLUDED.last_order_city);
                       """
            self._db.execute(query, params)
        else:
            bot = Bot()
            bot.send_message(f'{self._unit_id}: выгружен пустой файл Excel.')

        # записываем дату последнего обновления
        self._db.execute("""
        UPDATE auth
        SET last_update = now() AT TIME ZONE 'UTC'
        FROM units
        WHERE units.country_code = 'ru'
        AND units.unit_id = %s
        AND auth.unit_name = units.unit_name;
        """, (self._unit_id,))

        self._db_close()
