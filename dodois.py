import io
import random
from datetime import datetime

import pandas as pd
import requests

from pandas import CategoricalDtype

from config import CONNECT_TIMEOUT
from postgresql import Database


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
            if response.ok:
                self._authorized = True

    def parse_clients(self) -> None:
        if self._authorized:
            self._response = self._session.post('https://officemanager.dodopizza.ru/Reports/ClientsStatistic/Export',
                                                data={
                                                    'unitsIds': self._unit_id,
                                                    'beginDate': self._start_date.strftime('%d.%m.%Y'),
                                                    'endDate': self._end_date.strftime('%d.%m.%Y'),
                                                    'hidePhoneNumbers': 'false'})

    def read_response(self) -> pd.DataFrame:
        result = io.BytesIO(self._response.content)
        return pd.read_excel(result, skiprows=10, dtype='object')

    def process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:

        # Добавляем категорийный столбец first_order_types, который будет хранить значения Направления первого заказа
        order_type = CategoricalDtype(categories=['Доставка', 'Самовывоз', 'Ресторан'], ordered=True)
        df['first_order_type'] = df['Направление первого заказа'].astype(order_type).cat.codes

        # Дата первого заказа лежит в переданном диапазоне, который совпадает с диапазоном выгрузки
        df = df.drop(df[df['Дата первого заказа'] < self._start_date].index)
        df = df.drop(df[df['Дата последнего заказа'] >= self._end_date].index)

        # Отдел соответствует отделу первого И последнего заказа ! НО: МНОГО ПИЦЦЕРИЙ В ГОРОДЕ TODO
        df = df.drop(df[df['Отдел первого заказа'] != self._unit_name].index)
        df = df.drop(df[df['Отдел последнего заказа'] != self._unit_name].index)

        # Номер начинается на +79
        df = df.drop(df[~df['№ телефона'].str.startswith('+79')].index)

        return df


class DodoISStorer:
    def __init__(self, unit_id: int, db: Database = None):
        if not db:
            self._db = Database()
            self._db.connect()
            self._external_db = False
        else:
            self._db = db
            self._external_db = True
        self._unit_id = unit_id

    def store_clients(self, df: pd.DataFrame):
        for row in df.iterrows():
            self._db.execute("INSERT INTO clients (country_code, unit_id, phone, first_order_datetime, "
                             "first_order_city, last_order_datetime, last_order_city, first_order_type, sms_text, "
                             "sms_text_city, ftp_path_city) VALUES (%(country_code)s, %(unit_id)s, %(phone)s, "
                             "%(first_date)s, %(first_city)s, %(last_date)s, %(last_city)s, %(first_type)s, %(text)s, "
                             "%(text_city)s, %(path_city)s) "
                             "ON CONFLICT DO NOTHING",
                             {'country_code': 'ru', 'unit_id': self._unit_id, 'phone': row[1]['№ телефона'],
                              'first_date': row[1]['Дата первого заказа'], 'first_city': row[1]['Отдел первого заказа'],
                              'last_date': row[1]['Дата последнего заказа'],
                              'last_city': row[1]['Отдел последнего заказа'],
                              'first_type': row[1]['first_order_type'], 'text': '', 'text_city': '', 'path_city': ''})

        if not self._external_db:
            self._db.close()