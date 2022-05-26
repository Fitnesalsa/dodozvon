import io
import random
import time
from datetime import datetime, timedelta
from typing import List, Tuple

import pandas as pd
import requests

from pandas import CategoricalDtype

import config
from config import CONNECT_TIMEOUT
from parser import DatabaseWorker
from postgresql import Database


class DodoAuthError(Exception):
    def __init__(self, message: str = 'Ошибка авторизации'):
        self.message = message
        super().__init__(self.message)
        
        
class DodoResponseError(Exception):
    def __init__(self, message: str = 'Ошибка выгрузки'):
        self.message = message
        super().__init__(self.message)


class DodoEmptyExcelError(Exception):
    def __init__(self, message: str = 'Выгружен пустой файл Excel'):
        self.message = message
        super().__init__(self.message)


class DodoISParser:
    """
    Класс для сбора данных из ДОДО ИС с заданными параметрами
    """

    def __init__(self, unit_id: int, unit_name: str, login: str, password: str, tz_shift: int,
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
        self._tz_shift = tz_shift
        self._this_timezone = config.TIMEZONES[self._tz_shift]

    @staticmethod
    def _split_time_params(start_date: datetime, end_date: datetime, max_days: int = 30) -> List[Tuple[datetime]]:
        """
        Генерирует серию параметров начальной и конечной даты для интервала, который больше заданного.
        Пример:
        _split_time_params(
            start_date=datetime(year=2021, month=1, day=1),
            end_date=datetime(year=2021, month=12, day=31),
            max_days=30) = [
                (datetime(year=2021, month=1, day=1), datetime(year=2021, month=1, day=30),
                (datetime(year=2021, month=1, day=31), datetime(year=2021, month=3, day=1),
                (datetime(year=2021, month=3, day=2), datetime(year=2021, month=3, day=31),
                .....
                (datetime(year=2021, month=12, day=27), datetime(year=2021, month=12, day=31)
            ]
        :param start_date: datetime, начало интервала
        :param end_date: datetime, конец интервала
        :param max_days: integer, максимальная длина интервала в днях
        :return: список кортежей с двумя датами: начало и конец субинтервала включительно.
        """
        # проверка параметров
        if start_date > end_date:
            raise AttributeError('start_date cannot be later than end_date!')
        if max_days < 1:
            raise AttributeError('max_days cannot be zero or negative!')
        result_dates = []
        interval_start_date = start_date
        interval_end_date = interval_start_date + timedelta(days=max_days)
        while True:
            if interval_end_date < end_date:
                result_dates.append((interval_start_date, interval_end_date - timedelta(days=1)))
                interval_start_date = interval_end_date
                interval_end_date = interval_start_date + timedelta(days=max_days)
            else:
                result_dates.append((interval_start_date, end_date))
                break
        return result_dates

    def _auth(self) -> None:
        if not self._authorized:
            response = self._session.post('https://auth.dodopizza.ru/Authenticate/LogOn',
                                          data=self._auth_payload,
                                          headers=self._headers_auth,
                                          allow_redirects=True, timeout=CONNECT_TIMEOUT)
            if response.ok and response.url != 'https://auth.dodopizza.ru/Authenticate/LogOn':
                self._authorized = True

            elif response.url == 'https://auth.dodopizza.ru/Authenticate/LogOn':
                raise DodoAuthError('Ошибка авторизации. Проверьте правильность данных.')

    def _parse_report(self, start_date: datetime, end_date: datetime) -> None:
        if not self._authorized:
            self._auth()
        self._response = self._session.post('https://officemanager.dodopizza.ru/Reports/ClientsStatistic/Export',
                                            data={
                                                'unitsIds': self._unit_id,
                                                'beginDate': start_date.strftime('%d.%m.%Y'),
                                                'endDate': end_date.strftime('%d.%m.%Y'),
                                                'hidePhoneNumbers': 'false'})

    def _read_response(self) -> pd.DataFrame:
        if self._response.ok:
            result = io.BytesIO(self._response.content)
            return pd.read_excel(result, skiprows=10, dtype='object')
        else:
            raise DodoResponseError

    def _process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:

        if len(df) == 0:
            raise DodoEmptyExcelError

        # Добавляем категорийный столбец first_order_types, который будет хранить значения Направления первого заказа
        order_type = CategoricalDtype(categories=['Доставка', 'Самовывоз', 'Ресторан'], ordered=True)
        df['first_order_type'] = df['Направление первого заказа'].astype(order_type).cat.codes

        # Дата первого заказа лежит в переданном диапазоне, который совпадает с диапазоном выгрузки
        # df = df.drop(df[df['Дата первого заказа'].dt.date < self._start_date].index)
        # df = df.drop(df[df['Дата последнего заказа'].dt.date > self._end_date].index)

        # Сохраняем tz в даты
        df['Дата первого заказа'] = df['Дата первого заказа'].dt.tz_localize(self._this_timezone)
        df['Дата последнего заказа'] = df['Дата последнего заказа'].dt.tz_localize(self._this_timezone)

        # Переводим всё в UTC
        df['Дата первого заказа'] = df['Дата первого заказа'].dt.tz_convert('UTC')
        df['Дата последнего заказа'] = df['Дата последнего заказа'].dt.tz_convert('UTC')

        # Отдел соответствует отделу первого И последнего заказа
        # city_name = re.match(r'([А-Яа-я -]+)[ -][0-9 -]+', self._unit_name).group(1)
        # df = df.drop(df[~df['Отдел первого заказа'].str.startswith(city_name)].index)
        # df = df.drop(df[~df['Отдел последнего заказа'].str.startswith(city_name)].index)

        # Номер начинается на +79
        df = df.drop(df[~df['№ телефона'].str.startswith('+79')].index)

        # Удаляем лишние столбцы
        df = df[['№ телефона', 'Дата первого заказа', 'Отдел первого заказа', 'Дата последнего заказа',
                 'Отдел последнего заказа', 'first_order_type', 'Кол-во заказов', 'Сумма заказа']]

        return df

    @staticmethod
    def _concatenate(dfs: List[pd.DataFrame]) -> pd.DataFrame:
        df = pd.concat(dfs)
        groupby_cols = ['№ телефона', 'Дата первого заказа', 'Отдел первого заказа', 'first_order_type']
        agg_dict = {
            'Дата последнего заказа': 'max',
            'Отдел последнего заказа': 'max',
            'Кол-во заказов': 'sum',
            'Сумма заказа': 'sum'
        }
        df = df.groupby(groupby_cols, as_index=False).agg(agg_dict)
        return df

    def parse(self) -> pd.DataFrame:
        dfs = []
        for start_date, end_date in self._split_time_params(self._start_date, self._end_date):
            attempts = config.PARSE_ATTEMPTS
            while attempts > 0:
                attempts -= 1
                try:
                    self._parse_report(start_date, end_date)
                    df = self._read_response()
                    dfs.append(self._process_dataframe(df))
                    attempts = 0  # если всё получилось и исключение не сработало, обнуляем счетчик попыток сразу
                except DodoEmptyExcelError:
                    if end_date < self._end_date:  # если пиццерия открылась после начала срока, не выдаем ошибку
                        continue
                    else:
                        if attempts == 0:
                            raise DodoEmptyExcelError
                        time.sleep(2)
                except Exception as e:
                    if attempts == 0:
                        raise e
                    time.sleep(2)
        self._session.close()
        return self._concatenate(dfs)


class DodoISStorer(DatabaseWorker):
    def __init__(self, id_: int, db: Database = None):
        super().__init__(db)
        self._id = id_

    def store(self, df: pd.DataFrame):
        params = []
        for row in df.iterrows():
            params.append((self._id, row[1]['№ телефона'], row[1]['Дата первого заказа'],
                           row[1]['Отдел первого заказа'], row[1]['Дата последнего заказа'],
                           row[1]['Отдел последнего заказа'], row[1]['first_order_type'],
                           row[1]['Кол-во заказов'], row[1]['Сумма заказа'], '', '', ''))
        query = """INSERT INTO clients (db_unit_id, phone, first_order_datetime, first_order_city, 
                   last_order_datetime, last_order_city, first_order_type, orders_amt, orders_sum,
                   sms_text, sms_text_city, ftp_path_city) VALUES %s
                   ON CONFLICT (db_unit_id, phone) DO UPDATE
                   SET (last_order_datetime, last_order_city, orders_amt, orders_sum) = 
                   (EXCLUDED.last_order_datetime, EXCLUDED.last_order_city, 
                   EXCLUDED.orders_amt + clients.orders_amt,
                   EXCLUDED.orders_sum + clients.orders_sum);
                   """
        self._db.execute(query, params)

        # записываем дату последнего обновления
        self._db.execute("""
        UPDATE auth
        SET last_update = now() AT TIME ZONE 'UTC'
        WHERE auth.db_unit_id = %s;
        """, (self._id,))

        self.db_close()
