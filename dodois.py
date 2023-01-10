import io
import time
from datetime import datetime, timedelta
from typing import List, Tuple

import pandas as pd
import requests

from pandas import CategoricalDtype

import config
from psycopg2.errors import StringDataRightTruncation, NumericValueOutOfRange
from config import CONNECT_TIMEOUT
from parser import DatabaseWorker
from postgresql import Database


class DodoAuthError(Exception):
    """
    Исключение, выдается если не подошла пара "логин/пароль".
    """
    def __init__(self, message: str = 'Ошибка авторизации'):
        self.message = message
        super().__init__(self.message)
        
        
class DodoResponseError(Exception):
    """
    Исключение, выдается если возникла ошибка в ответе от сервера Додо ИС.
    """
    def __init__(self, message: str = 'Ошибка выгрузки'):
        self.message = message
        super().__init__(self.message)


class DodoEmptyExcelError(Exception):
    """
    Исключение, выдается если из отчета выгружен пустой файл Excel.
    """
    def __init__(self, message: str = 'Выгружен пустой файл Excel'):
        self.message = message
        super().__init__(self.message)


class DodoISParser:
    """
    Класс для сбора данных из ДОДО ИС с заданными параметрами.
    При инициализации принимает параметры для парсинга одной пиццерии.
    Один экземпляр класса отвечает за доступ только к одной пиццерии. Если нужен доступ к нескольким пиццериям,
    нужно создать несколько экземпляров класса.
    :param unit_id: int внутренний id пиццерии согласно API Dodo (хранится в таблице units)
    :param unit_name: str название пиццерии согласно принципам наименования пиццерий (хранится в таблице units)
    :param login: str учетная запись, хранится в таблице auth
    :param password: str пароль, хранится в таблице auth
    :param tz_shift: int сдвиг часового пояса пиццерии относительно GMT, хранится в таблице units
    :param start_date: datetime начало периода выгрузки
    :param end_date: datetime конец периода выгрузки включительно
    Для параметров start_date и end_date используется только часть до дня включительно; часы-минуты-секунды не влияют
    на параметры.
    Если период выгрузки превышает 30 дней, он разбивается на куски по 30 дней и выгрузка происходит кусками
    последовательно.
    """

    def __init__(self, unit_id: int, unit_name: str, login: str, password: str, tz_shift: int,
                 start_date: datetime, end_date: datetime, promos: str):
        # заголовки для запроса с авторизацией
        self._headers_auth = {'origin': 'https://auth.dodopizza.ru',
                              'referer': 'https://auth.dodopizza.ru/Authenticate/LogOn',
                              'User-Agent': 'dodoextbot'}
        # данные для POST-запроса авторизации
        self._auth_payload = {'State': '',
                              'fromSiteId': '',
                              'CountryCode': 'Ru',
                              'login': login,
                              'password': [password, 'ltr']}
        # флаг для определения статуса авторизации
        self._authorized = False
        self._session = requests.Session()
        # переменная для сохранения результата запроса
        self._response = None
        self._unit_id = unit_id
        self._unit_name = unit_name
        self._start_date = start_date
        self._end_date = end_date
        self._tz_shift = tz_shift
        self._promos = promos.split(',')
        # сохраняем значение часовой зоны строкой
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
        :param end_date: datetime, конец интервала включительно
        :param max_days: integer, максимальная длина интервала в днях
        :return: список кортежей с двумя датами: начало и конец субинтервала включительно.
        """
        # проверка параметров
        if start_date > end_date:
            raise AttributeError('Дата начала не может быть позже даты окончания')
        if max_days < 1:
            raise AttributeError('Параметр max_days должен быть больше нуля')

        result_dates = []
        interval_start_date = start_date
        interval_end_date = interval_start_date + timedelta(days=max_days)
        while True:
            # если мы не дошли до конца интервала
            if interval_end_date < end_date:
                # сохраняем кортеж с текущими параметрами
                result_dates.append((interval_start_date, interval_end_date - timedelta(days=1)))
                interval_start_date = interval_end_date
                interval_end_date = interval_start_date + timedelta(days=max_days)
            else:
                result_dates.append((interval_start_date, end_date))
                break
        return result_dates

    def _auth(self) -> None:
        """
        Авторизуемся в Додо ИС с текущими параметрами. Авторизация выполняется один раз перед началом запросов.
        Срок действия авторизации в Додо ИС - около 15 минут в случае неактивности.
        :return: None
        """
        if not self._authorized:
            response = self._session.post('https://auth.dodopizza.ru/Authenticate/LogOn',
                                          data=self._auth_payload,
                                          headers=self._headers_auth,
                                          allow_redirects=True, timeout=CONNECT_TIMEOUT)
            if response.ok and response.url != 'https://auth.dodopizza.ru/Authenticate/LogOn':
                self._authorized = True

            elif response.url == 'https://auth.dodopizza.ru/Authenticate/LogOn':
                raise DodoAuthError('Ошибка авторизации. Проверьте правильность данных.')

    def _parse_clients_statistic(self, **kwargs) -> None:
        """
        Парсим отчет "Статистика по клиентам" и сохраняем результат в self._response.
        :param start_date: Начало интервала
        :param end_date: Конец интервала
        Если интервал более 30 дней, необходимо разбить на части. self._split_time_params()
        :return: None
        """
        # Сначала авторизуемся
        if not self._authorized:
            self._auth()

        # Отправляем запрос к отчету и записываем в атрибут self._response
        # Ответ ожидается в виде Excel-файла.
        self._response = self._session.post('https://officemanager.dodopizza.ru/Reports/ClientsStatistic/Export',
                                            data={
                                                'unitsIds': self._unit_id,
                                                'beginDate': kwargs['start_date'].strftime('%d.%m.%Y'),
                                                'endDate': kwargs['end_date'].strftime('%d.%m.%Y'),
                                                'hidePhoneNumbers': 'false'})

    def _parse_promo(self, **kwargs) -> None:
        """
        Парсим отчет "Расход промо-кодов" и сохраняем результат в self._response.
        :param start_date: Начало интервала
        :param end_date: Конец интервала
        :param promos: Список промокодов
        Если интервал более 30 дней, необходимо разбить на части. self._split_time_params()
        :return: None
        """
        # Сначала авторизуемся
        if not self._authorized:
            self._auth()

        # Отправляем запрос к отчету и записываем в атрибут self._response
        # Ответ ожидается в виде Excel-файла.
        self._response = self._session.post('https://officemanager.dodopizza.ru/Reports/PromoCodeUsed/Export',
                                            data={
                                                'filterType': '',
                                                'unitsIds': self._unit_id,
                                                'OrderSources': ['Telephone', 'Site', 'Restaurant', 'DefectOrder',
                                                                 'Mobile', 'Pizzeria', 'Aggregator'],
                                                'beginDate': kwargs['start_date'].strftime('%d.%m.%Y'),
                                                'endDate': kwargs['end_date'].strftime('%d.%m.%Y'),
                                                'orderTypes': ['Delivery', 'Pickup', 'Stationary'],
                                                'promoCode': kwargs['promo'],
                                                'IsAllPromoCode': 'false',
                                                'OnlyComposition': 'false'
                                            })

    def _parse_orders(self, **kwargs) -> None:
        """
        Парсим отчет "Заказы" и сохраняем результат в self._response.
        :param start_date: Начало интервала
        :param end_date: Конец интервала
        Если интервал более 30 дней, необходимо разбить на части. self._split_time_params()
        :return: None
        """
        # Сначала авторизуемся
        if not self._authorized:
            self._auth()

        # Отправляем запрос к отчету и записываем в атрибут self._response
        # Ответ ожидается в виде Excel-файла.
        self._response = self._session.post('https://officemanager.dodopizza.ru/Reports/Orders/Export',
                                            data={
                                                'filterType': 'AllOrders',
                                                'unitsIds': self._unit_id,
                                                'OrderSources': ['Telephone', 'Site', 'Restaurant', 'DefectOrder',
                                                                 'Mobile', 'Pizzeria', 'Aggregator'],
                                                'beginDate': kwargs['start_date'].strftime('%d.%m.%Y'),
                                                'endDate': kwargs['end_date'].strftime('%d.%m.%Y'),
                                                'orderTypes': ['Delivery', 'Pickup', 'Stationary']})

    def _read_response(self, skiprows: int) -> pd.DataFrame:
        """
        Преобразует ответ в датафрейм pandas.
        :return: датафрейм.
        """
        if self._response.ok:
            result = io.BytesIO(self._response.content)

            # При чтении вручную сохраняем все в тип "object" - аналог строки в pandas.
            # Конвертацию в правильные типы произведем позднее.
            return pd.read_excel(result, skiprows=skiprows, dtype='object')
        else:
            raise DodoResponseError

    def _process_df_clients_statistics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Обрабатываем сырой датафрейм, применяем фильтры и возвращаем в виде, готовом для записи в БД.
        :param df: сырой датафрейм
        :return: датафрейм
        """
        if len(df) == 0:
            raise DodoEmptyExcelError

        # Добавляем категорийный столбец first_order_types, который будет хранить значения Направления первого заказа
        order_type = CategoricalDtype(categories=['Доставка', 'Самовывоз', 'Ресторан'], ordered=True)
        df['first_order_type'] = df['Направление первого заказа'].astype(order_type).cat.codes

        # Сохраняем tz в даты
        df['Дата первого заказа'] = df['Дата первого заказа'].dt.tz_localize(self._this_timezone)
        df['Дата последнего заказа'] = df['Дата последнего заказа'].dt.tz_localize(self._this_timezone)

        # Переводим всё в UTC
        df['Дата первого заказа'] = df['Дата первого заказа'].dt.tz_convert('UTC')
        df['Дата последнего заказа'] = df['Дата последнего заказа'].dt.tz_convert('UTC')

        # Номер начинается на +79
        df = df.drop(df[~df['№ телефона'].str.startswith('+79')].index)

        # Удаляем лишние столбцы
        df = df[['№ телефона', 'Дата первого заказа', 'Отдел первого заказа', 'Дата последнего заказа',
                 'Отдел последнего заказа', 'first_order_type', 'Кол-во заказов', 'Сумма заказа']]

        return df

    def _process_df_promo(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Обрабатываем сырой датафрейм, применяем фильтры и возвращаем в виде, готовом для записи в БД.
        :param df: сырой датафрейм
        :return: датафрейм
        """
        if len(df) == 0:
            raise DodoEmptyExcelError
        return df

    def _process_df_orders(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Обрабатываем сырой датафрейм, применяем фильтры и возвращаем в виде, готовом для записи в БД.
        :param df: сырой датафрейм
        :return: датафрейм
        """
        if len(df) == 0:
            raise DodoEmptyExcelError

        # Переводим тип и статус заказа в категории
        order_type = CategoricalDtype(categories=['Доставка', 'Самовывоз', 'Ресторан'], ordered=True)
        df['Тип заказа'] = df['Тип заказа'].astype(order_type).cat.codes
        status_type = CategoricalDtype(categories=[
            'Доставка', 'Отказ', 'Просрочен', 'Упакован', 'В работе', 'Принят', 'Выполнен'], ordered=True)
        df['Статус заказа'] = df['Статус заказа'].astype(status_type).cat.codes

        # Сохраняем tz в даты
        df['Дата'] = df['Дата'].dt.tz_localize(self._this_timezone)

        # Переводим всё в UTC
        df['Дата'] = df['Дата'].dt.tz_convert('UTC')

        # Удаляем лишние столбцы
        df = df[['Дата', '№ заказа', 'Тип заказа', 'Номер телефона', 'Сумма заказа', 'Статус заказа']]

        return df

    @staticmethod
    def _concatenate_clients_statistic(dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Склеиваем несколько датафреймов в один.
        В итоговом датафрейме сохраняется уникальность номеров телефонов.
        Для каждого клиента (номера телефона) остается:
         - дата первого заказа (одинаковая во всех файлах),
         - отдел первого заказа (одинаковый во всех файлах),
         - направление первого заказ (одинаковое во всех файлах),
         - дата последнего заказа (берем самую позднюю),
         - отдел последнего заказа (берем самый поздний),
         - количество заказов (суммируем)
         - сумма заказа (суммируем)
        :param dfs: список датафреймов с одинаковыми столбцами
        :return: склеенный датафрейм
        """
        # сначала склеиваем как есть
        df = pd.concat(dfs)
        # сортируем по дате последнего заказа по убыванию
        df = df.sort_values('Дата последнего заказа', ascending=False)
        # поля, по которым группируем (уникальные для каждого номера телефона)
        groupby_cols = ['№ телефона', 'Дата первого заказа', 'Отдел первого заказа', 'first_order_type']
        # правила агрегирования:
        # - верхняя дата последнего заказа и отдел последнего заказа: т.к. мы отсортировали по дате,
        #   первой будет самая поздняя дата и соответствующий ей отдел
        # - сумма количества заказов
        # - сумма сумм заказов
        agg_dict = {
            'Дата последнего заказа': 'first',
            'Отдел последнего заказа': 'first',
            'Кол-во заказов': 'sum',
            'Сумма заказа': 'sum'
        }
        # группируем и возвращаем
        df = df.groupby(groupby_cols, as_index=False).agg(agg_dict)
        return df

    @staticmethod
    def _concatenate_promo(dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Склеиваем несколько датафреймов в один.
        :param dfs: список датафреймов с одинаковыми столбцами
        :return: склеенный датафрейм
        """
        df = pd.concat(dfs)
        return df

    @staticmethod
    def _concatenate_orders(dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Склеиваем несколько датафреймов в один.
        :param dfs: список датафреймов с одинаковыми столбцами
        :return: склеенный датафрейм
        """
        df = pd.concat(dfs)
        return df

    def parse(self, report_type: str) -> pd.DataFrame:
        """
        Парсинг отчетов
        :return: словарь
        """
        parse_functions = {'clients_statistic':
                               {'parser': self._parse_clients_statistic,
                                'processor': self._process_df_clients_statistics,
                                'concatenator': self._concatenate_clients_statistic,
                                'rows': 10},
                           'promo':
                               {'parser': self._parse_promo,
                                'processor': self._process_df_promo,
                                'concatenator': self._concatenate_promo,
                                'rows': 4},
                           'orders':
                               {'parser': self._parse_orders,
                                'processor': self._process_df_orders,
                                'concatenator': self._concatenate_orders,
                                'rows': 7}
                           }
        dfs = []
        # делим общий интервал на субинтервалы
        for start_date, end_date in self._split_time_params(self._start_date, self._end_date):
            print(f'parsing from {start_date:%d.%m.%Y} to {end_date:%d.%m.%Y}')
            for promo in self._promos:
                # задаем количество попыток для запросов
                attempts = config.PARSE_ATTEMPTS
                while attempts > 0:
                    attempts -= 1
                    try:
                        # парсим отчет с субинтервалом в качестве начала и конца
                        parse_functions[report_type]['parser'](start_date=start_date, end_date=end_date, promo=promo)
                        # читаем и получаем датафрейм
                        df = self._read_response(skiprows=parse_functions[report_type]['rows'])
                        # добавляем к списку
                        dfs.append(parse_functions[report_type]['processor'](df))
                        attempts = 0  # если всё получилось и исключение не сработало, обнуляем счетчик попыток сразу
                    except DodoEmptyExcelError:
                        # "прокидываем" ошибку выше, но делаем исключения
                        if report_type in ('promo', 'orders'):  # промокоды и заказы могут быть пустыми
                            attempts = 0
                        else:
                            if attempts == 0:
                                # если это была последняя попытка, выкидываем ошибку
                                raise DodoEmptyExcelError
                            # в противном случае спим 2 секунды и пробуем заново
                            time.sleep(2)
                    except Exception as e:
                        # если вылезло другое исключение, выкидываем ошибку, если последняя попытка, или спим
                        if attempts == 0:
                            raise e
                        time.sleep(2)
        # закрываем сессию и возвращаем датафрейм
        self._session.close()
        return parse_functions[report_type]['concatenator'](dfs)


class DodoISStorer(DatabaseWorker):
    """
    Класс записывает результат парсинга в датафрейме в БД в таблицу clients.
    """
    def __init__(self, id_: int, db: Database = None):
        super().__init__(db)
        self._id = id_

    def store(self, df_clients: pd.DataFrame, df_orders: pd.DataFrame):
        """
        Записываем построчно результат из датафрейма в БД.
        Предполагаем, что датафрейм уже подготовленный.
        :param df: датафрейм с результатами
        :return: None
        """
        # клиентская статистика
        if df_clients is not None:
            params = []
            for row in df_clients.iterrows():
                params.append((self._id, row[1]['№ телефона'], row[1]['Дата первого заказа'],
                               row[1]['Отдел первого заказа'], row[1]['Дата последнего заказа'],
                               row[1]['Отдел последнего заказа'], row[1]['first_order_type'],
                               row[1]['Кол-во заказов'], row[1]['Сумма заказа'], '', '', ''))
            query = """INSERT INTO clients (db_unit_id, phone, first_order_datetime, first_order_city, 
                       last_order_datetime, last_order_city, first_order_type, orders_amt, orders_sum,
                       sms_text, sms_text_city, ftp_path_city) VALUES %s
                       ON CONFLICT (phone) DO UPDATE
                       SET (db_unit_id, last_order_datetime, last_order_city, orders_amt, orders_sum) = 
                       (EXCLUDED.db_unit_id, EXCLUDED.last_order_datetime, EXCLUDED.last_order_city, 
                       EXCLUDED.orders_amt + clients.orders_amt, EXCLUDED.orders_sum + clients.orders_sum)
                       WHERE EXCLUDED.last_order_datetime > clients.last_order_datetime;
                       """
            self._db.execute(query, params)

        # заказы
        if df_orders is not None:
            params = []
            for row in df_orders.iterrows():
                params.append((self._id, *row[1]))
            query = """INSERT INTO orders (
                            db_unit_id, date, order_id, order_type, phone, order_sum, status
                       ) VALUES %s 
                       ON CONFLICT (db_unit_id, date, order_id) DO NOTHING;
                    """
            self._db.execute(query, params)

        # записываем дату последнего обновления в таблицу auth
        if df_clients is not None or df_orders is not None:
            self._db.execute("""
            UPDATE auth
            SET last_update = now() AT TIME ZONE 'UTC'
            WHERE auth.db_unit_id = %s;
            """, (self._id,))

        # закрываем соединение, если открывали
        self.db_close()
