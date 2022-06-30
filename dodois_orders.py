from dodois import DodoISParser, DodoEmptyExcelError, DodoResponseError
from datetime import datetime 
from typing import List
import io
import time
import config
from parser import DatabaseWorker
from postgresql import Database

import pandas as pd

class DodoISParserOrders(DodoISParser):

    def _parse_report(self, start_date: datetime, end_date: datetime) -> None:
        """
        Парсим отчет "Заказы" и сохраняем результат в self._response.
        :param start_date: Начало итервала
        :param end_date: Конец интервала
        Разбиваем на части(self._split_time_params()), если интервал
        более 30 дней.
        :retrun: None
        """
        # Authorization.
        if not self._authorized:
            self._auth()

        # Send request to report and write it to attr self._response.
        url = 'https://officemanager.dodopizza.ru/Reports/Orders/Export'
        order_sources = [
                'Telephone',
                'Site',
                'Restaurant',
                'DefectOrder',
                'Mobile',
                'Pizzeria',
                'Aggregator'
                ]
        select_order_types = [
                'Delivery',
                'Pickup',
                'Stationary'
                ]
        data = {
                'unitsIds': self._unit_id,
                'OrderSources': order_sources, 
                'beginDate': start_date.strftime('%d.%m.%Y'),
                'endDate': end_date.strftime('%d.%m.%Y'),
                'SelectOrderTypes': select_order_types 
                }
        self._response = self._session.post(url, data=data) 

    def _read_response(self) -> pd.DataFrame:
        """
        Process response to pandas dataframe object
        :return: dataframe
        """
        if self._response.ok:
            result = io.BytesIO(self._response.content)
            # Convert to pandas object data type.
            return pd.read_excel(result, skiprows=7, dtype='object')
        else:
            raise DodoResponseError

    def _process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare dataframe to write to database
        :param df: raw dataframe
        :return: processed dataframe
        """
        if len(df) == 0:
            raise DodoEmptyExcelError
        
        # Phone number starts with +79.
        df = df.drop(df[~df['Номер телефона'].str.startswith('+79', na=False)].index) 

        # Only order with status "Доставка".
        df = df[df['Статус заказа'].str.contains('Доставка', na=False)]

        # Delete unnecessary column.
        df = df[[
                'Подразделение', 
                'Отдел',
                'Дата', 
                '№ заказа', 
                'Тип заказа', 
                'Номер телефона', 
                'Сумма заказа' 
                ]]
        
        return df

    def _concatenate(self, dfs: List[pd.DataFrame]) -> pd.DataFrame:
       """
       Concatenate dataframes.
       :param dfs: dataframes list with similar columns.
       :return: dataframe.
       """
       df = pd.concat(dfs)
       df = df.sort_values('Дата')
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
                    attempts = 0
                except DodoEmptyExcelError:
                    if end_date < self._end_date:
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

class DodoISStorerOrders(DatabaseWorker):
    
    def __init__(self, id_: int, db: Database = None):
        super().__init__(db)
        self._id = id_

    def store(self, df: pd.DataFrame):
        params = []
        for row in df.iterrows():
            params.append((
                    self._id,
                    row[1]['Подразделение'],
                    row[1]['Отдел'],
                    row[1]['№ заказа'],
                    row[1]['Тип заказа'],
                    row[1]['Номер телефона'],
                    row[1]['Дата'],
                    row[1]['Сумма заказа']
                    ))
        query = """
            INSERT INTO orders (db_unit_id, head_unit, unit_name, order_number, order_type, phone_number, date, order_sum) VALUES %s;
                """
        self._db.execute(query, params)

        # Write last update date.
        self._db.execute(
                """
                UPDATE auth
                SET last_update = now() AT TIME ZONE 'UTC'
                WHERE auth.db_unit_id = %s;
                """,
                (self._id,)
                )
        self.db_close()

