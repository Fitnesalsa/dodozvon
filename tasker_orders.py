import os
import pandas as pd

import config
from storage import YandexDisk
from parser import DatabaseWorker
from postgresql import Database
import datetime

class DatabaseTaskerOrders(DatabaseWorker):
    
    
    def __init__(self,
            begin_date: str,
            end_date: str,
            db: Database=None,
            upload_all: bool=True):
        self._storage = YandexDisk()
        self._begin_date = begin_date
        self._end_date = end_date
        self._upload_all = upload_all
        self._pizzerias = []
        super().__init__(db)

    def _select_pizzerias(self):
        if self._upload_all == True:
            for pizzeria in self._get_all_pizzerias_name():
                self._pizzerias.append(pizzeria)
        else:
            pizzerias_file_name = input(
                    '>> Type file name that contains pizzerias title:\n>> '
                    )
            with open(pizzerias_file_name, 'r') as file:
                for pizzeria in file:
                    self._pizzerias.append(pizzeria.strip())

    def _get_all_pizzerias_name(self):
        self._db.execute(
                """
                SELECT unit_name FROM units;
                """
                )
        return self._db.fetch()

    def _get_orders_table(self):
        # TODO: How to include right edge of range?
        self._db.execute(
                """
                SELECT * FROM orders AS o
                WHERE o.date 
                BETWEEN to_date(%s,'DD.MM.YYYY') AND to_date(%s,'DD.MM.YYYY')
                AND o.unit_name = ANY(%s)
                ;
                """,
                (self._begin_date, self._end_date, self._pizzerias)
                )
        return self._db.fetch()

    def _get_block_number(self):
        if self._upload_all == True:
            return 'All'
        elif len(self._pizzerias) > 1:
            return 'Few'
        elif len(self._pizzerias) == 1:
            self._db.execute(
                    """
                    SELECT m.customer_id FROM manager AS m
                    JOIN orders AS o
                    ON o.unit_name = %s
                    """,
                    (self._pizzerias)
                    )
            return self._db.fetch()
                     
    def _get_file_name(self):
        cur_date = datetime.date.today().strftime('%d.%m.%Y') 
        block = str(self._get_block_number()[0][0])
        upload_range = '(' + self._begin_date + ' - ' + self._end_date + ')' 
        file_name = cur_date + '_Zakaz_' + block + '_' + upload_range + '.xlsx'
        return file_name

    def upload(self):
        self._select_pizzerias()
        table = self._get_orders_table()
        df = pd.DataFrame(table, columns=[
            'id',
            'db_unit_id',
            'head_unit',
            'order_type',
            'order_number',
            'unit_name',
            'phone_number',
            'date',
            'order_sum'
            ])
        df['date'] = df['date'].dt.tz_localize(None)
        # Delete unnecessary columns.
        df = df[[
            'head_unit',
            'order_type',
            'order_number',
            'unit_name',
            'phone_number',
            'date',
            'order_sum',
            ]]
        file_name = self._get_file_name()
        # TODO: Add upload to YandexDisk.
        # TODO: Excel formating?
        df.to_excel(file_name, index=False)

