import os
import pandas as pd
import re

import config
from storage import YandexDisk
from parser import DatabaseWorker
from postgresql import Database
from datetime import datetime, date


class DatabaseTaskerOrders(DatabaseWorker):
    
    
    def __init__(self,db: Database=None):
        self._storage = YandexDisk()
        self._begin_date = ''
        self._end_date = ''
        self._upload_all = True 
        self._pizzerias = []
        super().__init__(db)

    def _set_date_range(self):
        print('>> Enter please date range (dd.mm.yyyy):')
        while True:
            self._begin_date = input('>> Begin date: ')
            self._end_date = input('>> End date: ')
            date_pattern = r'\d{2}\.\d{2}\.\d{4}'

            if re.search(date_pattern, self._begin_date):
                print(
                        '>> Error! Wrong begin date format.'
                        ' Try again please.'
                        )
            elif re.search(date_pattern, self._end_date):
                print(
                        '>> Error! Wrong end date format.'
                        ' Try again please.'
                        )
            elif datetime.strptime(self._begin_date, '%d.%m.%Y') > \
            datetime.strptime(self._end_date, '%d.%m.%Y'):
                print(
                        '>> Error! Begin date is later then end date.'
                        'Try again please.'
                        )
            # TODO: Dates beyond available date range from database.
            else:
                break

    def _select_pizzerias(self):
        while True:
            self._upload_all = input(
                    '>> Upload data for all available pizzerias(y/n)?:\n>> '
                    )
            if self._upload_all in ('y', 'Y', 'n', 'N'):
                break
            else:
                print('>> Error! Wrong answer! Try again please.')

        if self._upload_all.lower() == 'y':
            self._upload_all = True
        else:
            self._upload_all = False

        if self._upload_all == True:
            for pizzeria in self._get_all_pizzerias_name():
                self._pizzerias.append(pizzeria)
        else:
            while True:
                pizzerias_file_name = input(
                        '>> Type filename that contains pizzerias title:\n>> '
                        )
                if not re.search(r'.*\.txt', pizzerias_file_name):
                    print(
                        '>> Error! File should be txt format.' 
                        'Try again please.'
                        )
                elif not os.path.exists(pizzerias_file_name):
                    print(
                        ">> Error! File dosn't exist."
                        " Please check spelling and try again."
                        )
                else:
                    break

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
        self._db.execute(
                """
                SELECT * FROM orders AS o
                WHERE CAST(o.date AS date)
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
            return str(self._db.fetch()[0][0])
                     
    def _get_file_name(self):
        cur_date = date.today().strftime('%d.%m.%Y') 
        block = self._get_block_number()
        upload_range = '(' + self._begin_date + ' - ' + self._end_date + ')' 
        file_name = cur_date + '_Zakaz_' + block + '_' + upload_range + '.xlsx'
        return file_name

    def upload(self):
        self._set_date_range()
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
        # TODO: Excel formating?
        df.to_excel(file_name, index=False)
        #self._storage.upload(file_name, config.YANDEX_ORDERS_FOLDER)
        #os.remove(file_name)

