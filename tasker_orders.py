import os
import pandas as pd

import config
from storage import YandexDisk
from parser import DatabaseWorker
from postgresql import Database
from datetime import datetime


class DatabaseTaskerOrders(DatabaseWorker):
    
    
    def __init__(self, db: Database=None, begin_date: str,
            end_date: str, upload_all: bool=True):
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
                    '>>Type file name that contains pizzerias title:\n>>'
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
        self._db.execute(
                """
                SELECT * FROM orders AS o
                WHERE o.date > to_date(%s, 'DD-MM-YYYY')
                AND o.date < to_date(%s, 'DD-MM-YYYY')
                AND o.unit_name = ANY(%s)
                ;
                """,
                (self._begin_date, self._end_date, self._pizzerias)
                )
        return self._db.fetch()

