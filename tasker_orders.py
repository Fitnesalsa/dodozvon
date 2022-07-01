import os
import pandas as pd

import config
from storage import YandexDisk
from parser import DatabaseWorker
from postgresql import Database
from datetime import datetime


class DatabaseTaskerOrders(DatabaseWorker):
    
    
    def __init__(self, db: Database=None, begin_date: datetime,
            end_date: datetime, upload_all: bool=True):
        self._storage = YandexDisk()
        self._begin_date = begin_date
        self._end_date = end_date
        self._upload_all = upload_all
        self._pizzerias = []
        super().__init__(db)

