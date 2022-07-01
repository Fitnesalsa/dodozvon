import os
import pandas as pd

import config
from storage import YandexDisk
from parser import DatabaseWorker
from postgresql import Database


class DatabaseTaskerOrders(DatabaseWorker):
    def __init__(self, db: Database=None):
        self._storage = YandexDisk()
        super().__init__(db)

