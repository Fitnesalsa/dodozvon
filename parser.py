import pandas as pd

from postgresql import Database


class DatabaseWorker:
    """
    Метод подключается к базе данных
    """
    def __init__(self, db: pd.DataFrame = None):
        if not db:
            self._db = Database()
            self._db.connect()
            self._external_db = False
        else:
            self._db = db
            self._external_db = True

    """
    Метод закрывает соединения с БД
    """
    def _db_close(self):
        if not self._external_db:
            self._db.close()
