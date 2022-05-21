import pandas as pd

from postgresql import Database


class DatabaseWorker:
    def __init__(self, db: pd.DataFrame = None):
        """
        Метод подключается к базе данных
        """
        if not db:
            self._db = Database()
            self._db.connect()
            self._external_db = False
        else:
            self._db = db
            self._external_db = True

    def _db_close(self):
        """
        Метод закрывает соединения с БД
        """
        if not self._external_db:
            self._db.close()
