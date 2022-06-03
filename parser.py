from postgresql import Database


class DatabaseWorker:
    def __init__(self, db: Database = None):
        """
        Метод для подключения к базе данных.
        Если передается параметр db, то класс ожидает объект Database и не выполняет соединение,
        а предполагает, что оно уже установлено.
        В противном случае открывает новое соединение с БД (postgresql.Database).
        """
        if not db:
            self._db = Database()
            self._db.connect()
            self._external_db = False
        else:
            self._db = db
            self._external_db = True

    def db_close(self):
        """
        Метод закрывает соединение с БД, если соединение устанавливалось при инициализации.
        :return: None
        """
        if not self._external_db:
            self._db.close()
