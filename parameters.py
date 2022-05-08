from datetime import datetime, timedelta

from parser import DatabaseWorker
from postgresql import Database


class ParametersGetter(DatabaseWorker):
    """
    Собирает параметры для парсера: id, unit_id, login, password,
    tz_shift, unit_name, start_date, end_date.
    """

    def __init__(self, db: Database = None):
        super().__init__(db)

    def _get_units_from_db(self) -> list:
        """
        Возвращает все активные юниты из таблицы auth
        :return: список параметров для каждого активного юнита
        """
        self._db.execute(
            """
            SELECT u.id, u.unit_id, u.unit_name, u.tz_shift, a.login, a.password, a.last_update
            FROM units u
            JOIN auth a ON u.id = a.db_unit_id
            WHERE a.is_active = true
            AND (a.last_update IS NULL OR 
                 a.last_update < date_trunc('day', now() AT TIME ZONE 'UTC'));
            """
        )
        return self._db.fetch()

    def get_parsing_params(self) -> list:
        units_to_parse = []
        for (id_, unit_id, unit_name, tz_shift, login, password, last_update) in self._get_units_from_db():
            # overall_start_date = полтора года назад datetime.now() + часовая зона - timedelta(days=548)
            # overall_end_date = сегодня
            # start_date = overall_start_date
            # end_date = overall_start_date + timedelta(days=60)
            # while start_date < overall_end_date:
            #   if end_date > overall_end_date:
            #       end_date = overall_end_date
            #   выполняем основной код
            #   start_date = end_date + timedelta(days=1)
            #   end_date = start_date + timedelta(days=60)

            # параметры start_date и end_date определяем на этом этапе;
            # отказались от первоначальной идеи вставить фильтр в базу данных, т.к. логика другая:
            # мы все равно парсим все активные юниты (is_active), и забираем их все из бд,
            # а после этого уже определяем для каждого юнита их start_date и end_date
            if last_update is None or datetime.utcnow() + timedelta(hours=tz_shift) - last_update > timedelta(days=60):
                start_date = (datetime.utcnow() + timedelta(hours=tz_shift) - timedelta(days=60)).date()
            else:
                start_date = last_update.date()
            units_to_parse.append((id_, unit_id, unit_name, login, password, tz_shift, start_date,
                                   (datetime.utcnow() + timedelta(hours=tz_shift)).date()))

        self._db_close()

        return units_to_parse
