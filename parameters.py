from datetime import datetime, timedelta

from postgresql import Database


class ParametersGetter:
    """
    Собирает параметры для парсера: country_code, unit_id, login, password,
    tz_shift, unit_name, start_date, end_date.
    """

    def __init__(self, db: Database = None):
        if not db:
            self._db = Database()
            self._db.connect()
            self._external_db = False
        else:
            self._db = db
            self._external_db = True

    def _get_units_from_db(self) -> list:
        """
        Возвращает все активные юниты из таблицы auth
        :return: список параметров для каждого активного юнита
        """
        self._db.execute(
            """
            SELECT u.unit_id, u.unit_name, u.tz_shift, a.login, a.password, a.last_update
            FROM units u
            JOIN auth a ON u.unit_name = a.unit_name
            WHERE a.is_active = true
            AND (a.last_update IS NULL OR 
                 a.last_update < date_trunc('day', now() AT TIME ZONE 'UTC'));
            """
        )
        return self._db.fetch()

    def get_parsing_params(self) -> list:
        units_to_parse = []
        for unit in self._get_units_from_db():
            # параметры start_date и end_date определяем на этом этапе;
            # отказались от первоначальной идеи вставить фильтр в базу данных, т.к. логика другая:
            # мы все равно парсим все активные юниты (is_active), и забираем их все из бд,
            # а после этого уже определяем для каждого юнита их start_date и end_date
            if unit[5] is None or datetime.now() + timedelta(hours=unit[2]) - unit[5] > timedelta(days=60):
                start_date = datetime.now() + timedelta(hours=unit[2]) - timedelta(days=60)
            else:
                start_date = unit[5] + timedelta(days=1)
            units_to_parse.append((unit[0], unit[1], unit[3], unit[4],
                                   start_date, datetime.now()))

        if not self._external_db:
            self._db.close()

        return units_to_parse
