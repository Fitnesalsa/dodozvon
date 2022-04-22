from postgresql import Database


class ParametersGetter:
    """
    Собирает параметры для парсера: country_code, unit_id, login, password,
    tz_shift, unit_name, start_date, end_date.
    """
    def __init__(self):
        self._db = Database()
        self._db.connect()

    def get_from_db(self):
        self._db.execute(
            """
            SELECT u.unit_id, u.unit_name, a.login, a.password, a.last_update
            FROM units u
            JOIN auth a ON u.unit_name = a.unit_name
            WHERE a.is_active = true 
            AND (now() AT TIME ZONE 'UTC' + interval '1 hour' * u.tz_shift - a.last_update > interval '60 days'
                OR a.last_update IS NULL)
            """
        )
        units = self._db.fetch()
