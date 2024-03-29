from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Union

from parser import DatabaseWorker
from postgresql import Database


class ParametersGetter(DatabaseWorker):
    """
    Собирает параметры для парсера: id, unit_id, login, password,
    tz_shift, unit_name, start_date, end_date.
    """

    def __init__(self, db: Database = None):
        super().__init__(db)

    def _get_units_from_db(self) -> List:
        """
        Возвращает все активные пиццерии из таблицы auth, которые не обновлялись сегодня по времени пиццерии.
        :return: список параметров для каждой активной пиццерии.
        """
        self._db.execute(
            """
            SELECT u.id, u.unit_id, u.uuid, u.unit_name, u.tz_shift, a.login, a.password, a.last_update, u.begin_date_work
            FROM units u
            JOIN auth a ON u.id = a.db_unit_id
            WHERE a.is_active = true
            AND (a.last_update IS NULL OR 
                 a.last_update < date_trunc('day', now() AT TIME ZONE 'UTC'));
            """
        )
        return self._db.fetch()

    def get_parsing_params(self) -> List[Tuple]:
        """
        Собирает параметры для передачи в Додо парсер. Возвращает список кортежей.
        :return: список параметров в кортежах.
        """
        units_to_parse = []
        for (
                id_, unit_id, uuid, unit_name, tz_shift, login, password, last_update, begin_work_date
        ) in self._get_units_from_db():
            # местное время пиццерии
            local_time = datetime.now(timezone.utc) + timedelta(hours=tz_shift)
            # конец интервала - всегда вчера
            end_date = local_time - timedelta(days=1)
            # если никогда не обновляли
            if last_update is None:
                # обновляем с начала работы пиццерии
                start_date = datetime(begin_work_date.year, begin_work_date.month, begin_work_date.day)
            # если обновляли и меньше чем полтора года назад, обновляем с этого времени
            else:
                start_date = last_update
            units_to_parse.append((id_, unit_id, uuid, unit_name, login, password, tz_shift,
                                   start_date.date(), end_date.date(), 'empty'))  # какой пиздец костыль ПЕРЕПИСАТЬ ЭТО ГОВНО
        # закрываем соединение с БД, если открывали
        self.db_close()
        return units_to_parse

    def get_config_param(self, parameter: str) -> Union[Tuple, List]:
        """
        Получаем параметр конфигурации из таблицы config по имени.
        :param parameter:
        :return: результат в виде списка или кортежа
        """
        self._db.execute("""
            SELECT value 
            FROM config
            WHERE parameter = %s;
        """, (parameter,))

        return self._db.fetch(one=True)
