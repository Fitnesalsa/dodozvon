from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Any

from config import DELTA_DAYS
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

    @staticmethod
    def _split_time_params(start_date: datetime, end_date: datetime, max_days: int = 30) -> list[tuple[datetime]]:
        """
        Генерирует серию параметров начальной и конечной даты для интервала, который больше заданного.
        Пример:
        _split_time_params(
            start_date=datetime(year=2021, month=1, day=1),
            end_date=datetime(year=2021, month=12, day=31),
            max_days=30) = [
                (datetime(year=2021, month=1, day=1), datetime(year=2021, month=1, day=30),
                (datetime(year=2021, month=1, day=31), datetime(year=2021, month=3, day=1),
                (datetime(year=2021, month=3, day=2), datetime(year=2021, month=3, day=31),
                .....
                (datetime(year=2021, month=12, day=27), datetime(year=2021, month=12, day=31)
            ]
        :param start_date: datetime, начало интервала
        :param end_date: datetime, конец интервала
        :param max_days: integer, максимальная длина интервала в днях
        :return: список кортежей с двумя датами: начало и конец субинтервала включительно.
        """
        # проверка параметров
        if start_date > end_date:
            raise AttributeError('start_date cannot be later than end_date!')
        if max_days < 1:
            raise AttributeError('max_days cannot be zero or negative!')
        result_dates = []
        interval_start_date = start_date
        interval_end_date = interval_start_date + timedelta(days=max_days)
        while True:
            if interval_end_date < end_date:
                result_dates.append((interval_start_date, interval_end_date - timedelta(days=1)))
                interval_start_date = interval_end_date
                interval_end_date = interval_start_date + timedelta(days=max_days)
            else:
                result_dates.append((interval_start_date, end_date))
                break
        return result_dates

    def get_parsing_params(self) -> list:
        units_to_parse = []
        for (id_, unit_id, unit_name, tz_shift, login, password, last_update) in self._get_units_from_db():
            # местное время пиццерии
            local_time = datetime.now(timezone.utc) + timedelta(hours=tz_shift)
            # если никогда не обновляли или обновляли больше чем полтора года назад
            if last_update is None or last_update + timedelta(days=DELTA_DAYS) < local_time:
                # обновляем за полтора года
                update_start_date = local_time - timedelta(days=DELTA_DAYS)
            # если обновляли и меньше чем полтора года назад, обновляем с этого времени
            else:
                update_start_date = last_update
            # делим интервал на куски и сохраняем в список
            for start_date, end_date in self._split_time_params(start_date=update_start_date,
                                                                end_date=local_time - timedelta(days=1)):
                units_to_parse.append((id_, unit_id, unit_name, login, password, tz_shift,
                                       start_date.date(), end_date.date()))

        self._db_close()

        return units_to_parse
