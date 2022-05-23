from datetime import datetime, timezone
from typing import Union

import pandas as pd

from parser import DatabaseWorker
from postgresql import Database
from storage import YandexDisk


class FeedbackParser:

    def __init__(self):
        self._storage = YandexDisk()

    def parse(self, last_modified_date: Union[str, None]) -> Union[None, tuple[datetime, pd.DataFrame]]:
        filename = 'main_base/MainBase.xlsm'
        stop_list_modified_date = self._storage.get_modified_date(filename)
        try:
            last_modified_date = datetime.strptime(last_modified_date, '%Y-%m-%dT%H:%M:%S%z')
        except TypeError:
            last_modified_date = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        if last_modified_date < stop_list_modified_date:
            # читаем и сохраняем файл
            fh = self._storage.download(filename)
            df = pd.read_excel(fh)
            df['stop_list'] = df['forbiden'].str.len() > 0
            df = df[['Телефон', 'Дата завершения', 'stop_list']]
            df = df.groupby(['Телефон'], as_index=False).agg({'Дата завершения': 'max', 'stop_list': 'max'})
            return last_modified_date, df
        return None


class FeedbackStorer(DatabaseWorker):
    def __init__(self, db: Database = None):
        super().__init__(db)

    def store(self, last_modified_date: datetime, df: pd.DataFrame):
        # сохраняем df
        params = []
        for row in df.iterrows():
            params.append((row[1]['Телефон'], row[1]['Дата завершения'], row[1]['stop_list']))
        query = """
            INSERT INTO stop_list (phone, last_call_date, do_not_call) VALUES %s
            ON CONFLICT (phone) DO UPDATE 
            SET (last_call_date, do_not_call) = (EXCLUDED.last_call_date, EXCLUDED.do_not_call);
        """
        self._db.execute(query, params)

        # сохраняем дату
        self._db.execute('UPDATE config SET value = %s WHERE parameter = "StopListLastModifiedDate";',
                         (last_modified_date,))
