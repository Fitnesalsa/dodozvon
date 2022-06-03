from datetime import datetime, timezone
from typing import Union, Tuple

import pandas as pd

from parser import DatabaseWorker
from postgresql import Database
from storage import YandexDisk


class FeedbackParser:

    def __init__(self):
        self._storage = YandexDisk()

    def parse(self, last_modified_date: Union[str, None]) -> Union[None, Tuple[datetime, pd.DataFrame]]:
        filename = 'main_base/MainBase.xlsm'
        stop_list_modified_date = self._storage.get_modified_date(filename)
        try:
            last_modified_date = datetime.strptime(last_modified_date[0], '%Y-%m-%dT%H:%M:%S%z')
        except TypeError:
            last_modified_date = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        if last_modified_date < stop_list_modified_date:
            # читаем и сохраняем файл
            fh = self._storage.download(filename)
            df = pd.read_excel(fh, dtype='object')
            df['stop_list'] = df['forbiden'].str.len() > 0
            df = df[['Телефон', 'Дата завершения', 'stop_list']]
            df['Дата завершения'] = pd.to_datetime(df['Дата завершения'])
            df = df.groupby(['Телефон'], as_index=False).agg({'Дата завершения': 'max', 'stop_list': 'max'})
            return stop_list_modified_date, df
        return None


class FeedbackStorer(DatabaseWorker):
    def __init__(self, db: Database = None):
        super().__init__(db)

    def store(self, last_modified_date: datetime, df: pd.DataFrame):
        # сохраняем df
        params = []
        for row in df.iterrows():
            phone = str(row[1]['Телефон'])
            if phone[0] != '+':
                phone = '+' + phone
            params.append((phone, row[1]['Дата завершения'], row[1]['stop_list']))
        if len(params) > 0:
            query = """
                INSERT INTO stop_list (phone, last_call_date, do_not_call) VALUES %s
                ON CONFLICT (phone) DO UPDATE 
                SET (last_call_date, do_not_call) = (EXCLUDED.last_call_date, EXCLUDED.do_not_call);
            """
            self._db.execute(query, params)

        # сохраняем дату
        self._db.execute("""
            INSERT INTO config (parameter, value) VALUES (%s, %s)
            ON CONFLICT (parameter) DO UPDATE SET value = EXCLUDED.value;
            """, ('StopListLastModifiedDate', last_modified_date.strftime('%Y-%m-%dT%H:%M:%S%z')))

        self.db_close()
