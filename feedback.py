from datetime import datetime, timezone
from typing import Union, Tuple

import pandas as pd

from parser import DatabaseWorker
from postgresql import Database
from storage import YandexDisk


class FeedbackParser:
    """
    Класс считывает таблицу с отсеянными клиентами и преобразует в датафрейм.
    """
    def __init__(self):
        """
        Инициализируем хранилище - Яндекс.Диск. Нужно для обращения к исходному файлу.
        """
        self._storage = YandexDisk()

    def parse(self, last_modified_date: Union[str, None]) -> Union[None, Tuple[datetime, pd.DataFrame]]:
        """
        Получаем файл из хранилища, считываем дату последнего изменения. Сравниваем с сохраненной датой.
        Если дата изменилась (файл обновился), считываем новый файл, записываем в датафрейм, возвращаем
        кортеж из новой даты изменения и датафрейма с данными. Если файл не изменился, возвращаем None.
        :param last_modified_date: дата последнего изменения файла из БД.
        :return:
        """
        filename = 'main_base/MainBase.xlsm'
        # получаем дату последнего изменения файла
        stop_list_modified_date = self._storage.get_modified_date(filename)
        # преобразуем входящий параметр к типу datetime, т.к. он нам поступает как строка
        try:
            # либо так
            last_modified_date = datetime.strptime(last_modified_date, '%Y-%m-%dT%H:%M:%S%z')
        except TypeError:
            # либо если не получилось, предполагаем, что нет в БД еще даты, или произошла другая ошибка, поэтому
            # будем перезаписывать заново. Для этого считаем, что выгрузили 1.1.1970 - максимально раннюю дату.
            last_modified_date = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        # сравниваем реальную дату с датой в базе. Если дата в базе None, то сравниваем с 1.1.1970, значит
        # дата обновления в любом случае позже, значит обновляем.
        if last_modified_date < stop_list_modified_date:
            # читаем и сохраняем файл
            fh = self._storage.download(filename)
            # читаем в датафрейм
            df = pd.read_excel(fh, dtype='object')
            # создаем поле stop_list, 0 - если в forbiden пусто, 1 - если в forbiden что-то есть
            df['stop_list'] = df['forbiden'].str.len() > 0
            # выкидываем лишние столбцы
            df = df[['Телефон', 'Дата завершения', 'stop_list']]
            # выкидываем строки с пустыми номерами
            df = df.dropna()
            # преобразуем даты в datetime
            df['Дата завершения'] = pd.to_datetime(df['Дата завершения'])
            # группируем по телефону и оставляем только самую позднюю дату завершения.
            df = df.sort_values('Дата завершения', ascending=False)
            df = df.groupby(['Телефон'], as_index=False).agg({'Дата завершения': 'first', 'stop_list': 'first'})
            # возвращаем новую дату и датафрейм
            return stop_list_modified_date, df
        # если файл не обновился, возвращаем None
        return None


class FeedbackStorer(DatabaseWorker):
    """
    Сохраняем результат считывания файла обзвоненных в БД в таблицу stop_list.
    """
    def __init__(self, db: Database = None):
        super().__init__(db)

    def store(self, last_modified_date: datetime, df: pd.DataFrame):
        """
        Этот метод отвечает за запись в базу датафрейма и новой даты последнего изменения
        :param last_modified_date: дата последнего изменения файла
        :param df: датафрейм
        :return: None
        """
        # сохраняем датафрейм
        params = []
        for row in df.iterrows():
            # преобразуем телефон к строке и добавляем "+" при необходимости
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

        # закрываем соединение с БД, если открывали
        self.db_close()
