import os
from datetime import datetime, timedelta

import pandas as pd
import requests

import config
from config import YANDEX_API_TOKEN, YANDEX_NEW_CLIENTS_FOLDER
from parser import DatabaseWorker
from postgresql import Database


class YandexUploadError(Exception):
    def __init__(self, filename: str, upload_response: dict):
        self.message = f'Ошибка при загрузке на диск. Файл: {filename}, ответ: {upload_response}'
        super().__init__(self.message)


class YandexCreateFolderError(Exception):
    def __init__(self, filename: str, upload_response: dict):
        self.message = f'Ошибка при создании папки {YANDEX_NEW_CLIENTS_FOLDER}, ответ: {upload_response}'
        super().__init__(self.message)


class DatabaseTasker(DatabaseWorker):
    # разделяем таблицы по: блоку, часовому поясу
    def __init__(self, db: Database = None):
        super().__init__(db)

    def _get_query_pairs(self):
        self._db.execute("""
            SELECT m.customer_id, u.tz_shift
            FROM units u
            JOIN manager m on m.db_unit_id = u.id
            JOIN auth a on u.id = a.db_unit_id
            WHERE a.is_active = true
            GROUP BY m.customer_id, u.tz_shift;
        """)

        return self._db.fetch()

    @staticmethod
    def _yandex_upload(filename: str):
        request_url = 'https://cloud-api.yandex.net/v1/disk/resources'
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json',
                   'Authorization': f'OAuth {YANDEX_API_TOKEN}'}

        # check if folder exists
        check_folder_response = requests.get(f'{request_url}?path=%2F{YANDEX_NEW_CLIENTS_FOLDER}',
                                             headers=headers).json()
        if check_folder_response.get('error') == 'DiskNotFoundError':
            # create folder
            put_folder_response = requests.put(f'{request_url}?path=%2F{YANDEX_NEW_CLIENTS_FOLDER}',
                                               headers=headers).json()
            if 'error' in put_folder_response.keys():
                raise YandexCreateFolderError

        # upload file
        upload_response = requests.get(f'{request_url}/upload?path=%2F{YANDEX_NEW_CLIENTS_FOLDER}%2F{filename}'
                                       f'&overwrite=true', headers=headers).json()
        with open(filename, 'rb') as f:
            try:
                requests.put(upload_response['href'], files={'file': f})
            except KeyError:
                raise YandexUploadError(filename, upload_response)

    def create_new_clients_tables(self):
        pairs = self._get_query_pairs()
        for customer_id, tz_shift in pairs:
            self._db.execute("""
            WITH pair_table AS (
                SELECT 
                    c.phone,
                    c.first_order_type,
                    (case 
                        when c.first_order_type = 0 then m.source_deliv
                        when c.first_order_type = 1 then m.source_pickup
                        when c.first_order_type = 2 then m.source_rest
                    end) as source,
                    (case 
                        when c.first_order_type = 0 then m.promo_deliv
                        when c.first_order_type = 1 then m.promo_pickup
                        when c.first_order_type = 2 then m.promo_rest
                    end) as promocode,
                    m.city,
                    m.pizzeria,
                    c.first_order_city,
                    c.first_order_datetime
                FROM clients c
                JOIN units u ON c.db_unit_id = u.id
                JOIN manager m ON m.db_unit_id = u.id
                WHERE m.customer_id = %s 
                    AND u.tz_shift = %s
                    AND c.first_order_datetime + interval '1 hour' * u.tz_shift >= date_trunc(
                        'day', now() AT TIME ZONE 'UTC' + interval '1 hour' * u.tz_shift - interval '8 days')
                    AND c.first_order_datetime + interval '1 hour' * u.tz_shift < date_trunc(
                        'day', now() AT TIME ZONE 'UTC' + interval '1 hour' * u.tz_shift)
            )
            SELECT * FROM pair_table
            WHERE source IS NOT NULL
                AND promocode IS NOT NULL;
            """, (customer_id, tz_shift))

            table = self._db.fetch()

            df = pd.DataFrame(table, columns=[
                'phone', 'first_order_type', 'source', 'promokod', 'city', 'pizzeria', 'otdel',
                'first-order'
            ])

            # преобразовываем first-order в правильную таймзону, чтобы в итоговом файле были правильные даты
            df['first-order'] = df['first-order'].dt.tz_convert(config.TIMEZONES[tz_shift]).dt.tz_localize(None)

            # generate filename
            source_str = ''
            if 0 in df['first_order_type']:
                source_str += 'D_'
            if 1 in df['first_order_type'] or 2 in df['first_order_type']:
                source_str += 'R+SV_'

            # drop extra columns
            df = df[['phone', 'promokod', 'city', 'pizzeria', 'otdel', 'first-order', 'source']]

            filename = f'{datetime.utcnow() + timedelta(hours=3):%d.%m.%Y}_NK_{source_str}Blok-{customer_id}_' \
                       f'{datetime.utcnow() + timedelta(hours=tz_shift) - timedelta(days=8):%d.%m.%Y}-' \
                       f'{datetime.utcnow() + timedelta(hours=tz_shift) - timedelta(days=1):%d.%m.%Y}_tz-' \
                       f'{tz_shift - 3}.xlsx'

            # save file, upload to Yandex Disk and delete
            df.to_excel(filename, index=False)
            self._yandex_upload(filename)
            os.remove(filename)

    def create_lost_clients_tables(self):
        pass
