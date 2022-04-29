import os
from datetime import datetime, timedelta

import pandas as pd
import requests

from bot import Bot
from config import YANDEX_API_TOKEN
from parser import DatabaseWorker
from postgresql import Database


class DatabaseTasker(DatabaseWorker):
    # разделяем таблицы по: блоку, часовому поясу
    def __init__(self, db: Database = None):
        super().__init__(db)

    def _get_query_pairs(self):
        self._db.execute("""
            SELECT m.customer_id, u.tz_shift
            FROM units u
            JOIN manager m on u.country_code = m.country_code and u.unit_id = m.unit_id
            GROUP BY m.customer_id, u.tz_shift;
        """)

        return self._db.fetch()

    @staticmethod
    def _yandex_upload(filename: str):
        upload_request_url = 'https://cloud-api.yandex.net/v1/disk/resources/upload'
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json',
                   'Authorization': f'OAuth {YANDEX_API_TOKEN}'}
        upload_response = requests.get(f'{upload_request_url}?path=/call_new_clients/{filename}%overwrite=false',
                                       headers=headers).json()
        with open(filename, 'rb') as f:
            try:
                requests.put(upload_response['href'], files={'file': f})
            except KeyError:
                bot = Bot()
                bot.send_message(f'Ошибка при загрузке на диск. Файл: {filename}, ответ: {upload_response}')

    def create_tables(self):
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
                JOIN manager m ON c.unit_id = m.unit_id AND c.country_code = m.country_code
                JOIN units u ON c.unit_id = u.unit_id AND c.country_code = u.country_code
                WHERE m.customer_id = %s 
                    AND u.tz_shift = %s
                    AND c.first_order_datetime >= date_trunc(
                        'day', now() AT TIME ZONE 'UTC' + interval '1 hour' * u.tz_shift - interval '8 days')
                    AND c.first_order_datetime < date_trunc(
                        'day', now() AT TIME ZONE 'UTC' + interval '1 hour' * u.tz_shift)
            )
            SELECT * FROM pair_table
            WHERE source IS NOT NULL
                AND promocode IS NOT NULL;
            """, (customer_id, tz_shift))

            table = self._db.fetch()

            df = pd.DataFrame(table, columns=[
                'phone', 'first_order_type', 'source', 'promocode', 'city', 'shop', 'first_order_city',
                'first_order_datetime'
            ])

            df = df[['phone', 'promocode', 'city', 'shop', 'first_order_city', 'first_order_datetime', 'source']]

            # generate filename
            source_str = ''
            if 0 in df['first_order_type']:
                source_str += 'D_'
            if 1 in df['first_order_type'] or 2 in df['first_order_type']:
                source_str += 'R+SV_'
            filename = f'{datetime.now() + timedelta(hours=3):%d.%m.%Y}_NK_{source_str}Blok-{customer_id}_' \
                       f'{datetime.now() + timedelta(hours=tz_shift) - timedelta(days=8):%d.%m.%Y}-' \
                       f'{datetime.now() + timedelta(hours=tz_shift) - timedelta(days=1):%d.%m.%Y}_tz-' \
                       f'{tz_shift - 3}.xlsx'

            # save file, upload to Yandex Disk and delete
            df.to_excel(filename)
            self._yandex_upload(filename)
            os.remove(filename)
