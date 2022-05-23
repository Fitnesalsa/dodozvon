import os
from datetime import datetime, timedelta, timezone

import pandas as pd

import config
from storage import YandexDisk
from config import YANDEX_NEW_CLIENTS_FOLDER, YANDEX_LOST_CLIENTS_FOLDER
from parser import DatabaseWorker
from postgresql import Database


class DatabaseTasker(DatabaseWorker):
    def __init__(self, db: Database = None):
        self._storage = YandexDisk()
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

    def _get_units_by_pair(self, customer_id: int, tz_shift: int):
        self._db.execute("""
            SELECT u.id
            FROM units u
            JOIN manager m on m.db_unit_id = u.id
            JOIN auth a on u.id = a.db_unit_id
            WHERE a.is_active = true
            AND m.customer_id = %s
            AND u.tz_shift = %s;
        """, (customer_id, tz_shift))
        return self._db.fetch()

    def create_new_clients_tables(self):
        pairs = self._get_query_pairs()
        for customer_id, tz_shift in pairs:
            self._db.execute("""
            WITH pair_table AS (
                SELECT 
                    c.phone,
                    c.first_order_type,
                    (case 
                        when c.first_order_type = 0 then m.new_source_deliv
                        when c.first_order_type = 1 then m.new_source_pickup
                        when c.first_order_type = 2 then m.new_source_rest
                    end) as source,
                    (case 
                        when c.first_order_type = 0 then m.new_promo_deliv
                        when c.first_order_type = 1 then m.new_promo_pickup
                        when c.first_order_type = 2 then m.new_promo_rest
                    end) as promocode,
                    m.new_city,
                    m.pizzeria,
                    c.first_order_city,
                    c.first_order_datetime
                FROM clients c
                JOIN units u ON c.db_unit_id = u.id
                JOIN manager m ON m.db_unit_id = u.id
                WHERE m.customer_id = %s 
                    AND u.tz_shift = %s
                    AND c.first_order_city = u.unit_name
                    AND c.last_order_city = u.unit_name
                    AND c.first_order_datetime + interval '1 hour' * u.tz_shift >= 
                        coalesce(
                            m.new_start_date,
                            date_trunc('day', now() AT TIME ZONE 'UTC' + interval '1 hour' * 
                                                                         u.tz_shift - interval '6 days')
                        )
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

            filename = f'{datetime.now(timezone.utc) + timedelta(hours=3):%d.%m.%Y}_NK_{source_str}Blok-{customer_id}_' \
                       f'{datetime.now(timezone.utc) + timedelta(hours=tz_shift) - timedelta(days=7):%d.%m.%Y}-' \
                       f'{datetime.now(timezone.utc) + timedelta(hours=tz_shift) - timedelta(days=1):%d.%m.%Y}_tz-' \
                       f'{tz_shift - 3}.xlsx'

            # save file, upload to Yandex Disk and delete
            df.to_excel(filename, index=False)
            self._storage.upload(filename, YANDEX_NEW_CLIENTS_FOLDER)
            os.remove(filename)

    def create_lost_clients_tables(self):
        pairs = self._get_query_pairs()
        for customer_id, tz_shift in pairs:
            units = self._get_units_by_pair(customer_id, tz_shift)
            dfs = []
            for unit_id, in units:
                self._db.execute("""
                    SELECT lost_start_date, lost_shift_months
                    FROM manager WHERE db_unit_id = %s;
                    """, (unit_id,))
                unit_data = self._db.fetch(one=True)
                lost_start_date, lost_shift_months = unit_data
                lost_end_date = lost_start_date + timedelta(days=config.LOST_DURATION)
                local_time = datetime.now(timezone.utc) + timedelta(hours=tz_shift)
                shift_duration = timedelta(days=(lost_shift_months * config.LOST_DURATION))
                shift_start = local_time - shift_duration - timedelta(days=8)
                shift_end = local_time - shift_duration - timedelta(days=1)
                if lost_end_date < shift_start.date():
                    report_start_date = lost_start_date
                    report_end_date = lost_end_date
                    lost_start_date = lost_end_date + timedelta(days=1)
                else:
                    if lost_start_date < shift_start.date():
                        report_start_date = lost_start_date
                    else:
                        report_start_date = shift_start
                    report_end_date = shift_end
                    lost_start_date = shift_end + timedelta(days=1)
                self._db.execute("""
                SELECT 
                    c.phone,
                    m.lost_promo,
                    m.lost_city,
                    m.pizzeria,
                    c.last_order_city,
                    c.last_order_datetime,
                    m.lost_source
                FROM clients c
                JOIN units u ON c.db_unit_id = u.id
                JOIN manager m ON m.db_unit_id = u.id
                WHERE m.customer_id = %s 
                    AND u.tz_shift = %s
                    AND u.id = %s
                    AND c.orders_sum > 0
                    AND c.orders_amt > 2
                    AND c.first_order_city = u.unit_name
                    AND c.last_order_city = u.unit_name
                    AND c.last_order_datetime + interval '1 hour' * u.tz_shift >= %s
                    AND c.first_order_datetime + interval '1 hour' * u.tz_shift < %s;
                """, (customer_id, tz_shift, unit_id, report_start_date, report_end_date))

                table = self._db.fetch()

                df = pd.DataFrame(table, columns=[
                    'phone', 'promokod', 'city', 'pizzeria', 'otdel', 'last-order', 'source'
                ])

                # преобразовываем first-order в правильную таймзону, чтобы в итоговом файле были правильные даты
                df['last-order'] = df['last-order'].dt.tz_convert(config.TIMEZONES[tz_shift]).dt.tz_localize(None)

                dfs.append(df)

                self._db.execute("""
                    UPDATE manager
                    SET lost_start_date = %s
                    WHERE db_unit_id = %s; 
                    """, (lost_start_date, unit_id))

            df = pd.concat(dfs)
            filename = f'{datetime.now(timezone.utc) + timedelta(hours=3):%d.%m.%Y}_PROPAL_Blok-{customer_id}_' \
                       f'{datetime.now(timezone.utc) + timedelta(hours=tz_shift) - timedelta(days=7):%d.%m.%Y}-' \
                       f'{datetime.now(timezone.utc) + timedelta(hours=tz_shift) - timedelta(days=1):%d.%m.%Y}_tz-' \
                       f'{tz_shift - 3}.xlsx'

            # save file, upload to Yandex Disk and delete
            df.to_excel(filename, index=False)
            self._storage.upload(filename, YANDEX_LOST_CLIENTS_FOLDER)
            os.remove(filename)
