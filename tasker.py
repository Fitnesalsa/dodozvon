import os
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Union

import pandas as pd
from dateutil.relativedelta import relativedelta

import config
from dodois import DodoEmptyExcelError
from storage import YandexDisk
from config import YANDEX_NEW_CLIENTS_FOLDER, YANDEX_LOST_CLIENTS_FOLDER, YANDEX_NEW_PROMO_FOLDER, \
    YANDEX_LOST_PROMO_FOLDER, YANDEX_ORDERS_FOLDER
from parser import DatabaseWorker
from postgresql import Database


class DatabaseTasker(DatabaseWorker):
    """
    Класс выгружает отчеты из БД.
    """
    def __init__(self, db: Database = None):
        # инициализируем хранилище для записи отчетов
        self._storage = YandexDisk()
        super().__init__(db)

    def _get_new_params(self) -> Union[List, Tuple]:
        """
        Получает параметры для формирования отчета о новых клиентах.
        :return: список или кортеж
        """
        self._db.execute("""
            SELECT m.customer_id, u.tz_shift
            FROM units u
            JOIN manager m on m.db_unit_id = u.id
            JOIN auth a on u.id = a.db_unit_id
            WHERE a.is_active = true
            AND m.new_shop_exclude = false
            GROUP BY m.customer_id, u.tz_shift;
        """)
        return self._db.fetch()

    def _get_lost_params(self) -> Union[List, Tuple]:
        """
        Получает параметры для формирования отчета о пропавших клиентах.
        :return: список или кортеж
        """
        self._db.execute("""
            SELECT m.customer_id, u.tz_shift, u.id, m.lost_start_date, m.lost_shift_months
            FROM units u
            JOIN manager m on m.db_unit_id = u.id
            JOIN auth a on u.id = a.db_unit_id
            WHERE a.is_active = true
            AND m.lost_shop_exclude = false
            GROUP BY m.customer_id, u.tz_shift, u.id, m.lost_start_date, m.lost_shift_months
            ORDER BY m.customer_id, u.tz_shift;
        """)
        return self._db.fetch()

    def create_new_clients_tables(self):
        """
        Формирует отчет о новых клиентах.
        :return: None
        """
        pairs = self._get_new_params()
        # выгрузка раздельно по каждому набору параметров
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
                LEFT JOIN stop_list sl ON c.phone = sl.phone
                WHERE m.customer_id = %s 
                    AND u.tz_shift = %s
                    AND c.first_order_city = u.unit_name
                    AND c.last_order_city = u.unit_name
                    AND m.new_shop_exclude = false
                    AND c.first_order_datetime + interval '1 hour' * u.tz_shift >= 
                        coalesce(
                            m.new_start_date,
                            date_trunc('day', now() AT TIME ZONE 'UTC' + interval '1 hour' * 
                                                                         u.tz_shift - interval '7 days')
                        )
                    AND c.first_order_datetime + interval '1 hour' * u.tz_shift < date_trunc(
                        'day', now() AT TIME ZONE 'UTC' + interval '1 hour' * u.tz_shift)
                    AND (sl.last_call_date IS NULL
                         OR now() AT TIME ZONE 'UTC' + interval '1 hour' * u.tz_shift - sl.last_call_date > 
                            interval '180 days')
                    AND (sl.do_not_call IS NULL OR NOT sl.do_not_call)
            )
            SELECT * FROM pair_table
            WHERE source IS NOT NULL
                AND length(source) > 0
                AND promocode IS NOT NULL
                AND length(promocode) > 0;
            """, (customer_id, tz_shift))

            table = self._db.fetch()

            df = pd.DataFrame(table, columns=[
                'phone', 'first_order_type', 'source', 'promokod', 'city', 'pizzeria', 'otdel',
                'first-order'
            ])

            # преобразуем first-order в правильную таймзону, чтобы в итоговом файле были правильные даты
            df['first-order'] = df['first-order'].dt.tz_convert(config.TIMEZONES[tz_shift]).dt.tz_localize(None)

            # генерируем имя файла
            source_str = ''
            if 0 in df['first_order_type']:
                source_str += 'D_'
            if 1 in df['first_order_type'] or 2 in df['first_order_type']:
                source_str += 'R+SV_'

            filename = f'{datetime.now(timezone.utc) + timedelta(hours=3):%d.%m.%Y}_NK_{source_str}Blok-{customer_id}_' \
                       f'{datetime.now(timezone.utc) + timedelta(hours=tz_shift) - timedelta(days=7):%d.%m.%Y}-' \
                       f'{datetime.now(timezone.utc) + timedelta(hours=tz_shift) - timedelta(days=1):%d.%m.%Y}_tz-' \
                       f'{tz_shift - 3}.xlsx'

            # удаляем лишние поля
            df = df[['phone', 'promokod', 'city', 'pizzeria', 'otdel', 'first-order', 'source']]

            # сохраняем файл локально, загружаем в хранилище и удаляем локально.
            df.to_excel(filename, index=False)
            self._storage.upload(filename, YANDEX_NEW_CLIENTS_FOLDER)
            os.remove(filename)

    def create_lost_clients_tables(self):
        """
        Формирует отчет о пропавших клиентах.
        Логика формирования отчета прописана в ТЗ:
        https://docs.google.com/document/d/1XxGJ_m0LIqEudvVknUMsI0lnXCy1IjfQCrDZUEfblxU/
        :return: None
        """
        dfs = []
        param_cursor = []  # хранит customer_id, tz_shift в кортеже
        for customer_id, tz_shift, unit_id, lost_start_date, lost_shift_months in self._get_lost_params():
            lost_end_date = lost_start_date + relativedelta(months=1)
            local_time = datetime.now(timezone.utc) + timedelta(hours=tz_shift)
            shift_duration = relativedelta(months=lost_shift_months)
            shift_start = local_time - shift_duration - timedelta(days=8)
            shift_end = local_time - shift_duration - timedelta(days=1)
            if lost_end_date < shift_start.date():
                report_start_date = lost_start_date
                report_end_date = lost_end_date
                lost_start_date = lost_end_date
            else:
                report_start_date = lost_start_date
                report_end_date = shift_end.date()
                lost_start_date = shift_end
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
            LEFT JOIN stop_list sl on c.phone = sl.phone
            WHERE m.customer_id = %s 
                AND u.tz_shift = %s
                AND u.id = %s
                AND c.orders_sum > 0
                AND c.orders_amt > 1 
                AND c.last_order_city = u.unit_name
                AND m.lost_shop_exclude = false
                AND c.last_order_datetime + interval '1 hour' * u.tz_shift >= %s
                AND c.last_order_datetime + interval '1 hour' * u.tz_shift < %s
                AND (sl.last_call_date IS NULL
                     OR now() AT TIME ZONE 'UTC' + interval '1 hour' * u.tz_shift - sl.last_call_date > 
                        interval '180 days')
                AND (sl.do_not_call IS NULL OR NOT sl.do_not_call);
            """, (customer_id, tz_shift, unit_id, report_start_date, report_end_date))

            table = self._db.fetch()

            df = pd.DataFrame(table, columns=[
                'phone', 'promokod', 'city', 'pizzeria', 'otdel', 'last-order', 'source'
            ])

            if len(df) > 0:

                # преобразовываем first-order в правильную таймзону, чтобы в итоговом файле были правильные даты
                df['last-order'] = df['last-order'].dt.tz_convert(config.TIMEZONES[tz_shift]).dt.tz_localize(None)

                dfs.append(df)
                param_cursor.append((customer_id, tz_shift, report_start_date, report_end_date))

                self._db.execute("""
                    UPDATE manager
                    SET lost_start_date = %s
                    WHERE db_unit_id = %s; 
                    """, (lost_start_date, unit_id))

        if len(dfs) > 0:
            # сохранение: обратная разбивка по customer_id, tz_shift
            prev_new_idx = 0
            for idx, params in enumerate(param_cursor):
                # начался новый файл, записываем старый
                if idx == len(param_cursor) - 1 or params != param_cursor[idx + 1]:
                    df = pd.concat(dfs[prev_new_idx:idx + 1])
                    customer_id, tz_shift, _, _ = params
                    start_date = min([row[2] for row in param_cursor[prev_new_idx:idx + 1]])
                    end_date = max([row[3] for row in param_cursor[prev_new_idx:idx + 1]])
                    filename = f'{datetime.now(timezone.utc) + timedelta(hours=3):%d.%m.%Y}_PROPAL_Blok-{customer_id}_'\
                               f'{start_date:%d.%m.%Y}-{end_date:%d.%m.%Y}_tz-{tz_shift - 3}.xlsx'
                    prev_new_idx = idx + 1
                    # save file, upload to Yandex Disk and delete
                    df.to_excel(filename, index=False)
                    self._storage.upload(filename, YANDEX_LOST_CLIENTS_FOLDER)
                    os.remove(filename)

    def get_new_promo_params(self):
        self._db.execute("""
            SELECT 
                u.id,
                m.customer_id,
                u.unit_id, 
                u.uuid,
                u.unit_name, 
                a.login, 
                a.password, 
                u.tz_shift,
                m.custom_start_date, 
                m.custom_end_date,
                m.new_clients_promos_all
            FROM units u
            JOIN auth a
                ON u.id = a.db_unit_id
            JOIN manager m
                ON u.id = m.db_unit_id
            WHERE a.is_active = true AND
                m.new_shop_exclude = false AND
                m.custom_start_date IS NOT NULL AND
                m.custom_end_date IS NOT NULL AND
                m.new_clients_promos_all IS NOT NULL;
        """)
        return self._db.fetch()

    def get_lost_promo_params(self):
        self._db.execute("""
            SELECT 
                u.id,
                m.customer_id,
                u.unit_id,
                u.uuid,
                u.unit_name, 
                a.login, 
                a.password, 
                u.tz_shift,
                m.custom_start_date, 
                m.custom_end_date,
                m.lost_clients_promos_all
            FROM units u
            JOIN auth a
                ON u.id = a.db_unit_id
            JOIN manager m
                ON u.id = m.db_unit_id
            WHERE a.is_active = true AND
                m.lost_shop_exclude = false AND
                m.custom_start_date IS NOT NULL AND
                m.custom_end_date IS NOT NULL AND
                m.lost_clients_promos_all IS NOT NULL;
        """)
        return self._db.fetch()

    def create_promo_tables(self, df: pd.DataFrame, customer_id: int, shop_name: str,
                            start_date: datetime, end_date: datetime,
                            suffix: str):
        filename = f'Расход промо-кодов_{customer_id}_{shop_name}_{suffix}_({start_date:%Y-%m-%d} - {end_date:%Y-%m-%d}).xlsx'
        df.to_excel(filename, index=False)
        if suffix == 'НК':
            folder = YANDEX_NEW_PROMO_FOLDER
        elif suffix == 'ПК':
            folder = YANDEX_LOST_PROMO_FOLDER
        else:
            raise ValueError('wrong suffix!')
        self._storage.upload(filename, folder)
        os.remove(filename)

    def _get_orders_params(self):
        self._db.execute("""
            SELECT
                u.id,
                u.unit_name,
                u.tz_shift,
                m.customer_id,
                m.custom_start_date,
                m.custom_end_date
            FROM units u
            JOIN manager m ON u.id = m.db_unit_id
            JOIN auth a ON u.id = a.db_unit_id
            WHERE a.is_active = true
                AND m.custom_start_date IS NOT NULL
                AND m.custom_end_date IS NOT NULL;
        """)
        return self._db.fetch()

    def create_orders_tables(self):
        for db_unit_id, shop_name, tz_shift, customer_id, start_date, end_date in self._get_orders_params():
            print(f'parsing orders for {shop_name}...')
            start_date_full = datetime(start_date.year, start_date.month, start_date.day, 0, 0) - timedelta(hours=tz_shift)
            end_date_full = datetime(end_date.year, end_date.month, end_date.day, 0) - timedelta(hours=tz_shift)
            self._db.execute("""
                SELECT o.*, u.unit_name FROM orders o
                JOIN units u ON u.id = o.db_unit_id
                WHERE o.db_unit_id = %s
                    AND o.date >= %s
                    AND o.date < %s;
            """, (db_unit_id, start_date_full, (end_date_full + timedelta(days=1))))

            df = pd.DataFrame(self._db.fetch(), columns = [
                'id', 'db_unit_id', 'Дата', '№ заказа', 'Тип заказа', 'Номер телефона', 'Сумма заказа',
                'Статус заказа', 'Отдел'])

            if len(df) == 0:
                raise DodoEmptyExcelError(f'Выгружен пустой файл Excel для пиццерии {shop_name}. Возможно,'
                                          f' на сервере нет заказов от этой пиццерии.')
            else:
                # восстановление полей таблицы
                df['Подразделение'] = df['Отдел'].str.extract(r'(.+)(?=-)')
                df['Дата'] = df['Дата'].dt.tz_convert(config.TIMEZONES[tz_shift]).dt.tz_localize(None)
                df['Время'] = df['Дата']
                df['Время продажи (печати чека)'] = 0
                df['Тип заказа'].replace(to_replace={0: 'Доставка', 1: 'Самовывоз', 2: 'Ресторан'}, inplace=True)
                df['Имя клиента'] = '**********'
                df['Способ оплаты'] = 0
                df['Статус заказа'].replace(to_replace={0: 'Доставка', 1: 'Отказ', 2: 'Просрочен', 3: 'Упакован',
                                                        4: 'В работе', 5: 'Принят', 6: 'Выполнен'}, inplace=True)
                df['Оператор заказа'] = 0
                df['Курьер'] = 0
                df['Причина просрочки'] = 0
                df['Адрес'] = '**********'
                df['id заказа'] = 0
                df['id транзакции'] = 0

                df = df[['Подразделение', 'Отдел', 'Дата', 'Время', 'Время продажи (печати чека)', '№ заказа', 'Тип заказа',
                         'Имя клиента', 'Номер телефона', 'Сумма заказа', 'Способ оплаты', 'Статус заказа',
                         'Оператор заказа', 'Курьер', 'Причина просрочки', 'Адрес', 'id заказа', 'id транзакции']]

                filename = f'Заказы_{customer_id}_{shop_name}_({start_date:%Y-%m-%d} - {end_date:%Y-%m-%d}).xlsx'
                df.to_excel(filename, index=False)
                self._storage.upload(filename, YANDEX_ORDERS_FOLDER)
                os.remove(filename)
                print(f'orders for {shop_name} uploaded successfully!')
