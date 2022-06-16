from typing import Union, List, Tuple

import psycopg2
from psycopg2.extras import execute_values
import config


class Database:

    def __init__(self):
        self._database = config.PG_DATABASE
        self._user = config.PG_USER
        self._pass = config.PG_PASS
        self._host = config.PG_HOST
        self._port = config.PG_PORT
        self._conn = None
        self._cur = None

    def connect(self):
        """
        Connect to an existing database
        """
        self._conn = psycopg2.connect(dbname=self._database,
                                      user=self._user,
                                      password=self._pass,
                                      host=self._host,
                                      port=self._port)

        # Open a cursor to perform database operations
        self._cur = self._conn.cursor()

        # Создать таблицы
        self._create_table_units()
        self._create_table_clients()
        self._create_table_auth()
        self._create_table_manager()
        self._create_table_stop_list()
        self._create_table_config()

    def _create_table_units(self):
        self.execute("""
            CREATE TABLE IF NOT EXISTS units (
                id BIGSERIAL PRIMARY KEY,
                country_code VARCHAR(2),
                unit_id INTEGER,
                uuid VARCHAR(32),
                unit_name VARCHAR(40),
                tz_shift INTEGER,
                UNIQUE (country_code, unit_id)
            );
        """)

    def _create_table_clients(self):
        # first_order_type: 0 - Доставка, 1 - Самовывоз, 2 - Ресторан, 3 - Прочее
        self.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id BIGSERIAL PRIMARY KEY,
                db_unit_id BIGINT,
                phone VARCHAR(20),
                first_order_datetime TIMESTAMP WITH TIME ZONE,
                first_order_city VARCHAR(40),
                last_order_datetime TIMESTAMP WITH TIME ZONE,
                last_order_city VARCHAR(40),
                first_order_type INTEGER,
                orders_amt INTEGER,
                orders_sum INTEGER,
                sms_text VARCHAR(150),
                sms_text_city VARCHAR(30),
                ftp_path_city VARCHAR(15),
                UNIQUE (db_unit_id, phone),
                CONSTRAINT fk_units
                    FOREIGN KEY (db_unit_id)
                        REFERENCES units(id)
                        ON DELETE CASCADE
            );
        """)

    def _create_table_auth(self):
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS auth (
                id BIGSERIAL PRIMARY KEY,
                db_unit_id BIGINT,
                login VARCHAR(256),
                password VARCHAR(256),
                is_active BOOLEAN,
                last_update TIMESTAMP WITH TIME ZONE,
                CONSTRAINT fk_units
                    FOREIGN KEY (db_unit_id)
                        REFERENCES units(id)
                        ON DELETE CASCADE
            );
            """)

    def _create_table_manager(self):
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS manager (
                id BIGSERIAL PRIMARY KEY,
                db_unit_id BIGINT,
                bot_id INTEGER,
                customer_id INTEGER,
                new_start_date DATE,
                new_shop_exclude BOOLEAN,
                new_city VARCHAR(40),
                new_source_deliv VARCHAR(20),
                new_source_rest VARCHAR(20),
                new_source_pickup VARCHAR(20),
                new_promo_deliv TEXT,
                new_promo_rest TEXT,
                new_promo_pickup TEXT,
                pizzeria VARCHAR(30),
                new_is_active_deliv BOOLEAN,
                new_is_active_rest BOOLEAN,
                new_is_active_pickup BOOLEAN,
                lost_start_date DATE,
                lost_shift_months INTEGER,
                lost_shop_exclude BOOLEAN,
                lost_city VARCHAR(40),
                lost_source VARCHAR(20),
                lost_is_active BOOLEAN,
                lost_promo TEXT,
                UNIQUE (country_code, unit_id),
                CONSTRAINT fk_units
                    FOREIGN KEY (db_unit_id)
                        REFERENCES units(id)
                        ON DELETE CASCADE
            );
            """)

    def _create_table_stop_list(self):
        self.execute("""
            CREATE TABLE IF NOT EXISTS stop_list(
            id BIGSERIAL PRIMARY KEY,
            phone VARCHAR(20) UNIQUE,
            last_call_date TIMESTAMP WITH TIME ZONE,
            do_not_call BOOLEAN
            );
        """)

    def _create_table_config(self):
        self.execute("""
            CREATE TABLE IF NOT EXISTS config (
                id BIGSERIAL PRIMARY KEY,
                parameter VARCHAR(100) UNIQUE,
                value VARCHAR(100)
            );
        """)

    def execute(self, query: str, argslist: Union[List, Tuple] = None):
        if argslist and len(argslist) > 1 and query.count('%s') == 1:
            execute_values(self._cur, query, argslist)
        else:
            self._cur.execute(query, argslist)

    def fetch(self, one: bool = False) -> Union[Tuple, List]:
        return self._cur.fetchone() if one else self._cur.fetchall()

    def clean(self):
        query = """
        DELETE FROM clients
        USING units
        WHERE clients.db_unit_id = units.id 
        AND date_trunc('day', last_order_datetime + interval '%s days') 
              < date_trunc('day', now() AT TIME ZONE 'UTC' + interval '1 hour' * units.tz_shift);
        """
        self.execute(query, (config.DELTA_DAYS,))

    def close(self):
        # Make the changes to the database persistent
        self._conn.commit()
        # Close communication with the database
        self._cur.close()
        self._conn.close()
