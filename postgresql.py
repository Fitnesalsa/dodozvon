from typing import Union

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
        # Connect to an existing database
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

    def _create_table_units(self):
        self.execute("""
            CREATE TABLE IF NOT EXISTS units (
                id SERIAL PRIMARY KEY,
                country_code VARCHAR(2),
                unit_id INTEGER,
                uuid VARCHAR(32),
                unit_name VARCHAR(30),
                tz_shift INTEGER,
                UNIQUE (country_code, unit_id)
            );
        """)

    def _create_table_clients(self):
        # first_order_type: 0 - Доставка, 1 - Самовывоз, 2 - Ресторан, 3 - Прочее
        self.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                db_unit_id INTEGER,
                phone VARCHAR(20),
                first_order_datetime TIMESTAMP WITH TIME ZONE,
                first_order_city VARCHAR(30),
                last_order_datetime TIMESTAMP WITH TIME ZONE,
                last_order_city VARCHAR(30),
                first_order_type INTEGER,
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
                id SERIAL PRIMARY KEY,
                db_unit_id INTEGER,
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
                id SERIAL PRIMARY KEY,
                db_unit_id INTEGER,
                bot_id INTEGER,
                customer_id INTEGER,
                start_date DATE,
                end_date DATE,
                shop_exclude BOOLEAN,
                city VARCHAR(20),
                source_deliv VARCHAR(20),
                source_rest VARCHAR(20),
                source_pickup VARCHAR(20),
                promo_deliv VARCHAR(150),
                promo_rest VARCHAR(150),
                promo_pickup VARCHAR(150),
                pizzeria VARCHAR(30),
                is_active_deliv BOOLEAN,
                is_active_rest BOOLEAN,
                is_active_pickup BOOLEAN,
                UNIQUE (country_code, unit_id),
                CONSTRAINT fk_units
                    FOREIGN KEY (db_unit_id)
                        REFERENCES units(id)
                        ON DELETE CASCADE
            );
            """)

    def execute(self, query: str, argslist: Union[list, tuple] = None):
        if argslist and len(argslist) > 1 and query.count('%s') == 1:
            execute_values(self._cur, query, argslist)
        else:
            self._cur.execute(query, argslist)

    def fetch(self, one: bool = False) -> Union[tuple, list]:
        if not one:
            return self._cur.fetchall()
        if one:
            return self._cur.fetchone()

    def clean(self):
        query = """
        DELETE FROM clients
        USING units
        WHERE clients.db_unit_id = units.id 
        AND date_trunc('day', first_order_datetime + interval '60 days') 
              < date_trunc('day', now() AT TIME ZONE 'UTC' + interval '1 hour' * units.tz_shift);
        """
        self.execute(query)

    def close(self):
        # Make the changes to the database persistent
        self._conn.commit()
        # Close communication with the database
        self._cur.close()
        self._conn.close()
