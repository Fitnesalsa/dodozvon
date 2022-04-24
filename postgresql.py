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

    def _create_table_units(self):
        self.execute("""
            CREATE TABLE IF NOT EXISTS units (
                country_code VARCHAR(2),
                unit_id INTEGER,
                uuid VARCHAR(32),
                unit_name VARCHAR(30),
                tz_shift INTEGER,
                PRIMARY KEY (country_code, unit_id)
            );
        """)

    def _create_table_clients(self):
        # first_order_type: 0 - Доставка, 1 - Самовывоз, 2 - Ресторан, 3 - Прочее
        self.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                country_code VARCHAR(2),
                unit_id INTEGER,
                phone VARCHAR(20),
                first_order_datetime TIMESTAMP,
                first_order_city VARCHAR(30),
                last_order_datetime TIMESTAMP,
                last_order_city VARCHAR(30),
                first_order_type INTEGER,
                sms_text VARCHAR(150),
                sms_text_city VARCHAR(30),
                ftp_path_city VARCHAR(15),
                PRIMARY KEY (country_code, unit_id, phone)
            );
        """)

    def _create_table_auth(self):
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS auth (
                unit_name VARCHAR(30) PRIMARY KEY,
                login VARCHAR(256),
                password VARCHAR(256),
                is_active BOOLEAN,
                last_update TIMESTAMP
            );
            """)

    def execute(self, query: str, argslist: Union[list, tuple] = None):
        if argslist and len(argslist) > 1 and query.count('%s') == 1:
            execute_values(self._cur, query, argslist)
        else:
            self._cur.execute(query, argslist)

    def fetch(self, one=False) -> Union[tuple, list]:
        if not one:
            return self._cur.fetchall()
        if one:
            return self._cur.fetchone()

    def clean(self):
        query = """
        DELETE FROM clients
        USING units
        WHERE clients.country_code = units.country_code 
        AND clients.unit_id = units.unit_id 
        AND date_trunc('day', last_order_datetime + interval '1 day' * units.tz_shift + interval '60 days') 
              < date_trunc('day', now() AT TIME ZONE 'UTC' + interval '1 hour');
        """
        self.execute(query)

    def close(self):
        # Make the changes to the database persistent
        self._conn.commit()
        # Close communication with the database
        self._cur.close()
        self._conn.close()
