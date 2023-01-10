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
        self._create_table_orders()

        # Создать функции
        self._create_functions()

        # Создать триггеры
        # В 12-й версии нет CREATE OR REPLACE для триггеров, поэтому создаем только один раз, иначе ошибка
        # self._create_triggers()

    def _create_table_units(self):
        self.execute("""
            CREATE TABLE IF NOT EXISTS units (
                id BIGSERIAL PRIMARY KEY,
                country_code VARCHAR(2),
                unit_id INTEGER,
                uuid VARCHAR(32),
                unit_name VARCHAR(40),
                tz_shift INTEGER,
                begin_date_work DATE,
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
                UNIQUE (phone),
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
                new_clients_promos_all TEXT,
                lost_clients_promos_all TEXT,
                custom_start_date DATE,
                custom_end_date DATE,
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

    def _create_table_orders(self):
        self.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id BIGSERIAL PRIMARY KEY,
                db_unit_id BIGINT,
                city VARCHAR(40),
                department VARCHAR(40),
                date TIMESTAMP WITH TIME ZONE,
                time TIMESTAMP WITH TIME ZONE,
                sales_time TIMESTAMP WITH TIME ZONE,
                order_id VARCHAR(11),
                order_type INTEGER,
                client_name BOOLEAN,
                phone VARCHAR(12),
                order_sum INTEGER,
                payment_type INTEGER,
                status INTEGER,
                operator INTEGER,
                courier VARCHAR(100),
                reason VARCHAR(100),
                address BOOLEAN,
                order_it_int INTEGER,
                transaction_id INTEGER,
                UNIQUE (db_unit_id, date, order_id),
                CONSTRAINT fk_units
                    FOREIGN KEY (db_unit_id)
                        REFERENCES units(id)
                        ON DELETE CASCADE
            );
        """)

    def _create_functions(self):
        # обновление промокодов для новых клиентов
        # переписать по-хорошему эти функции, чтобы срабатывали только на одну строку, а не на всю таблицу сразу
        self.execute("""
            CREATE OR REPLACE FUNCTION update_new_promos() RETURNS trigger AS
            $$
            BEGIN
            WITH np AS (
                SELECT m.id,
                    CASE 
                        WHEN regexp_match(m.new_promo_deliv, '(?<=промо |промокод |Промо |Промокод )\m[A-Z0-9]+\M') IS NULL
                            THEN CASE 
                                    WHEN regexp_match(m.new_promo_pickup, '(?<=промо |промокод |Промо |Промокод )\m[A-Z0-9]+\M') IS NULL
                                        THEN (regexp_match(m.new_promo_rest, '(?<=промо |промокод |Промо |Промокод )\m[A-Z0-9]+\M'))[1]
                                    ELSE (regexp_match(m.new_promo_pickup, '(?<=промо |промокод |Промо |Промокод )\m[A-Z0-9]+\M'))[1]
                                 END
                            ELSE (regexp_match(m.new_promo_deliv, '(?<=промо |промокод |Промо |Промокод )\m[A-Z0-9]+\M'))[1]
                    END AS np
                FROM manager m
            )
            UPDATE manager
            SET new_clients_promos_all =
                CASE 
                    WHEN manager.new_clients_promos_all IS NULL 
                        THEN np.np
                    WHEN np.np = ANY(regexp_split_to_array(manager.new_clients_promos_all, ','))
                        THEN manager.new_clients_promos_all
                    ELSE manager.new_clients_promos_all || ',' || np.np
                END
            FROM np
            WHERE manager.id = np.id;
            RETURN NULL;
            END;
            $$ 
            LANGUAGE plpgsql;
        """)

        # обновление промокодов для пропавших клиентов
        self.execute("""
            CREATE OR REPLACE FUNCTION update_lost_promos() RETURNS trigger AS
            $$
            BEGIN
            WITH np AS (
                SELECT m.id,
                       (regexp_match(m.lost_promo, '(?<=промо |промокод |Промо |Промокод )\m[A-Z0-9]+\M'))[1] AS np
                FROM manager m
            )
            UPDATE manager
            SET lost_clients_promos_all =
                CASE
                    WHEN manager.lost_clients_promos_all IS NULL
                        THEN np.np
                    WHEN np.np = ANY(regexp_split_to_array(manager.lost_clients_promos_all, ','))
                        THEN manager.lost_clients_promos_all
                    ELSE manager.lost_clients_promos_all || ',' || np.np
                END
            FROM np
            WHERE manager.id = np.id;
            RETURN NULL;
            END;
            $$
            LANGUAGE plpgsql;
        """)

    def _create_triggers(self):
        self.execute("""
            CREATE TRIGGER on_update_manager_update_new_promos
                AFTER UPDATE OF new_promo_deliv, new_promo_pickup, new_promo_rest ON manager
                FOR EACH ROW
                EXECUTE FUNCTION update_new_promos();
        """)
        self.execute("""
            CREATE TRIGGER on_update_manager_update_lost_promos
                AFTER UPDATE OF lost_promo ON manager
                FOR EACH ROW
                EXECUTE FUNCTION update_lost_promos();
        """)

    def execute(self, query: str, argslist: Union[List, Tuple] = None):
        if argslist and len(argslist) > 1 and query.count('%s') == 1:
            execute_values(self._cur, query, argslist)
        else:
            self._cur.execute(query, argslist)

    def fetch(self, one: bool = False) -> Union[Tuple, List]:
        return self._cur.fetchone() if one else self._cur.fetchall()

    def close(self):
        # Make the changes to the database persistent
        self._conn.commit()
        # Close communication with the database
        self._cur.close()
        self._conn.close()
