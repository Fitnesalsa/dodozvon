"""
Модуль для загрузки переменных из файла .env.
Также хранит основные переменные проекта.
"""

from environs import Env

env = Env()
env.read_env()

PG_USER = env.str('PG_USER')
PG_PASS = env.str('PG_PASS')
PG_DATABASE = env.str('PG_DATABASE')
PG_HOST = env.str('PG_HOST')
PG_PORT = env.int('PG_PORT')

TG_BOT_TOKEN = env.str('TG_BOT_TOKEN')
TG_ADMIN_ID = env.list('TG_ADMIN_ID', subcast=int)

YANDEX_API_TOKEN = env.str('YANDEX_API_TOKEN')
YANDEX_NEW_CLIENTS_FOLDER = 'call_new_clients'
YANDEX_LOST_CLIENTS_FOLDER = 'call_lost_clients'

# таймаут для запросов requests в секундах
CONNECT_TIMEOUT = 180
# количество повторений для попытки запросов requests парсера
PARSE_ATTEMPTS = 5
# период актуальность локальной базы клиентов
DELTA_DAYS = 550

TIMEZONES = {
    2: 'Europe/Kaliningrad',
    3: 'Europe/Moscow',
    4: 'Europe/Samara',
    5: 'Asia/Yekaterinburg',
    6: 'Asia/Omsk',
    7: 'Asia/Krasnoyarsk',
    8: 'Asia/Irkutsk',
    9: 'Asia/Yakutsk',
    10: 'Asia/Vladivostok',
    11: 'Asia/Magadan',
    12: 'Asia/Kamchatka'
}
