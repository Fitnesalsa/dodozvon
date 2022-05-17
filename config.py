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
YANDEX_NEW_CLIENTS_FOLDER = env.str('YANDEX_NEW_CLIENTS_FOLDER')

CONNECT_TIMEOUT = 180
PARSE_ATTEMPTS = 5

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
