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

CONNECT_TIMEOUT = env.int('CONNECT_TIMEOUT')
