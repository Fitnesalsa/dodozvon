from environs import Env

env = Env()
env.read_env()

PG_USER = env.str('PG_USER')
PG_PASS = env.str('PG_PASS')
PG_DATABASE = env.str('PG_DATABASE')
PG_HOST = env.str('PG_HOST')
PG_PORT = env.int('PG_PORT')

CONNECT_TIMEOUT = env.int('CONNECT_TIMEOUT')
