import time
from zipfile import BadZipFile

from bot import Bot
from config import PARSE_ATTEMPTS
from dodo_openapi import DodoOpenAPIParser, DodoOpenAPIStorer
from dodois import DodoISParser, DodoISStorer, DodoAuthError, DodoEmptyExcelError, DodoResponseError
from parameters import ParametersGetter
from postgresql import Database


def run():
    db = Database()
    db.connect()

    bot = Bot()

    # обновляем данные таблицы units
    api_parser = DodoOpenAPIParser()
    api_storer = DodoOpenAPIStorer(db=db)
    api_result = api_parser.parse()
    api_storer.store(api_result)

    # получаем параметры парсинга
    params_getter = ParametersGetter(db=db)
    params = params_getter.get_parsing_params()

    # передаем парсеру клиентской статистики
    for (id_, *params_set) in params:  # (unit_id, unit_name, login... )
        attempts = PARSE_ATTEMPTS
        while attempts > 0:
            attempts -= 1
            try:
                print(f'parsing id {id_}, params {params_set}...')
                dodois_parser = DodoISParser(*params_set)
                dodois_storer = DodoISStorer(id_, db=db)
                dodois_result = dodois_parser.parse()
                dodois_storer.store(dodois_result)
                attempts = 0  # если всё получилось и исключение не сработало, обнуляем счетчик попыток сразу
            except (ValueError, BadZipFile) as e:
                bot.send_message(f'{params_set[1]}: Что-то пошло не так ({e}) , еще попыток: {attempts}')
            except (DodoAuthError, DodoResponseError, DodoEmptyExcelError) as e:
                bot.send_message(f'{params_set[1]}: {e.message}; еще попыток: {attempts}')
            time.sleep(2)  # ну на всякий случай, дадим серверу отдохнуть

    # чистим бд
    db.clean()
    # закрываем соединение
    db.close()


if __name__ == '__main__':  # явный запуск скрипта
    run()
