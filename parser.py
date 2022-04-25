from bot import Bot
from dodo_openapi import DodoOpenAPIParser, DodoOpenAPIStorer
from dodois import DodoISParser, DodoISStorer, DodoAuthError
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
    for params_set in params:
        try:
            dodois_parser = DodoISParser(*params_set)
            dodois_storer = DodoISStorer(params_set[0], db=db)
            dodois_result = dodois_parser.parse()
            dodois_storer.store(dodois_result)
        except ValueError:
            bot.send_message(f'{params_set[1]}: Что-то пошло не так')
        except DodoAuthError as e:
            bot.send_message(e.message)

    # чистим бд
    db.clean()
    # закрываем соединение
    db.close()


if __name__ == '__main__':
    run()
