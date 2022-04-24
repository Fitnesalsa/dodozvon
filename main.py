from dodo_openapi import DodoOpenAPIParser, DodoOpenAPIStorer
from dodois import DodoISParser, DodoISStorer
from parameters import ParametersGetter
from postgresql import Database


def run():
    db = Database()
    db.connect()

    # обновляем данные таблицы units
    api_parser = DodoOpenAPIParser()
    api_storer = DodoOpenAPIStorer(db=db)
    api_result = api_parser.parse()
    api_storer.store(api_result)

    # получаем параметры парсинга
    params_getter = ParametersGetter(db=db)
    params = params_getter.get_parsing_params()
    for params in params:
        print(params)

    # передаем парсеру клиентской статистики
    # for params_set in params:
    #     dodois_parser = DodoISParser(*params_set)
    #     dodois_storer = DodoISStorer(params_set[0], db)
    #     dodois_result = dodois_parser.parse()
    #     dodois_storer.store(dodois_result)

    # чистим бд
    db.clean()
    # закрываем соединение
    db.close()


if __name__ == '__main__':
    run()