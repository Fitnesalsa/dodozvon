from dodo_openapi import DodoOpenAPIParser, DodoOpenAPIStorer
from parameters import ParametersGetter
from postgresql import Database


def run():
    db = Database()
    db.connect()

    # обновляем данные таблицы units
    api_parser = DodoOpenAPIParser()
    api_storer = DodoOpenAPIStorer(db=db)
    api_result = api_parser.parse_unit_info()
    api_storer.store_unit_info(api_result)

    # получаем параметры парсинга
    params_getter = ParametersGetter(db=db)
    params = params_getter.get_parsing_params()
    print(params)
    db.close()


if __name__ == '__main__':
    run()