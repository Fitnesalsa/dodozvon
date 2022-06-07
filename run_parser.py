from zipfile import BadZipFile

from bot import Bot
from dodo_openapi import DodoOpenAPIParser, DodoOpenAPIStorer
from dodois import DodoISParser, DodoISStorer, DodoAuthError, DodoEmptyExcelError, DodoResponseError
from feedback import FeedbackParser, FeedbackStorer
from parameters import ParametersGetter
from postgresql import Database


def run():
    db = Database()
    db.connect()

    bot = Bot()

    # обновляем данные таблицы units
    try:
        print('Parsing OpenAPI...')
        api_parser = DodoOpenAPIParser()
        api_storer = DodoOpenAPIStorer(db=db)
        api_result = api_parser.parse()
        api_storer.store(api_result)
    except Exception as e:
        bot.send_message(f'Ошибка выгрузки DodoOpenAPI: {e}')
        raise e

    # получаем параметры парсинга
    try:
        print('Getting params...')
        params_getter = ParametersGetter(db=db)
        params = params_getter.get_parsing_params()
    except Exception as e:
        bot.send_message(f'Ошибка получения параметров: {e}')
        raise e

    # передаем парсеру клиентской статистики
    for (id_, *params_set) in params:  # (unit_id, unit_name, login... )
        try:
            print(f'parsing id {id_}, params {params_set}...')
            dodois_parser = DodoISParser(*params_set)
            dodois_storer = DodoISStorer(id_, db=db)
            dodois_result = dodois_parser.parse()
            dodois_storer.store(dodois_result)
        except (ValueError, BadZipFile) as e:
            bot.send_message(f'{params_set[1]}: Что-то пошло не так ({e})')
        except (DodoAuthError, DodoResponseError, DodoEmptyExcelError) as e:
            bot.send_message(f'{params_set[1]}: {e.message}')
        except Exception as e:
            bot.send_message(f'Ошибка выгрузки из Додо ИС: {e}')
            raise e

    # обновляем таблицы с фидбеком
    try:
        print('Updating stop list...')
        stop_list_last_modified_date = params_getter.get_config_param('StopListLastModifiedDate')[0]
        feedback_parser = FeedbackParser()
        feedback_storer = FeedbackStorer(db=db)
        feedback_result = feedback_parser.parse(stop_list_last_modified_date)
        if feedback_result:
            feedback_storer.store(*feedback_result)
    except Exception as e:
        bot.send_message(f'Ошибка выгрузки файла с обзвоненными клиентами: {e}')
        raise e

    print('Parsing complete!')

    # чистим бд
    db.clean()
    # закрываем соединение
    db.close()


if __name__ == '__main__':  # явный запуск скрипта
    run()
