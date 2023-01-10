from zipfile import BadZipFile

from dodois import DodoISParser, DodoAuthError, DodoResponseError, DodoEmptyExcelError
from postgresql import Database
from tasker import DatabaseTasker


def run():
    """
    Скрипт делает три вещи:
    1. парсит промокоды из Додо ИС, с данными: пиццерии is_active + new_shop_exclude \ lost_shop_exclude, даты из
    promos_start_date, promos_end_date;
    2. делает excel файлы по спарcенным промо и выгружает на яндекс диск
    3. выгружает заказы за период promos_start_date, promos_end_date по пиццериям is_active на яндекс диск
    """

    db = Database()
    db.connect()

    tasker = DatabaseTasker(db=db)

    # новые клиенты - промо

    for id_, bot_id, *params in tasker.get_new_promo_params():
        try:
            print(f'parsing new clients promos for id {id_}, params {params}')
            dodois_parser = DodoISParser(*params)
            dodois_result = dodois_parser.parse('promo')
            tasker.create_promo_tables(dodois_result, bot_id, params[1], params[5], params[6], 'НК')
            print(f'creating new clients promo report for id {id_} completed.')

        except (ValueError, BadZipFile) as e:
            print(f'{params[1]}: Что-то пошло не так ({e})')
        except (DodoAuthError, DodoResponseError, DodoEmptyExcelError) as e:
            print(f'{params[1]}: {e.message}')
        except Exception as e:
            print(f'Ошибка выгрузки из Додо ИС: {e}')
            raise e

    # потоерянные клиенты - промо
    for id_, bot_id, *params in tasker.get_lost_promo_params():
        try:
            print(f'parsing lost clients promos for id {id_}, params {params}')
            dodois_parser = DodoISParser(*params)
            dodois_result = dodois_parser.parse('promo')
            tasker.create_promo_tables(dodois_result, bot_id, params[1], params[5], params[6], 'ПК')
            print(f'creating lost clients promo report for id {id_} completed.')

        except (ValueError, BadZipFile) as e:
            print(f'{params[1]}: Что-то пошло не так ({e})')
        except (DodoAuthError, DodoResponseError, DodoEmptyExcelError) as e:
            print(f'{params[1]}: {e.message}')
        except Exception as e:
            print(f'Ошибка выгрузки из Додо ИС: {e}')
            raise e

    # заказы


    print('all tasks completed.')


if __name__ == '__main__':  # явный запуск скрипта
    run()