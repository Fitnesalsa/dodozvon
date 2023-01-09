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

    # новые клиенты - промо
    db.execute("""
        SELECT 
            u.id,
            m.bot_id,
            u.unit_id, 
            u.unit_name, 
            a.login, 
            a.password, 
            u.tz_shift,
            m.custom_start_date, 
            m.custom_end_date,
            m.new_clients_promos_all
        FROM units u
        JOIN auth a
            ON u.id = a.db_unit_id
        JOIN manager m
            ON u.id = m.db_unit_id
        WHERE a.is_active = true AND
            m.new_shop_exclude = false AND
            m.custom_start_date IS NOT NULL AND
            m.custom_end_date IS NOT NULL;
    """)

    for id_, bot_id, *params in db.fetch():
        try:
            print(f'parsing promos for id {id_}, params {params}')
            dodois_parser = DodoISParser(*params)
            dodois_result = dodois_parser.parse('promo')
            print('parsing completed')
            db_tasker = DatabaseTasker(db=db)
            print('tasker created')
            db_tasker.create_new_promo_tables(dodois_result, bot_id, params[1], params[5], params[6])
            print(f'creating promo report for id {id_} completed.')

        except (ValueError, BadZipFile) as e:
            print(f'{params[1]}: Что-то пошло не так ({e})')
        except (DodoAuthError, DodoResponseError, DodoEmptyExcelError) as e:
            print(f'{params[1]}: {e.message}')
        except Exception as e:
            print(f'Ошибка выгрузки из Додо ИС: {e}')
            raise e

    print('all tasks completed.')


if __name__ == '__main__':  # явный запуск скрипта
    run()