from dodois_orders import DodoISParserOrders, DodoISStorerOrders
from parameters import ParametersGetter
from postgresql import Database
from dodo_openapi import DodoOpenAPIParser, DodoOpenAPIStorer
from dodois import DodoAuthError, DodoEmptyExcelError, DodoResponseError
#from bot import Bot

from zipfile import BadZipFile

import schedule
from datetime import datetime
import time


def main():
    #bot = Bot()
    cur_time = datetime.now().strftime('%H:%M:%S')
    print(f'{cur_time}: Begin parsing process.')
    #bot.send_message(f'{cur_time}: Begin parsing process.')
    db = Database()
    db.connect()
    
    # Units table data update.
    api_parser = DodoOpenAPIParser()
    api_storer = DodoOpenAPIStorer(db=db)
    api_result = api_parser.parse()
    api_storer.store(api_result)

    # Get parsing parameters.
    params_getter = ParametersGetter(db=db)
    params = params_getter.get_parsing_params()

    # Pass to orders parser.
    for (id_, *params_set) in params:
        try:
            cur_time = datetime.now().strftime('%H:%M:%S')
            print(f'{cur_time}: Parsing id {id}, params {params_set}...')
            #bot.send_message(f'Parsing id {id}, params {params_set}...')
            dodois_parser_orders = DodoISParserOrders(*params_set)
            dodois_storer_orders = DodoISStorerOrders(id_, db=db)
            dodois_result_orders = dodois_parser_orders.parse()
            dodois_storer_orders.store(dodois_result_orders)
        except (ValueError, BadZipFile) as err:
            cur_time = datetime.now().strftime('%H:%M:%S')
            print(f'{cur_time}: Something went wrong with {params_set[1]}: {err}')
            #bot.send_message(f'{cur_time}: Something went wrong with {params_set[1]}: {err}')
        except (DodoAuthError, DodoResponseError, DodoEmptyExcelError) as e:
            cur_time = datetime.now().strftime('%H:%M:%S')
            print(f'{cur_time}: {params_set[1]}: {e.message}')
            #bot.send_message(f'{cur_time}: {params_set[1]}: {e.message}')
    
    db.close()
    cur_time = datetime.now().strftime('%H:%M:%S')
    print(f'{cur_time}: Parsing process succesfully end.\nWaiting...')
    #bot.send_message(f'{cur_time}: Parsing process succesfully end.\nWaiting...')

if __name__=='__main__':
    schedule.every(6).hours.do(main)
    # For the first run.
    main()
    while True:
        schedule.run_pending()
        time.sleep(1)

