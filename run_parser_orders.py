from dodois_orders import DodoISParserOrders, DodoISStorerOrders
from parameters import ParametersGetter
from postgresql import Database
from dodo_openapi import DodoOpenAPIParser, DodoOpenAPIStorer
from dodois import DodoAuthError, DodoEmptyExcelError, DodoResponseError

from zipfile import BadZipFile

import pandas as pd
from datetime import datetime


def main():
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
            print(f'Parsing id {id}, params {params_set}...')
            dodois_parser_orders = DodoISParserOrders(*params_set)
            dodois_storer_orders = DodoISStorerOrders(id_, db=db)
            dodois_result_orders = dodois_parser_orders.parse()
            dodois_storer_orders.store(dodois_result_orders)
        except (ValueError, BadZipFile) as err:
            print(f'Something went wrong with {params_set[1]}: {err}')
        except (DodoAuthError, DodoResponseError, DodoEmptyExcelError) as e:
            print(f'{params_set[1]}: {e.message}')
    
    db.close()

if __name__=='__main__':
    main()

