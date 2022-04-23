from dodo_openapi import DodoOpenAPIParser, DodoOpenAPIStorer


def run():
    # update unit ids
    api_parser = DodoOpenAPIParser()
    api_storer = DodoOpenAPIStorer()
    api_result = api_parser.parse_unit_info()
    api_storer.store_unit_info(api_result)

if __name__ == '__main__':
    run()