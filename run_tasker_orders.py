from tasker_orders import DatabaseTaskerOrders
from bot import Bot
from config import YANDEX_ORDERS_FOLDER
from storage import YandexCreateFolderError, YandexUploadError, YandexFileNotFound


def main():
    #bot = Bot()

    try:
        db_tasker_orders = DatabaseTaskerOrders()
        db_tasker_orders.upload()
        db_tasker_orders.db_close()
    except (YandexCreateFolderError,
            YandexUploadError,
            YandexFileNotFound) as e:
        print('>> ' + e.message)
    
    print('>> Upload is successfully completed.')


if __name__ == '__main__':
    main()

