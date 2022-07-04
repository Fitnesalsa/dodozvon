from tasker_orders import DatabaseTaskerOrders
from bot import Bot
from config import YANDEX_ORDERS_FOLDER
from storage import YandexCreateFolderError, YandexUploadError, YandexFileNotFound


def main():
    #bot = Bot()
    
    # TODO: Add exceptions handling:
    # TODO: Wrong date format; begin_date > end_date.
    # TODO: Nonexisting file; wrong fileformat.
    # TODO: Wrong answer.
    # TODO: Encapsulate all input in class.
    print('>> Please, enter date range (dd.mm.yyyy):') 
    begin_date = input('>> Begin date: ')
    end_date = input('>> End date: ')
    upload_all = input('>> Upload data for all available pizzerias(y/n):\n>> ')
    if upload_all.lower() == 'y':
        upload_all = True
    else:
        upload_all = False

    try:
        db_tasker_orders = DatabaseTaskerOrders(
                upload_all=upload_all,
                end_date=end_date,
                begin_date=begin_date
                )
        db_tasker_orders.upload()
        db_tasker_orders.db_close()
    except (YandexCreateFolderError,
            YandexUploadError,
            YandexFileNotFound) as e:
        print('>> ' + e.message)
    
    print('>> Upload is successfully completed.')


if __name__ == '__main__':
    main()

