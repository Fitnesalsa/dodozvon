from bot import Bot
from config import YANDEX_NEW_CLIENTS_FOLDER
from tasker import DatabaseTasker, YandexUploadError, YandexCreateFolderError


def run():
    bot = Bot()

    try:
        db_tasker = DatabaseTasker()
        db_tasker.create_new_clients_tables()
    except (YandexCreateFolderError, YandexUploadError) as e:
        bot.send_message(e.message)

    bot.send_message(f'Выгрузка отчётов завершена. https://disk.yandex.ru/client/disk/{YANDEX_NEW_CLIENTS_FOLDER}')


if __name__ == '__main__':
    run()
