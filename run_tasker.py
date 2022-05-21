from bot import Bot
from config import YANDEX_NEW_CLIENTS_FOLDER, YANDEX_LOST_CLIENTS_FOLDER
from tasker import DatabaseTasker, YandexUploadError, YandexCreateFolderError


def run():
    bot = Bot()

    try:
        db_tasker = DatabaseTasker()
        db_tasker.create_new_clients_tables()
        db_tasker.create_lost_clients_tables()
    except (YandexCreateFolderError, YandexUploadError) as e:
        bot.send_message(e.message)

    bot.send_message(f'Выгрузка отчётов завершена.\n'
                     f'Новые: https://disk.yandex.ru/client/disk/{YANDEX_NEW_CLIENTS_FOLDER}\n'
                     f'Пропавшие: https://disk.yandex.ru/client/disk/{YANDEX_LOST_CLIENTS_FOLDER}')


if __name__ == '__main__':
    run()
