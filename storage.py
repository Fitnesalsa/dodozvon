from datetime import datetime
from urllib.parse import quote

import requests

from config import YANDEX_API_TOKEN


class YandexUploadError(Exception):
    def __init__(self, filename: str, response: dict):
        self.message = f'Ошибка при загрузке на диск. Файл: {filename}, ответ: {response}'
        super().__init__(self.message)


class YandexCreateFolderError(Exception):
    def __init__(self, folder: str, response: dict):
        self.message = f'Ошибка при создании папки {folder}, ответ: {response}'
        super().__init__(self.message)


class YandexFileNotFound(Exception):
    def __init__(self, path: str):
        self.message = f'Файл не найден по адресу {path}.'
        super().__init__(self.message)


class YandexDisk:
    """
    Класс реализует работу с АПИ Яндекс.Диска.
    Доступные методы: выгрузка на диск, чтение даты последнего обноеления, скачивание с диска.
    """
    def __init__(self):
        self._request_url = 'https://cloud-api.yandex.net/v1/disk/resources'
        self._headers = {'Content-Type': 'application/json',
                         'Accept': 'application/json',
                         'Authorization': f'OAuth {YANDEX_API_TOKEN}'}

    def upload(self, filename: str, folder: str):
        """
        Выгрузка файла на Яндекс.Диск в заданную папку.
        :param filename: имя файла
        :param folder: имя папки
        :return: None
        """
        # проверяем существует ли папка
        check_folder_response = requests.get(f'{self._request_url}?path=%2F{folder}',
                                             headers=self._headers).json()
        if check_folder_response.get('error') == 'DiskNotFoundError':
            # Если нет, создаем папку
            put_folder_response = requests.put(f'{self._request_url}?path=%2F{folder}',
                                               headers=self._headers).json()
            if 'error' in put_folder_response.keys():
                # если возникла ошибка при создании папки, выкидываем исключение
                raise YandexCreateFolderError(folder, put_folder_response)

        # выгружаем файл в папку
        upload_response = requests.get(f'{self._request_url}/upload?path=%2F{folder}%2F{filename}'
                                       f'&overwrite=true', headers=self._headers).json()
        with open(filename, 'rb') as f:
            try:
                requests.put(upload_response['href'], files={'file': f})
            except KeyError:
                raise YandexUploadError(filename, upload_response)

    def get_modified_date(self, path: str) -> datetime:
        """
        Получение даты изменения файла
        :param path: имя файла с полным путем
        :return: объект datetime
        """
        meta_response = requests.get(f'{self._request_url}?path=%2F{quote(path)}',
                                     headers=self._headers)
        if not meta_response.ok:
            # если путь неверный, выкидываем исключение
            raise YandexFileNotFound(path)

        # преобразуем дату из строки в объект datetime
        date_modified = datetime.strptime(meta_response.json()['modified'], '%Y-%m-%dT%H:%M:%S%z')

        return date_modified

    def download(self, path: str) -> bytes:
        """
        Скачивание файла.
        :param path: полный путь к файлу.
        :return: возвращает содержимое файла в формате bytes (бинарная строка).
        """
        download_response = requests.get(f'{self._request_url}/download?path=%2F{quote(path)}',
                                         headers=self._headers)
        if not download_response.ok:
            raise YandexFileNotFound(path)

        download_link = download_response.json()['href']
        return requests.get(download_link).content
