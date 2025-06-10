import time
import traceback

from webdav3.client import Client
from qt_pvp.logger import logger
from qt_pvp import settings
import posixpath
import json
import uuid
import os

# Настройки подключения к WebDAV серверу
options = {
    'webdav_hostname': os.environ.get("webdav_hostname"),
    'webdav_login': os.environ.get("webdav_login"),
    'webdav_password': os.environ.get("webdav_password")
}

client = Client(options)


def parse_filename(filename):
    """
    Парсинг названия файла для извлечения имени регистратора и даты.
    Предполагается, что имя файла имеет следующий формат:
    "регистр_имя_YYYY-MM-DD_HH_MM_SS.mp4"
    """
    # Разбиваем строку на части
    parts = filename.split(' ')
    main_part = parts[0]
    main_parts = main_part.split("_")
    reg_id = main_parts[0]
    date_str = main_parts[1]
    return reg_id, date_str


def create_folder_if_not_exists(client, folder_path):
    """
    Проверяем существование папки и создаем её, если она отсутствует.
    """
    try:
        if client.check(folder_path):
            return True  # Уже есть
        logger.info(f"Папка {folder_path} не существует. Создаю...")
        count = 0
        while count < 2:
            try:
                client.mkdir(folder_path)
                return True
            except Exception as e:
                logger.warning(
                    f"Ошибка при создании папки {folder_path} на WebDAV! ({e}) "
                    f"Попытка {count+1}/2")
                count += 1
                time.sleep(1)
        logger.critical(f"Не удалось создать папку {folder_path}")
        return False
    except Exception as e:
        logger.error(f"Ошибка при проверке существования папки {folder_path}: {e}")
        return False


def upload_file_to_cloud(client, local_file_path, remote_path):
    """
    Загрузка файла на WebDAV сервер в указанную папку.
    """
    try:
        client.upload_sync(remote_path=remote_path,
                           local_path=local_file_path)
        print(f"Файл {local_file_path} успешно загружен.")
        return True
    except Exception as e:
        print(f"Ошибка загрузки файла {local_file_path}: {e}")


def delete_local_file(local_file_path):
    """
    Удаление локального файла после успешной загрузки.
    """
    try:
        os.remove(local_file_path)
        print(f"Локальный файл {local_file_path} удалён.")
    except OSError as e:
        print(f"Не удалось удалить локальный файл {local_file_path}: {e}")


def get_interest_folder_path(interest_name, dest_directory):
    registr_name, date_str = parse_filename(interest_name)
    # Формируем пути на удаленном сервере
    registr_folder = posixpath.join(dest_directory, registr_name)
    date_folder = f'{date_str}'
    date_folder_path = posixpath.join(registr_folder, date_folder)
    interest_folder_path = posixpath.join(date_folder_path, interest_name)
    return registr_folder, date_folder_path, interest_folder_path


def create_interest_folder_path(interest_name, dest_directory):
    registr_folder, date_folder_path, interest_folder_path = get_interest_folder_path(
        interest_name, dest_directory)

    created_registr = create_folder_if_not_exists(client, registr_folder)
    created_date = create_folder_if_not_exists(client, date_folder_path)
    created_interest = create_folder_if_not_exists(client, interest_folder_path)

    if not (created_registr and created_date and created_interest):
        logger.error(
            f"Не удалось создать структуру папок для интереса {interest_name}. "
            f"registr_folder: {created_registr}, "
            f"date_folder_path: {created_date}, "
            f"interest_folder_path: {created_interest}")
        return None  # Явно

    return interest_folder_path

def upload_file(file_path, interest_folder_path):
    """
    Загружает файл и фотографии в облако через WebDAV.

    :param file_path: Путь к файлу для загрузки.
    :param dest_directory: Базовая директория на удаленном сервере.
    :param pics: Словарь с фотографиями (before и after).
    :return: True, если все файлы загружены успешно, иначе False.
    """

    remote_path = posixpath.join(interest_folder_path,
                                 os.path.basename(file_path))
    success = upload_file_to_cloud(client, file_path, remote_path)
    return success


def create_pics(interest_folder_path, pics_before, pics_after):
    after_pics_folder = posixpath.join(interest_folder_path, "after_pics")
    before_pics_folder = posixpath.join(interest_folder_path, "before_pics")

    a = create_folder_if_not_exists(client, after_pics_folder)
    b = create_folder_if_not_exists(client, before_pics_folder)
    # Загружаем основной файл на сервер
    if pics_before and a:
        upload_pics(pics_before, before_pics_folder)
    if pics_after and b:
        upload_pics(pics_after, after_pics_folder)


def upload_pics(pics, destinaton_folder):
    try:
        for photo_path in pics:
            if photo_path:  # Проверяем, что путь к фото не пустой
                photo_name = os.path.basename(photo_path)
                remote_path = posixpath.join(destinaton_folder,
                                             photo_name)
                upload_success = upload_file_to_cloud(client, photo_path,
                                                      remote_path)
                if upload_success:
                    delete_local_file(photo_path)

    except Exception as e:
        print(f"Ошибка при загрузке фотографий: {e}")


def upload_dict_as_json_to_cloud(data: dict, remote_folder_path: str,
                                 filename: str = "report.json"):
    """
    Сохраняет словарь в JSON и загружает на WebDAV в указанную папку.

    :param data: Словарь с данными для сохранения
    :param remote_folder_path: Папка в облаке для загрузки (WebDAV)
    :param filename: Имя файла (по умолчанию — data.json)
    """
    logger.info(f"Выгрузка отчета в {remote_folder_path}")
    try:
        # Уникальное имя временного файла
        local_filename = f"{uuid.uuid4().hex}.json"
        local_file_path = os.path.join(settings.REPORTS_TEMP_FOLDER,
                                       local_filename)

        # Сохраняем словарь в JSON
        with open(local_file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        # Убедимся, что папка в облаке существует
        a = create_folder_if_not_exists(client, remote_folder_path)
        if not a:
            return
        # Задаём путь в облаке
        remote_file_path = posixpath.join(remote_folder_path, filename)

        # Загружаем файл
        success = upload_file_to_cloud(client, local_file_path,
                                       remote_file_path)

        # Удаляем локальный файл после загрузки
        if success:
            delete_local_file(local_file_path)
        logger.info("Отчет успешно выгружен")
        return success

    except Exception as e:
        print(f"Ошибка при сохранении JSON в облако: {e}")
        return False
