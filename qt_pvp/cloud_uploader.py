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

def _download_file_safe(client, remote_path: str, local_path: str) -> bool:
    """
    Пытается скачать через webdav3, а при KeyError('content-length') —
    делает raw GET через client.session без требования Content-Length.
    """
    try:
        client.download_sync(remote_path=remote_path, local_path=local_path)
        return True
    except KeyError as e:
        if str(e).strip("'\"").lower() != "content-length":
            raise
        logger.warning(f"[REPORTS] Нет Content-Length у {remote_path}; fallback на raw GET")

        # Собираем полный URL
        from urllib.parse import quote

        base = (client.options.get("webdav_hostname") or "").rstrip("/")
        root = (client.options.get("webdav_root") or "").strip("/")
        if not base:
            raise RuntimeError("webdav_hostname is not configured")

        # слепим корень и путь (оба POSIX-стайл), затем процитируем небезопасные символы (пробелы/кириллица)
        joined_path = "/".join(p for p in [root, remote_path.lstrip("/")] if p)
        full_url = f"{base}/{joined_path}"
        full_url = quote(full_url, safe=":/%")

        # Качаем потоково
        resp = client.session.get(full_url, stream=True)
        resp.raise_for_status()

        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
        with open(local_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return True


def append_report_line_to_cloud(
    remote_folder_path: str,
    created_start_time: str,
    created_end_time: str,
    file_name: str,
    report_filename: str = "reports.txt",
) -> bool:
    """
    Создаёт (если нет) или обновляет reports.txt в заданной папке WebDAV, добавляя строку:
    "{created_start_time} {created_end_time} {file_name}"
    Все сетевые обращения выполняются с до 3 попыток.
    """
    tmp_local = None
    try:
        # Подготовка путей и временного файла
        remote_file_path = posixpath.join(remote_folder_path, report_filename)
        tmp_dir = getattr(settings, "REPORTS_TEMP_FOLDER", "/tmp")
        os.makedirs(tmp_dir, exist_ok=True)
        tmp_local = os.path.join(tmp_dir, f"{uuid.uuid4().hex}.txt")

        line = f"{created_start_time} {created_end_time} {file_name}\n"

        # Ретрай-цикл на все облачные операции
        last_err = None
        for attempt in range(1, 4):
            try:
                logger.debug(f"[REPORTS] attempt {attempt}/3 → {remote_file_path}")

                # 1) гарантируем наличие папки
                if not create_folder_if_not_exists(client, remote_folder_path):
                    raise RuntimeError(f"Папка {remote_folder_path} недоступна для записи")

                # 2) проверяем, существует ли отчёт
                exists = False
                try:
                    exists = client.check(remote_file_path)
                except Exception as e_check:
                    # Некоторые WebDAV-сервера могут не поддерживать PROPFIND корректно
                    logger.warning(f"[REPORTS] check({remote_file_path}) упал: {e_check}. Продолжаем как 'не существует'.")
                    exists = False

                # 3) либо скачиваем и аппендим, либо создаём заново
                if exists:
                    # безопасно скачиваем (с fallback)
                    if not _download_file_safe(client, remote_file_path, tmp_local):
                        raise RuntimeError(f"Не удалось скачать {remote_file_path}")

                    # аккуратно добавим строку (с \n, если его не было)
                    with open(tmp_local, "rb") as frb:
                        content = frb.read()
                    needs_nl = len(content) > 0 and not content.endswith(b"\n")
                    with open(tmp_local, "ab") as fab:
                        if needs_nl:
                            fab.write(b"\n")
                        fab.write(line.encode("utf-8"))
                else:
                    # создаём новый локальный файл с одной строкой
                    with open(tmp_local, "w", encoding="utf-8") as fw:
                        fw.write(line)

                # 4) загружаем обратно (тоже под ретрай внешнего цикла)
                ok = upload_file_to_cloud(client, tmp_local, remote_file_path)
                if not ok:
                    raise RuntimeError("upload_file_to_cloud вернул False")

                logger.info(f"[REPORTS] Обновлён {remote_file_path}")
                return True

            except Exception as e:
                last_err = e
                logger.warning(f"[REPORTS] Ошибка на попытке {attempt}/3: {e}")
                # на последней попытке упадём окончательно
                if attempt < 3:
                    # можно добавить небольшую паузу при желании:
                    # time.sleep(0.5)
                    continue
                break

        if last_err:
            raise last_err
        return False

    except Exception as e:
        logger.error(f"[REPORTS] Не удалось обновить {remote_folder_path}/{report_filename}: {e}\n{traceback.format_exc()}")
        return False
    finally:
        # Чистим временный файл
        try:
            if tmp_local and os.path.exists(tmp_local):
                delete_local_file(tmp_local)
        except Exception:
            pass


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

# qt_pvp/cloud_uploader.py
def interest_folder_exists(interest_name: str, dest_directory: str) -> bool:
    registr_folder, date_folder_path, interest_folder_path = get_interest_folder_path(interest_name, dest_directory)
    try:
        return client.check(interest_folder_path)
    except Exception as e:
        logger.warning(f"Не удалось проверить наличие папки {interest_folder_path}: {e}")
        return False


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


def upload_file_to_cloud(client, local_file_path, remote_path, retries=3, delay_sec=2):
    """
    Загрузка файла на WebDAV сервер в указанную папку с повторами при ошибке.

    :param client: WebDAV клиент.
    :param local_file_path: Путь к локальному файлу.
    :param remote_path: Путь на сервере.
    :param retries: Количество попыток.
    :param delay_sec: Задержка между попытками.
    :return: True если успех, иначе False.
    """
    for attempt in range(1, retries + 1):
        try:
            client.upload_sync(remote_path=remote_path,
                               local_path=local_file_path)
            logger.info(f"Файл {local_file_path} успешно загружен в {remote_path}.")
            return True
        except Exception as e:
            logger.warning(f"Попытка {attempt} загрузки {local_file_path} не удалась: {e}")
            if attempt < retries:
                time.sleep(delay_sec)
            else:
                logger.error(f"Файл {local_file_path} не удалось загрузить после {retries} попыток.")
    return False


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

    return {"register_folder_path": registr_folder,
            "date_forder_path": date_folder_path,
            "interest_folder_path": interest_folder_path}

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
