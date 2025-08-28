from qt_pvp.cms_interface import functions
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from qt_pvp.logger import logger
from qt_pvp import settings
import numpy as np
import datetime
import requests
import aiohttp
import asyncio
import uuid
import time
import cv2
import os


@functions.cms_data_get_decorator()
def get_online_devices(jsession, device_id=None):
    return requests.get(
        f"{settings.cms_host}/StandardApiAction_getDeviceOlStatus.action?",
        params={"jsession": jsession,
                "status": 1,
                "devIdno": device_id})


@functions.cms_data_get_decorator()
def login():
    data = requests.get(
        f"{settings.cms_host}/StandardApiAction_login.action?",
        params={"account": settings.cms_login,
                "password": settings.cms_password})
    return data


@functions.cms_data_get_decorator()
def get_video(jsession, device_id: str, start_time_seconds: int,
              end_time_seconds: int, year: int, month: int, day: int,
              chanel_id: int = 0, fileattr: int = 2):
    params = {
        "DevIDNO": device_id, "LOC": 1, "CHN": chanel_id,
        "YEAR": year, "MON": month, "DAY": day,
        "RECTYPE": -1, "FILEATTR": fileattr,
        "BEG": start_time_seconds, "END": end_time_seconds,
        "ARM1": 0, "ARM2": 0, "RES": 0, "STREAM": -1, "STORE": 0,
        "jsession": jsession, "DownType": 2
    }
    url = f"{settings.cms_host}/StandardApiAction_getVideoFileInfo.action"
    logger.debug(f"Getting request {url}. \nParams: {params}")
    # connect=4s, read=8s — меньше ложных таймаутов при ответе
    return requests.get(url, params=params, timeout=(4, 8))



async def fetch_photo_url(data_list, chn_values):
    """
    Функция для получения пути к фото (dph) по заданным значениям chn.
    Работает только с самыми последними (свежими) записями для каждого chn.

    :param data_list: Список словарей с данными (уже отсортированный, свежие записи идут последними).
    :param chn_values: Список значений chn, которые нужно обработать.
    :return: Словарь с результатами, где ключ — chn, значение — путь к фото (dph).
    """
    # Собираем последние записи для каждого chn
    latest_items = {}
    for item in data_list:
        chn = item.get('chn')
        if chn in chn_values:
            # Просто перезаписываем значение для каждого chn
            latest_items[chn] = item

    results = {}
    async with aiohttp.ClientSession() as session:
        for chn, item in latest_items.items():
            down_task_url = item.get('DownTaskUrl')
            # Отправляем GET-запрос и ждем ответа
            while True:
                async with session.get(down_task_url) as response:
                    data = await response.json()
                    # Проверяем, появилось ли значение dph
                    if data.get("oldTaskReal", {}).get("dph") is not None:
                        results[chn] = data["oldTaskReal"]["dph"]
                        break
                    # Если dph еще не появился, ждем некоторое время
                    await asyncio.sleep(1)  # Интервал проверки — 1 секунда

    return results


def get_alarms(jsession, reg_id, begin_time, end_time):
    url = f"{settings.cms_host}/StandardApiAction_queryAlarmDetail.action?"
    print(url)
    params = {"jsession": jsession,
              "devIdno": reg_id,
              "begintime": begin_time,
              # "begintime": to_timestamp(begin_time),
              "endtime": end_time,
              # "endtime": to_timestamp(end_time),
              "armType": "19,20,69,70",
              }
    return requests.get(
        url,
        params=params
    )


@functions.cms_data_get_decorator()
def get_gps(jsession):
    response = requests.get(
        f"{settings.cms_host}/StandardApiAction_getDeviceStatus.action?",
        params={"jsession": jsession})
    return response


@functions.cms_data_get_decorator()
def get_device_track(jsession: str, device_id: str, start_time: str,
                     stop_time: str, page: int | None = None):
    params = {
        "jsession": jsession,
        "devIdno": device_id,
        "begintime": start_time,
        "endtime": stop_time,
    }
    if page is not None:
        params["currentPage"] = page  # только если есть

    url = f"{settings.cms_host}/StandardApiAction_queryTrackDetail.action"

    # Ретраи на обрывы/502-504; идем без keep-alive
    retry = Retry(
        total=5, connect=3, read=3,
        backoff_factor=0.5,
        status_forcelist=[502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess = requests.Session()
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)

    headers = {
        "User-Agent": "qt_pvp/1.0",
        "Connection": "close",  # отключаем keep-alive
    }

    try:
        response = sess.get(
            url,
            params=params,
            headers=headers,
            timeout=(5, 60)  # connect, read
        )
        response.raise_for_status()
        return response
    finally:
        sess.close()


@functions.cms_data_get_decorator()
def get_devices(jsessuibg):
    response = requests.get(
        f"{settings.cms_host}/StandardApiAction_queryUserVehicle.action?")


def get_device_status(jsession: str, device_id: str):
    response = requests.get(
        f"{settings.cms_host}/StandardApiAction_getDeviceStatus.action?",
        params={"jsession": jsession,
                "devIdno": device_id,
                })
    return response


def get_device_track_all_pages(jsession: str, device_id: str, start_time: str,
                               stop_time: str):
    total_pages = 2
    current_page = 1
    all_tracks = []
    while current_page < total_pages:
        tracks = get_device_track(jsession, device_id, start_time, stop_time,
                                  page=current_page)
        tracks_json = tracks.json()
        total_pages = tracks_json["pagination"]["totalPages"]
        current_page = tracks_json["pagination"]["currentPage"]
        all_tracks += tracks_json["tracks"]
        if current_page >= total_pages:
            break
    # logger.debug(f"Got tracks: {all_tracks}")
    return all_tracks


@functions.cms_data_get_decorator_async()
async def execute_download_task(jsession, download_task_url: str):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(download_task_url,
                                   params={"jsession": jsession}) as response:
                response.raise_for_status()
                return await response.json()
    except aiohttp.ClientError as e:
        logger.error(f"HTTP request failed: {e}")
        return None


async def wait_and_get_dwn_url(jsession, download_task_url):
    logger.info("Downloading...")
    while True:
        response_json = await execute_download_task(
            jsession=jsession,
            download_task_url=download_task_url)
        result = response_json["result"]
        if result == 11 and response_json["oldTaskAll"]["dph"]:
            logger.info(f"{response_json['oldTaskAll']['id']}. Download done!")
            logger.debug(
                f'Get path: {str(response_json["oldTaskAll"]["dph"])}')
            return response_json["oldTaskAll"]["dph"]
        else:
            time.sleep(1)


async def download_interest_videos(jsession, interest, chanel_id, reg_id, split=False):
    logger.info("Загружаем видео...")

    start_time_datetime = datetime.datetime.strptime(
        interest["start_time"], "%Y-%m-%d %H:%M:%S"
    )

    beg_sec = interest["beg_sec"]
    end_sec = interest["end_sec"]

    file_paths = await download_video(
        jsession=jsession,
        reg_id=reg_id,
        channel_id=chanel_id,
        year=start_time_datetime.year,
        month=start_time_datetime.month,
        day=start_time_datetime.day,
        start_sec=beg_sec,
        end_sec=end_sec,
    )

    if not file_paths:
        logger.warning(f"{reg_id}: Не удалось получить видеофайлы для интереса")
        return None

    interest["file_paths"] = file_paths
    return interest



async def get_frames(jsession, reg_id: str,
                     year: int, month: int, day: int,
                     start_sec: int, end_sec: int):
    channels = [0, 1, 2, 3]
    frames = []
    for channel_id in channels:
        try:
            videos_path = await download_video(jsession=jsession,
                                               reg_id=reg_id,
                                               channel_id=channel_id,
                                               year=year,
                                               month=month, day=day,
                                               start_sec=start_sec,
                                               end_sec=end_sec)
            logger.debug(f"Channel id {channel_id} - {videos_path}")
            if not videos_path:
                continue
            video_path = videos_path[0]

            # Попытка извлечь кадр
            frame_path = extract_first_frame(video_path)


            if not frame_path:
                logger.error(f"Не удалось получить кадр с канала {channel_id}")
                continue

            frames.append(frame_path)
        except Exception as e:
            logger.exception(f"Ошибка при обработке канала {channel_id}: {e}")
    return frames


def log_no_image_event(reg_id: str, frame_path: str, context: str = "unknown"):
    """
    Логирует событие создания заглушки в отдельный файл.

    :param reg_id: ID регистратора
    :param frame_path: Путь к заглушке
    :param context: Контекст - фото ДО, ПОСЛЕ и т.д.
    """
    NO_IMAGE_LOG_FILE = "no_image_events.log"

    os.makedirs(os.path.dirname(NO_IMAGE_LOG_FILE) or ".", exist_ok=True)
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"{now} | RegID: {reg_id} | Context: {context} | Placeholder: {frame_path}\n"

    with open(NO_IMAGE_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(log_line)

    logger.info(f"NO IMAGE событие залогировано: {log_line.strip()}")


def extract_first_frame(video_path: str,
                        output_dir: str = settings.FRAMES_TEMP_FOLDER,
                        max_retries: int = 3,
                        min_file_size_kb: int = 10,
                        allow_placeholder: bool = True):
    if not os.path.exists(video_path) or (
            os.path.getsize(video_path) / 1024) < min_file_size_kb:
        logger.error(f"Файл слишком маленький или не найден: {video_path}")
        log_no_image_event(reg_id="dummy", frame_path=video_path,
                           context="photo_before_after")
        return _create_placeholder_image(
            output_dir) if allow_placeholder else False

    cap = None
    for attempt in range(max_retries):
        cap = cv2.VideoCapture(video_path)
        if cap.isOpened():
            break
        logger.warning(
            f"Попытка {attempt + 1}: Не удалось открыть видео: {video_path}")
        time.sleep(1)

    if not cap or not cap.isOpened():
        logger.error(
            f"Не удалось открыть видео после {max_retries} попыток: {video_path}")
        return _create_placeholder_image(
            output_dir) if allow_placeholder else False

    os.makedirs(output_dir, exist_ok=True)

    filename = f"{uuid.uuid4().hex}.jpg"
    output_path = os.path.join(output_dir, filename)

    logger.debug(f"Пытаемся сохранить кадр в {output_path}")
    success, frame = cap.read()
    cap.release()

    if success and frame is not None:
        cv2.imwrite(output_path, frame)
        logger.info(f"Кадр успешно сохранён в: {output_path}")
        return output_path
    else:
        logger.warning("Не удалось прочитать кадр из видео.")
        return _create_placeholder_image(
            output_dir) if allow_placeholder else False


def _create_placeholder_image(output_dir: str):
    """Создаёт заглушку — чёрное изображение с надписью NO IMAGE"""
    os.makedirs(output_dir, exist_ok=True)

    filename = f"{uuid.uuid4().hex}_placeholder.jpg"
    output_path = os.path.join(output_dir, filename)

    # Создаём чёрное изображение
    img = np.zeros((720, 1280, 3), dtype=np.uint8)

    # Пишем текст "NO IMAGE"
    cv2.putText(img, "NO IMAGE", (400, 360),
                cv2.FONT_HERSHEY_SIMPLEX, 2, (255, 255, 255), 3, cv2.LINE_AA)

    cv2.imwrite(output_path, img)
    logger.warning(f"Создана заглушка: {output_path}")
    return output_path


async def download_video(jsession, reg_id: str, channel_id: int,
                         year: int, month: int, day: int,
                         start_sec: int, end_sec: int,
                         max_attempts: int = 8,
                         adjustment_step: int = 15):
    """
    Пытается получить список файлов для заданного временного окна.
    - Сетевые сбои/занятость CMS ретраятся ДЕКОРАТОРОМ бесконечно с бэкоффом.
    - Если устройство офлайн (result==32) — ждём и пробуем снова, attempts НЕ растёт.
    - Если ответ валидный, но файлов нет — расширяем интервал и считаем попытку.
    """
    file_paths: list[str] = []
    attempt = 0

    # границы суток (чтобы не вылезти)
    start_limit = 0
    end_limit = 24 * 60 * 60 - 1

    while True:
        logger.debug(f"Попытка {attempt + 1}: start_sec={start_sec}, end_sec={end_sec}")

        response = get_video(
            jsession=jsession,
            device_id=reg_id,
            chanel_id=channel_id,
            start_time_seconds=start_sec,
            end_time_seconds=end_sec,
            year=year,
            month=month,
            day=day
        )

        # декоратор уже гарантировал, что тут есть валидный JSON и 200 (или осмысленный ответ)
        try:
            response_json = response.json()
        except Exception as e:
            # избыточная защита; теоретически не должны сюда попадать
            logger.warning(f"Ошибка при парсинге JSON (post-decorator): {e}")
            await asyncio.sleep(2)
            continue

        logger.debug(f"Get video response: {response_json}, {response.status_code}")

        result = response_json.get("result")
        message = response_json.get("message", "")
        files = response_json.get("files") or []

        # 1) Устройство офлайн — просто ждём до успеха (attempt НЕ растёт)
        if result == 32 and "Device is not online" in message:
            logger.warning(f"Устройство {reg_id} не в сети. Ждём 5с и пробуем снова...")
            await asyncio.sleep(5)
            continue

        # 2) Файлы есть — запускаем выдачу URL-ов
        if files:
            for file in files:
                download_task_url = file["DownTaskUrl"]
                file_path = await wait_and_get_dwn_url(jsession=jsession, download_task_url=download_task_url)
                file_paths.append(file_path)
            return file_paths

        # 3) Валидный ответ, но файлов нет — расширяем окно и считаем попытку
        attempt += 1

        # Расширяем окно с ограничениями суток
        start_sec = max(start_limit, start_sec - adjustment_step)
        end_sec = min(end_limit, end_sec + adjustment_step)

        if attempt >= max_attempts:
            logger.warning(f"Файлы не найдены после {attempt} попыток (устройство в сети/ответ валиден).")
            return None



def send_cmsv6_message(dev_idno: str, jsession: str, text: str,
                       host: str = "82.146.45.88", port: int = 6603):
    """
    Отправляет текстовое сообщение на экран регистратора через CMSV6 API.

    :param dev_idno: ID регистратора (например, "018270348452")
    :param jsession: Ключ сессии CMSV6 (получается через login)
    :param text: Текст, который должен появиться на экране регистратора
    :param host: IP-адрес CMSV6 сервера
    :param port: Порт CMSV6 сервера (обычно 6603)
    """
    url = f"http://{host}:{port}/2/74"
    params = {
        "Command": "33536",
        "DevIDNO": dev_idno,
        "toMap": "1",
        "jsession": jsession
    }
    payload = {
        "Flag": 20,
        "TextInfo": text,
        "TextType": 1,
        "utf8": 1
    }

    response = requests.post(url, params=params, json=payload)

    try:
        result = response.json()
    except Exception:
        result = {"error": "Invalid response", "raw": response.text}

    return result


def get_dev_idno_by_plate(jsession: str, plate_number: str,
                          host: str = "82.146.45.88", port: int = 8080):
    """
    Получает DevIDNO по гос.номеру автомобиля через CMSV6 API.

    :param jsession: Ключ сессии CMSV6
    :param plate_number: Гос.номер машины (например, "А123ВС102")
    :param host: IP-адрес CMSV6 сервера
    :param port: Порт CMSV6 API (обычно 8080)
    :return: DevIDNO (str) или None
    """
    url = f"http://{host}:{port}/StandardApiAction_queryDevice.action"
    params = {"jsession": jsession}

    response = requests.get(url, params=params)
    try:
        print(response)
        data = response.json()
        print(data)
        devices = data.get("devices", [])
        for device in devices:
            print(device)
            if device.get("vehicleNumber", "").replace(" ",
                                                       "").upper() == plate_number.replace(
                " ", "").upper():
                return device.get("devIdno")
        return None
    except Exception:
        print("123")
        return None


# for interest in interests:
#    get_interest_download_path(jsession, interest)


# print(log_data)
# if res["result"] == 32:
#    pass
# print(res)
# files = res["files"]
# file = files[0]
# device_id = "104040"
# print(file["DownTaskUrl"])
# print("getting gps")
# track = gps.json()["status"]
# print(gps.json())
# tracks = get_device_track_all_pages(jsession=jsession,
# device_id=device_id,
# start_time="2025-02-05 15:00:00",
# stop_time="2025-02-05 16:00:00", )

# interests = functions.analyze_tracks_get_interests(tracks)
