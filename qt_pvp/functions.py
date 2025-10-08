from qt_pvp.logger import logger
from qt_pvp.data import settings
from qt_pvp.filelocker import FileLock, _load_states, _atomic_save_states, LOCK_PATH
from typing import List
import subprocess
import datetime
import zipfile
import ffmpeg
import shutil
import json
import uuid
import time
import os



def _default_new_reg_info(plate=None):
    last_upload = datetime.datetime.today() - datetime.timedelta(days=7)
    return {
        "ignore": False,
        "interests": [],
        "chanel_id": 0,
        "last_upload_time": last_upload.strftime("%Y-%m-%d %H:%M:%S"),
        "by_trigger": 1,
        "by_stops": 0,
        "by_door_limit_switch": 0,
        "by_lifting_limit_switch": 1,
        "continuous": 0,
        "euro_container_alarm": 4,
        "kgo_container_alarm": 3,
        "plate": plate,
    }

def rename_file_on_disk(path: str, new_name: str) -> str:
    """
    Переименовывает файл по указанному пути в новое имя.
    Возвращает новый путь.
    """
    directory = os.path.dirname(path)
    new_path = os.path.join(directory, new_name)
    os.rename(path, new_path)
    return new_path



def unzip_archives_in_directory(input_dir, output_dir):
    # Проверка существования входящей директории
    logger.debug(f'Распаковка {input_dir} в {output_dir}')
    if not os.path.exists(input_dir):
        logger.error(f'Директория {input_dir} не найдена')
        return
    # Получение списка всех файлов в input_dir
    files = os.listdir(input_dir)
    for file in files:
        logger.debug(f'Распаковка файла {file}...')
        # Проверяем, является ли файл архивом .zip
        if file.endswith('.zip'):
            zip_path = os.path.join(input_dir, file)
            # Определяем имя архива без расширения
            archive_name = os.path.splitext(file)[0]
            # Формируем путь для новой директории
            new_output_dir = os.path.join(output_dir, archive_name)
            if os.path.exists(new_output_dir):
                continue
            # Создаём новую директорию, если она не существует
            if not os.path.exists(new_output_dir):
                os.makedirs(new_output_dir)
            # Распаковка архива
            with zipfile.ZipFile(zip_path, 'r') as zf:
                zf.extractall(path=new_output_dir)
                logger.debug(
                    f'Файл {file} успешно распакован в {new_output_dir}.')
    logger.info(f'Распаковка {input_dir} в {output_dir} завершена.')


def split_time_range_to_dicts(start_time, end_time, interval):
    if isinstance(start_time, str): start_time = datetime.datetime.fromisoformat(start_time)
    if isinstance(end_time, str):   end_time   = datetime.datetime.fromisoformat(end_time)
    if start_time >= end_time: raise ValueError("...")
    result = []
    cur = start_time
    while cur < end_time:
        nxt = min(cur + interval, end_time)
        result.append({"time_start": cur, "time_end": nxt})
        cur = nxt
    return result



# qt_pvp/functions.py
def concatenate_videos(converted_files, output_abs_name):
    concat_candidates = []
    for f in converted_files:
        if not f:
            continue
        try:
            if os.path.isfile(f) and os.path.getsize(f) > 0:
                concat_candidates.append(f)
            else:
                logger.error(f"[CONCAT] Файл отсутствует или пустой: {f}")
        except OSError as e:
            logger.error(f"[CONCAT] Ошибка доступа к файлу {f}: {e}")

    if len(concat_candidates) == 0:
        raise FileNotFoundError("[CONCAT] Нет ни одного валидного входного файла — пропускаю интерес.")

    if len(concat_candidates) == 1:
        # вместо ffmpeg — просто копия единственного файла как итог
        src = concat_candidates[0]
        os.makedirs(os.path.dirname(output_abs_name), exist_ok=True)
        shutil.copyfile(src, output_abs_name)
        logger.debug(f"[CONCAT] Единственный файл — скопирован: {src} -> {output_abs_name}")
        #logger.debug(f"Удаляем исходный файл ({src})")
        #os.remove(src)
        return

    # стандартная concat через ffmpeg
    concat_list_path = os.path.join(
        os.path.dirname(output_abs_name),
        f"concat_list_{uuid.uuid4().hex}.txt"
    )
    logger.debug(f"[CONCAT] Конкатенация файлов {concat_candidates}")
    try:
        with open(concat_list_path, "w", encoding="utf-8") as f:
            for file in concat_candidates:
                f.write(f"file '{file}'\n")
        cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0",
               "-i", concat_list_path, "-c", "copy", output_abs_name]
        logger.debug(f"[CONCAT] Команда: {' '.join(cmd)}")
        # захватываем stderr для нормального логирования
        proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.debug(f"[CONCAT] Успех. Результат: {output_abs_name}")
    except subprocess.CalledProcessError as e:
        logger.error(f"[CONCAT] ffmpeg упал: {e.stderr or e.stdout}")
        raise
    finally:
        try:
            os.remove(concat_list_path)
        except OSError:
            pass



def convert_video_file(input_video_path: str, output_dir: str = None,
                       output_format: str = "mp4"):
    if not output_dir:
        logger.debug("Output dir for converted files is not specified. "
                     "Input file`s dir has been choosen.")
        output_dir = os.path.dirname(input_video_path)
    filename = os.path.basename(input_video_path)
    output_video_path = os.path.join(output_dir,
                                     filename + '.' + output_format)
    # Команда для конвертации через FFMPEG
    conversion_command = ['ffmpeg', '-y', '-i', input_video_path, '-c:v',
                          'libx264', '-crf', '23', '-preset', 'medium',
                          output_video_path]
    logger.debug(
        f"Команда на конвертацию {' '.join(conversion_command)}")
    try:
        subprocess.run(conversion_command, check=True)
    except subprocess.CalledProcessError:
        logger.critical("Ошибка конвертации!")
        return None
    return output_video_path


def _ensure_alarms_fields(regs: dict, reg_id: str = None) -> bool:
    changed = False
    target_ids = [reg_id] if reg_id else list(regs.keys())
    for rid in target_ids:
        reg = regs.get(rid) or {}
        if "euro_container_alarm" not in reg:
            reg["euro_container_alarm"] = 4
            changed = True
        regs[rid] = reg
    return changed


def ensure_alarms_structure_inplace(regs: dict, reg_id: str | None = None) -> bool:
    """
    НИЧЕГО НЕ ПИШЕТ В ФАЙЛ. Только правит regs in-place.
    Возвращает True, если структура была дополнена/исправлена.
    """
    changed = False
    if reg_id is None:
        for rid in list(regs.keys()):
            if _ensure_alarms_fields(regs, rid):
                changed = True
        return changed
    else:
        return _ensure_alarms_fields(regs, reg_id)


def save_new_interests(reg_id, interests):
    with FileLock(LOCK_PATH):
        states = _load_states()
        regs = states.setdefault("regs", {})
        if reg_id not in regs:
            regs[reg_id] = _default_new_reg_info()
        ensure_alarms_structure_inplace(regs, reg_id)
        regs[reg_id]["interests"] = interests  # <-- тут был баг: раньше писалось в "states"
        _atomic_save_states(states)

def _get_processed_set(reg_id: str) -> set[str]:
    with FileLock(LOCK_PATH):
        states = _load_states()
        reg = states.setdefault("regs", {}).setdefault(reg_id, _default_new_reg_info())
        ensure_alarms_structure_inplace(states["regs"], reg_id)
        processed = reg.get("processed_interests", [])
        return set(processed)

def _save_processed(reg_id: str, name: str, keep_last: int = 1000):
    with FileLock(LOCK_PATH):
        states = _load_states()
        regs = states.setdefault("regs", {})
        reg = regs.setdefault(reg_id, _default_new_reg_info())
        ensure_alarms_structure_inplace(regs, reg_id)
        arr = reg.get("processed_interests", [])
        if name not in arr:
            arr.append(name)
            # ограничим размер кольцевым буфером
            if len(arr) > keep_last:
                arr = arr[-keep_last:]
            reg["processed_interests"] = arr
        _atomic_save_states(states)

def filter_already_processed(reg_id: str, interests: list[dict]) -> list[dict]:
    done = _get_processed_set(reg_id)
    out = []
    for it in interests:
        nm = it.get("name")
        if nm in done:
            logger.info(f"[DEDUP] Пропуск уже обработанного интереса: {nm}")
            continue
        out.append(it)
    return out

def clean_interests(reg_id):
    with FileLock(LOCK_PATH):
        logger.debug("Cleaning interests in states.json")
        states = _load_states()
        regs = states.setdefault("regs", {})
        if reg_id not in regs:
            regs[reg_id] = _default_new_reg_info()
        ensure_alarms_structure_inplace(regs, reg_id)
        regs[reg_id]["interests"] = []
        _atomic_save_states(states)



def get_reg_info(reg_id: str):
    with FileLock(LOCK_PATH):
        states = _load_states()
        regs = states.setdefault("regs", {})
        created = False
        if reg_id not in regs:
            regs[reg_id] = _default_new_reg_info()
            created = True
        changed = ensure_alarms_structure_inplace(regs, reg_id) or created
        if changed:
            _atomic_save_states(states)
        return json.loads(json.dumps(regs[reg_id]))



def create_new_reg(reg_id, plate):
    with FileLock(LOCK_PATH):
        states = _load_states()
        regs = states.setdefault("regs", {})
        if reg_id in regs:
            return json.loads(json.dumps(regs[reg_id]))
        regs[reg_id] = _default_new_reg_info(plate=plate)
        # ensure — только in-place
        ensure_alarms_structure_inplace(regs, reg_id)
        _atomic_save_states(states)
        return json.loads(json.dumps(regs[reg_id]))



def get_reg_last_upload_time(reg_id):
    reg_info = get_reg_info(reg_id=reg_id)
    if not reg_info:
        reg_info = create_new_reg(reg_id)
    if not reg_info or "last_upload_time" not in reg_info.keys():
        return
    return reg_info["last_upload_time"]


def save_new_reg_last_upload_time(reg_id: str, timestamp: str):
    with FileLock(LOCK_PATH):
        states = _load_states()
        regs = states.setdefault("regs", {})
        if reg_id not in regs:
            regs[reg_id] = _default_new_reg_info()  # ← вместо create_new_reg(...)
        # ensure только in-place
        ensure_alarms_structure_inplace(regs, reg_id)
        regs[reg_id]["last_upload_time"] = timestamp
        _atomic_save_states(states)
    logger.info(f"{reg_id}. Обновлен `last_upload_time`: {timestamp}")

def video_remover_cycle():
    while True:
        all_videos = get_all_files(settings.INTERESTING_VIDEOS_FOLDER)
        for video_abs_name in all_videos:
            if check_if_file_old(video_abs_name):
                os.remove(video_abs_name)
        time.sleep(3600)


def get_all_files(files_dir):
    only_files = [os.path.join(files_dir, f) for f in os.listdir(files_dir)
                  if os.path.isfile(os.path.join(files_dir, f))]
    return only_files


def check_if_file_old(file_abs_path, old_time_days=60):
    ti_m = os.path.getmtime(file_abs_path)
    created_time = datetime.datetime.fromtimestamp(ti_m)
    if (datetime.datetime.now() - created_time).days >= old_time_days:
        return True


def get_video_info(file_path):
    """
    Получает информацию о видеофайле: формат и видеокодек.
    """
    try:
        probe = ffmpeg.probe(file_path)
        print(probe)
        format_name = probe['format']['format_name']
        video_stream = next((stream for stream in probe['streams'] if
                             stream['codec_type'] == 'video'), None)
        video_codec = video_stream['codec_name'] if video_stream else None
        return format_name, video_codec
    except ffmpeg.Error as e:
        logger.error(f"Ошибка при анализе файла {file_path}: {e.stderr}")
        return None, None


def convert_to_mp4_h264(input_file, output_file):
    """
    Конвертирует видео в MP4 с кодеком H.264.
    1) MP4, H.264 → копируем без изменений
    2) IFV-файл (метаданные битые) → обрабатываем как H.265, FPS=5
    3) MP4, H.265 → перекодируем в H.264
    """
    try:
        input_ext = os.path.splitext(input_file)[-1].lower()

        # 1) Если файл уже MP4 с H.264, просто копируем
        if input_ext == ".mp4":
            video_codec = get_video_codec(input_file)
            if video_codec == "h264":
                logger.info(
                    f"Файл {input_file} уже в формате MP4, H.264. Копируем без изменений.")
                shutil.copy(input_file, output_file)
                return

        # 2) Если файл содержит ".ifv" в названии, обрабатываем его как H.265, FPS=5
        if ".ifv" in input_file:
            logger.info(
                f"Файл {input_file} определен как IFV. Предполагаем кодек H.265, FPS=5.")

        # 3) Если кодек H.265 (HEVC) или мы обрабатывали IFV, перекодируем в H.264
        logger.info(f"Конвертируем {input_file} в MP4 (H.264).")
        ffmpeg.input(input_file, vcodec="hevc").output(output_file, vcodec="libx264", preset="medium").run(
            overwrite_output=True
        )

        logger.info(f"Файл успешно обработан: {output_file}")

    except ffmpeg.Error as e:
        logger.error(f"Ошибка при обработке файла {input_file}: {e.stderr}")


def get_video_codec(file_path):
    """
    Определяет видеокодек файла через ffmpeg.
    """
    try:
        probe = ffmpeg.probe(file_path)
        return probe["streams"][0]["codec_name"]
    except Exception as e:
        logger.warning(f"Не удалось определить кодек для {file_path}: {e}")
        return None  # Если не удалось определить, возвращаем None


def process_video_file(file_path, output_file_path):
    """
    Обрабатывает видеофайл: проверяет формат и кодек, при необходимости конвертирует.
    """
    # Получаем информацию о файле
    format_name, video_codec = get_video_info(file_path)
    if not format_name or not video_codec:
        logger.error(f"Не удалось получить информацию о файле: {file_path}")
        return file_path

    logger.info(f"Файл: {file_path}")
    logger.info(f"Формат: {format_name}")
    logger.info(f"Видеокодек: {video_codec}")

    if not settings.config.getboolean("Video", "convert_required"):
        logger.info("Конвертация отключена в конфиге, пропуск...")
        return file_path

    # Определяем, нужно ли конвертировать
    need_conversion = False
    if format_name != 'mp4':
        need_conversion = True
        logger.info(f"Файл не в формате MP4 ({format_name}). Требуется конвертация.")
    elif video_codec != 'h264':
        need_conversion = True
        logger.info(
            f"Файл в формате MP4, но кодек не H.264 ({video_codec}). Требуется конвертация.")
    else:
        logger.info(
            "Файл уже в формате MP4 с кодеком H.264. Конвертация не требуется.")

    # Конвертируем, если нужно
    if need_conversion:
        #output_file = os.path.splitext(file_path)[0] + "_converted.mp4"
        convert_to_mp4_h264(file_path, output_file_path)
        return output_file_path
    return file_path


def merge_overlapping_interests(interests: List[dict]) -> List[dict]:
    if not interests:
        return []

    # Сортируем по началу
    sorted_interests = sorted(interests, key=lambda x: x['beg_sec'])
    merged = []

    current = sorted_interests[0].copy()
    for next_interest in sorted_interests[1:]:
        # Пересекаются, если начало следующего раньше конца текущего
        if next_interest['beg_sec'] <= current['end_sec']:
            logger.info("Обнаружение пересечение интересов. Объединение...")
            # Объединяем интервалы
            current['beg_sec'] = min(current['beg_sec'], next_interest['beg_sec'])
            current['end_sec'] = max(current['end_sec'], next_interest['end_sec'])

            # Объединяем временные метки
            current['start_time'] = min(current['start_time'], next_interest['start_time'])
            current['end_time'] = max(current['end_time'], next_interest['end_time'])
            current['photo_before_timestamp'] = min(
                current.get('photo_before_timestamp', current['start_time']),
                next_interest.get('photo_before_timestamp', next_interest['start_time'])
            )
            current['photo_after_timestamp'] = max(
                current.get('photo_after_timestamp', current['end_time']),
                next_interest.get('photo_after_timestamp', next_interest['end_time'])
            )

            # Объединяем фото в секундах
            current['photo_before_sec'] = min(current.get('photo_before_sec', current['beg_sec']),
                                              next_interest.get('photo_before_sec', next_interest['beg_sec']))
            current['photo_after_sec'] = max(current.get('photo_after_sec', current['end_sec']),
                                             next_interest.get('photo_after_sec', next_interest['end_sec']))

            # Объединяем события переключателей
            if 'report' in current and 'report' in next_interest:
                current_switches = current['report'].get('switch_events', [])
                next_switches = next_interest['report'].get('switch_events', [])
                merged_switches = current_switches + next_switches
                merged_switches.sort(key=lambda x: x['datetime'])
                current['report']['switch_events'] = merged_switches
                current['report']['switches_amount'] = len(merged_switches)
        else:
            merged.append(current)
            current = next_interest.copy()

    merged.append(current)
    return merged
