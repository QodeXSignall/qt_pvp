from qt_pvp.logger import logger
from qt_pvp import settings
from qt_pvp.functions import get_reg_info
import datetime
import requests
import functools
import asyncio
import time

io_to_reg_map = {1: 20, 2: 21, 3: 22, 4: 23}


def int_to_32bit_binary(number):
    # Преобразуем число в 32-битное представление, учитывая знак
    binary_str = format(number & 0xFFFFFFFF, '032b')
    bits = [int(bit) for bit in binary_str]
    bits.reverse()
    return bits


def form_add_download_task_url(reg_id, start_timestamp, end_timestamp,
                               channel_id, reg_fph=None):
    req_url = f"{settings.add_download_task}?" \
              f"did={reg_id}" \
              f"&fbtm={start_timestamp}" \
              f"&fetm={end_timestamp}" \
              f"&chn={channel_id}" \
              f"&sbtm={datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}" \
              f"&dtp=2" \
              f"&ftp=2" \
              f"&vtp=0"
    return req_url


# f"&fph={reg_fph}" \


def analyze_s1(s1_int: int):
    bits_list = int_to_32bit_binary(s1_int)
    return {
        "acc_state": bits_list[1],
        "forward_state": bits_list[5],
        "static_state": bits_list[13],
        # "parked_acc_state": bits_list[19],
        "io1": bits_list[20],
        "io2": bits_list[21],
        "io3": bits_list[22],
        "io4": bits_list[23],
        "io5": bits_list[24],
    }


def get_interest_from_track(track, start_time: str, end_time: str,
                            photo_before_timestamp: str = None,
                            photo_after_timestamp: str = None):
    start_time_datetime = datetime.datetime.strptime(start_time,
                                                     "%Y-%m-%d %H:%M:%S")
    end_time_datetime = datetime.datetime.strptime(end_time,
                                                   "%Y-%m-%d %H:%M:%S")
    photo_before_datetime = datetime.datetime.strptime(photo_before_timestamp,
                                                       "%Y-%m-%d %H:%M:%S")
    photo_after_datetime = datetime.datetime.strptime(photo_after_timestamp,
                                                      "%Y-%m-%d %H:%M:%S")
    return {
        "name": f"{track['vid']}_"
                f"{start_time_datetime.year}."
                f"{start_time_datetime.month:02d}."
                f"{start_time_datetime.day:02d} "
                f"{start_time_datetime.hour:02d}."
                f"{start_time_datetime.minute:02d}."
                f"{start_time_datetime.second:02d}-"
                f"{end_time_datetime.hour:02d}."
                f"{end_time_datetime.minute:02d}."
                f"{end_time_datetime.second:02d}",
        "beg_sec": seconds_since_midnight(start_time_datetime),
        "end_sec": seconds_since_midnight(end_time_datetime),
        "year": start_time_datetime.year,
        "month": start_time_datetime.month,
        "day": start_time_datetime.day,
        "start_time": start_time,
        "end_time": end_time,
        "car_number": track["vid"],
        "photo_before_timestamp": photo_before_timestamp,
        "photo_after_timestamp": photo_after_timestamp,
        "photo_before_sec": seconds_since_midnight(photo_before_datetime),
        "photo_after_sec": seconds_since_midnight(photo_after_datetime),
    }


def find_stops(tracks):
    stop_intervals = []
    start_time = None
    gt_time = None
    logger.info("Getting interests by stops")

    for track in tracks:
        speed = track.get("sp", 0)
        gt_time = track.get("gt")

        if gt_time:
            current_time = gt_time
        else:
            continue

        if speed <= 50:
            if start_time is None:
                start_time = current_time
        else:
            if start_time is not None:
                stop_intervals.append(
                    get_interest_from_track(track, start_time, current_time))
                start_time = None  # Сбрасываем start_time после добавления

    if start_time is not None and gt_time:
        stop_intervals.append(
            get_interest_from_track(tracks[-1], start_time, gt_time))

    # Возвращаем список без первого и последнего элемента
    return stop_intervals[1:-1] if len(stop_intervals) > 2 else []


def find_interests_by_lifting_switches(tracks, sec_before=30, sec_after=30, start_tracks_search_time=None, reg_id=None):
    loading_intervals = []
    i = 0
    first_interest = True   # Используем в случаях, когда для первого интереса не найдена начальная остановка в заданных треках
    reg_cfg = get_reg_info(reg_id) if reg_id else None
    try:
        euro_alarm_cfg = int((reg_cfg or {}).get("euro_container_alarm", 4))
    except Exception:
        euro_alarm_cfg = 4
    try:
        kgo_alarm_cfg_val = (reg_cfg or {}).get("kgo_container_alarm")
        kgo_alarm_cfg = int(kgo_alarm_cfg_val) if kgo_alarm_cfg_val is not None else None
    except Exception:
        kgo_alarm_cfg = None

    euro_bit_idx = io_to_reg_map.get(euro_alarm_cfg, 23)
    kgo_bit_idx = io_to_reg_map.get(kgo_alarm_cfg, None) if kgo_alarm_cfg is not None else None
    while i < len(tracks):
        track = tracks[i]
        s1 = track.get("s1")
        timestamp = track.get("gt")
        s1_int = int(s1)

        bits = list(bin(s1_int & 0xFFFFFFFF)[2:].zfill(32))
        bits.reverse()
        print(timestamp, bits)
        i += 1
        min_speed_for_switch_detect = settings.config.getint("Interests", "MIN_SPEED_FOR_SWITCH_DETECT")
        euro_on = bits[euro_bit_idx] == '1'
        kgo_on = (kgo_bit_idx is not None) and (bits[kgo_bit_idx] == '1')
        if euro_on or kgo_on:
            cargo_type = "КГО" if kgo_on else "Контейнер"
            logger.info(f"[SWITCH] Срабатывание концевика в {timestamp}, EuroIO(bit {euro_bit_idx})={bits[euro_bit_idx]}" + (f", KGOIO(bit {kgo_bit_idx})={bits[kgo_bit_idx]}" if kgo_bit_idx is not None else ""))
            if track.get("sp") > min_speed_for_switch_detect:
                logger.debug(f"[SWITCH] Игнор: скорость {track.get('sp')} > {min_speed_for_switch_detect}")
                continue

            if kgo_on:
                sec_before = 120

            logger.debug(f"[SWITCH] Принято: {cargo_type} в {timestamp}")
            switch_events = []
            current_dt = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            #time_30_before_dt = current_dt - datetime.timedelta(seconds=sec_before)

            if i >= len(tracks):
                logger.warning(f"[SWITCH] Индекс {i} вне диапазона треков. Прерывание.")
                break

            # Находим время для фото ДО (Последнее время в окне стабильных остановок)
            time_before = find_first_stable_stop(tracks, i, current_dt, settings, first_interest, start_tracks_search_time)
            if not time_before:
                logger.warning(f"[BEFORE] Не найдена остановка до сработки концевика в {timestamp}")
                if first_interest:
                    logger.warning("[BEFORE] Это был первый интерес, возвращаемся для получения дополнительных треков")
                    return {"error": "No stop before switch found for first interest"}

            lifting_end_idx = i
            last_switch_index = i

            if euro_on:
                switch_events.append({"datetime": timestamp, "switch": euro_bit_idx})
            if kgo_on:
                switch_events.append({"datetime": timestamp, "switch": kgo_bit_idx})

            # В этом цикле мы перебираем треки и ищем трек, когда погрузка закочена (по скорости и концевику)
            logger.debug("Теперь ищем когда машина поехала после погрузки.")

            move_started_at = None
            while lifting_end_idx + 1 < len(tracks):
                next_track = tracks[lifting_end_idx + 1]
                next_s1 = next_track.get("s1")
                next_spd = next_track.get("sp") or 0
                try:
                    next_s1_int = int(next_s1)
                except (ValueError, TypeError):
                    break

                next_bits = list(bin(next_s1_int & 0xFFFFFFFF)[2:].zfill(32))
                next_bits.reverse()

                logger.debug(f"Ищем момент когда машина поехала после погрузки. {next_track.get('gt')}, EuroIO(bit {euro_bit_idx})={next_bits[euro_bit_idx]}" + (f", KGOIO(bit {kgo_bit_idx})={next_bits[kgo_bit_idx]}" if kgo_bit_idx is not None else "") + f", sp={next_spd}")

                min_move_speed = settings.config.getint("Interests", "MIN_MOVE_SPEED")
                min_move_duration = settings.config.getint("Interests", "MIN_MOVE_DURATION_SEC")
                sw_time = next_track.get("gt")
                ts = _to_ts(sw_time)

                # 1) если сработал концевик — фиксируем и продолжаем расширять окно
                if next_bits[euro_bit_idx] == '1' or (kgo_bit_idx is not None and next_bits[kgo_bit_idx] == '1'):
                    lifting_end_idx += 1
                    if next_bits[euro_bit_idx] == '1':
                        switch_events.append({"datetime": sw_time, "switch": euro_bit_idx})
                    if kgo_bit_idx is not None and next_bits[kgo_bit_idx] == '1':
                        switch_events.append({"datetime": sw_time, "switch": kgo_bit_idx})
                    last_switch_index = lifting_end_idx

                    # концевики нас не выводят из цикла, просто идём дальше
                    # но при этом сбрасывать/ставить движение не нужно
                    # (оставляем move_started_at как есть)

                # 2) если скорость низкая — расширяем окно, но сбрасываем накопление «движения»
                elif next_spd < min_move_speed:
                    lifting_end_idx += 1
                    move_started_at = None  # потеряли устойчивость — начинаем отсчёт заново

                # 3) скорость выше порога — проверяем длительность устойчивого движения
                else:
                    # начинаем отсчёт, если ещё не начат
                    if move_started_at is None:
                        move_started_at = ts

                    # расширяем окно на этой точке в любом случае
                    lifting_end_idx += 1

                    # если длительность непрерывного движения достигла порога — выходим
                    if (ts - move_started_at) >= min_move_duration:
                        break
            time_after, last_stop_idx = find_stop_after_lifting(tracks, last_switch_index + 1, settings, logger)

            if not time_after:
                time_after = fallback_photo_after_time(tracks, last_switch_index, settings, logger)
                if not time_after:
                    i = lifting_end_idx + 1
                    continue

            if last_stop_idx is None:
                logger.warning(f"[AFTER] Нет last_stop_idx — остановка не найдена после {timestamp}")
                i = lifting_end_idx + 1
                continue

            # Применяем сдвиг - фото ПОСЛЕ за несколько секунд до движения
            raw_time_after = datetime.datetime.strptime(time_after, "%Y-%m-%d %H:%M:%S")
            adjusted_time_after = raw_time_after - datetime.timedelta(
                seconds=settings.config.getint("Interests", "PHOTO_AFTER_SHIFT_SEC"))
            time_after = adjusted_time_after.strftime("%Y-%m-%d %H:%M:%S")

            last_alarm_dt = datetime.datetime.strptime(tracks[last_switch_index].get("gt"), "%Y-%m-%d %H:%M:%S")
            time_30_after_dt = last_alarm_dt + datetime.timedelta(seconds=sec_after)
            time_30_after = time_30_after_dt.strftime("%Y-%m-%d %H:%M:%S")

            if time_before and time_after:
                logger.info(f"[INTEREST] Интерес от {time_before} до {time_after}")
                interval = get_interest_from_track(
                    tracks[-1],
                    start_time=time_before,
                    #start_time=time_30_before_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    #end_time=time_after,
                    end_time=time_30_after,
                    photo_before_timestamp=time_before,
                    photo_after_timestamp=time_after
                )
                interval["report"] = {
                    "cargo_type": cargo_type,
                    "geo": track["ps"],
                    "switches_amount": len(switch_events),
                    "switch_events": switch_events
                }
                loading_intervals.append(interval)
                first_interest = False
            else:
                logger.info(f"[SKIP] Пропуск: нет {'time_before' if not time_before else ''}{' и ' if not time_before and not time_after else ''}{'time_after' if not time_after else ''}")

            i = lifting_end_idx + 1
        else:
            i += 1

    return {"interests": loading_intervals}


def _to_ts(gt):
    """Универсально переводим поле времени в timestamp (сек)."""
    if isinstance(gt, (int, float)):
        return float(gt)
    # пробуем ISO8601
    from datetime import datetime
    try:
        return datetime.fromisoformat(gt).timestamp()
    except Exception:
        # запасной формат "YYYY-mm-dd HH:MM:SS"
        return datetime.strptime(gt, "%Y-%m-%d %H:%M:%S").timestamp()


def find_stop_after_lifting(tracks, start_idx, settings, logger=None):
    # В этом блоке мы ищем время для фото ПОСЛЕ погрузки — после последнего срабатывания концевика
    stop_count = 0          # Количество точек с низкой скоростью (стоп)
    move_count = 0          # Количество точек подряд с высокой скоростью (движение)
    last_stop_idx = None    # Индекс последней точки с "настоящей" остановкой

    # Читаем пороги из настроек
    min_stop_speed = settings.config.getint("Interests", "MIN_STOP_SPEED")
    min_stop_duration = settings.config.getint("Interests", "MIN_STOP_DURATION_SEC")
    min_move_speed = settings.config.getint("Interests", "MIN_MOVE_SPEED")
    min_move_duration = settings.config.getint("Interests", "MIN_MOVE_DURATION_SEC")

    # Начинаем проходить треки сразу после последнего концевика
    k = start_idx
    while k < len(tracks):
        spd = tracks[k].get("sp") or 0  # Текущая скорость

        # Если объект почти стоит — возможно, началась остановка
        if int(spd) <= min_stop_speed:
            stop_count += 1
            move_count = 0
            last_stop_idx = k  # Сохраняем индекс этой потенциальной остановки

        # Если была остановка и теперь пошло стабильное движение — считаем, что остановка завершена
        elif stop_count >= min_stop_duration and int(spd) >= min_move_speed:
            move_count += 1
            # Подтверждаем, что было и стабильное движение
            if move_count >= min_move_duration and last_stop_idx is not None:
                if logger:
                    logger.debug(f"[PHOTO AFTER] Найдена стабильная остановка на idx={last_stop_idx}, gt={tracks[last_stop_idx].get('gt')}")
                return tracks[last_stop_idx].get("gt"), last_stop_idx  # Возвращаем и время, и индекс

        # Иначе — сбрасываем всё, потому что последовательность нарушена
        else:
            if logger:
                logger.debug(f"[PHOTO AFTER] Сброс счётчиков на idx={k} (spd={spd}, stop={stop_count}, move={move_count})")
            stop_count = 0
            move_count = 0

        k += 1

    # Если цикл прошёл до конца и мы так и не нашли момент — логируем это
    if logger:
        logger.warning(f"[PHOTO AFTER] Не удалось найти стабильную остановку после lifting (start_idx={start_idx})")
    return None, None


def fallback_photo_after_time(tracks, last_switch_index, settings, logger=None):
    """
    Страховочный механизм на случай, если не удалось найти стабильную остановку.
    Если с момента последнего срабатывания концевика прошло достаточно времени,
    то возвращаем время после как last_switch_time + 60 сек.
    """
    last_switch_time = datetime.datetime.strptime(tracks[last_switch_index]['gt'], "%Y-%m-%d %H:%M:%S")
    now = datetime.datetime.now()
    max_wait_sec = settings.config.getint("Interests", "MAX_WAIT_TIME_MINUTES") * 60

    if (now - last_switch_time).total_seconds() > max_wait_sec:
        fallback_time = last_switch_time + datetime.timedelta(seconds=60)
        if logger:
            logger.warning(
                f"[AFTER-FALLBACK] Используем страховку: прошло >{max_wait_sec} сек, берём last_switch_time + 60 сек => {fallback_time}"
            )
        return fallback_time.strftime("%Y-%m-%d %H:%M:%S")

    if logger:
        logger.warning(
            f"[AFTER-FALLBACK] Не прошло достаточно времени с момента срабатывания ({last_switch_time}), интерес отклонён"
        )
    return None



def find_first_stable_stop(
    tracks,
    start_index,
    current_dt,
    settings,
    first_interest=False,
    start_tracks_search_time=None,  # оставляем для совместимости
):
    logger.debug("Ищем движение и остановку до первого срабатывания концевика")

    cutoff_time = current_dt - datetime.timedelta(
        seconds=settings.config.getint("Interests", "MAX_LOOKBACK_SECONDS"))
    min_stop_speed = settings.config.getint("Interests", "MIN_STOP_SPEED")
    min_stop_duration_sec = settings.config.getint("Interests", "MIN_STOP_DURATION_SEC")

    stop_start_idx = None
    stop_end_idx = None  # край «позже» в серии
    j = start_index

    def ts(idx: int) -> datetime.datetime:
        return datetime.datetime.strptime(tracks[idx]["gt"], "%Y-%m-%d %H:%M:%S")

    while j >= 0:
        point_time = ts(j)
        spd = int(tracks[j].get("sp") or 0)

        logger.debug(
            f"[СКАНИРОВАНИЕ] j={j}, время={point_time}, скорость={spd}, "
            f"серия={None if stop_start_idx is None else (stop_start_idx, stop_end_idx)}"
        )

        if point_time < cutoff_time:
            logger.debug(f"[ОБРЫВ] Точка {point_time} за пределами окна {cutoff_time}")
            break

        if spd <= min_stop_speed:
            if stop_start_idx is None:
                stop_start_idx = j
                stop_end_idx = j
            else:
                stop_start_idx = j  # двигаем начало серии назад
            logger.debug(f"[ОСТАНОВКА] скорость={spd} <= {min_stop_speed}, серия=({stop_start_idx}->{stop_end_idx})")
        else:
            # серия закончилась — оцениваем длительность
            if stop_start_idx is not None:
                dur = (ts(stop_end_idx) - ts(stop_start_idx)).total_seconds()
                if dur >= min_stop_duration_sec:
                    logger.debug(
                        f"[ДВИЖЕНИЕ ДО ОСТАНОВКИ] Найдено. Длительность {dur:.0f} сек, "
                        f"начало {tracks[stop_start_idx]['gt']}"
                    )
                    return tracks[stop_start_idx]["gt"]
            # сбрасываем серию
            stop_start_idx = None
            stop_end_idx = None

        j -= 1

    # Выход из цикла: либо дошли до начала массива, либо упёрлись в cutoff
    if stop_start_idx is not None:
        dur = (ts(stop_end_idx) - ts(stop_start_idx)).total_seconds()
        if dur >= min_stop_duration_sec:
            first_track_dt = ts(0)
            # Если это первый интерес и серия начинается ровно с нулевого индекса,
            # и мы ещё не вышли за cutoff — просим догрузить (вернём None).
            if first_interest and stop_start_idx == 0 and first_track_dt >= cutoff_time:
                logger.debug(
                    "[ДОГРУЗКА] Серия достаточна, но упёрлись в начало куска и ещё не прошли cutoff — нужна догрузка"
                )
                return None
            logger.debug(f"[ФИНАЛ] Длительность {dur:.0f} сек, начало {tracks[stop_start_idx]['gt']}")
            return tracks[stop_start_idx]["gt"]

    logger.warning("[ОСТАНОВКА НЕ НАЙДЕНА]")
    return None


def find_first_stable_stop_depr(tracks, start_index, current_dt, settings):
    logger.debug("Анализ трека")
    cutoff_time = current_dt - datetime.timedelta(
        seconds=settings.config.getint("Interests", "MAX_LOOKBACK_SECONDS"))
    min_stop_speed = settings.config.getint("Interests", "MIN_STOP_SPEED")
    min_stop_duration = settings.config.getint("Interests",
                                               "MIN_STOP_DURATION_SEC")
    min_distance_from_event = 20  # секунд до концевика

    stop_start_idx = None
    stop_end_idx = None
    stop_count = 0

    j = start_index
    while j >= 0:
        track = tracks[j]
        point_time = datetime.datetime.strptime(track.get("gt"),
                                                "%Y-%m-%d %H:%M:%S")
        spd = track.get("sp") or 0

        logger.debug(
            f"[СКАНИРОВАНИЕ] j={j}, время={point_time}, скорость={spd}, текущая_длина_остановки={stop_count}")

        if int(spd) <= min_stop_speed:
            stop_count += 1
            logger.debug(
                f"[ОСТАНОВКА] скорость={spd} <= {min_stop_speed}, длина серии={stop_count}")
            if stop_end_idx is None:
                stop_end_idx = j
            stop_start_idx = j
        else:
            if stop_count >= min_stop_duration and stop_end_idx is not None:
                end_time = datetime.datetime.strptime(track['gt'],
                                                      "%Y-%m-%d %H:%M:%S")  # end_time - это время первого трека во время остановки
                delta_sec = (current_dt - end_time).total_seconds()
                logger.debug(
                    f"[КАНДИДАТ] остановка длиной {stop_count} сек, конец={end_time}, событие={current_dt}, разница={delta_sec} сек"
                )
                if delta_sec >= min_distance_from_event:
                    logger.debug(
                        f"[ОСТАНОВКА ПОДТВЕРЖДЕНА] c {tracks[stop_start_idx]['gt']} по {tracks[stop_end_idx]['gt']}")
                    return tracks[stop_start_idx].get("gt")
                else:
                    logger.debug(
                        f"[ОТКЛОНЕНО] слишком близко к событию: {delta_sec} сек < {min_distance_from_event} сек"
                    )

            stop_start_idx = None
            stop_end_idx = None
            stop_count = 0

        j -= 1

    # Проверка последней серии, если цикл закончился
    logger.debug(
        f"Цикл закончился. Stop_start_idx - {stop_start_idx}, stop_end_idx - {stop_end_idx}")
    if stop_count >= min_stop_duration and stop_end_idx is not None:
        end_time = datetime.datetime.strptime(tracks[stop_start_idx]['gt'],
                                              "%Y-%m-%d %H:%M:%S")
        delta_sec = (current_dt - end_time).total_seconds()
        logger.debug(
            f"[ФИНАЛЬНАЯ ПРОВЕРКА] Последняя серия: разница={delta_sec} сек, "
            f"с {tracks[stop_start_idx]['gt']} по {tracks[stop_end_idx]['gt']}"
        )
        if delta_sec >= min_distance_from_event:
            logger.debug(
                f"[ФИНАЛЬНАЯ ОСТАНОВКА ПРИНЯТА] с {tracks[stop_start_idx]['gt']} по {tracks[stop_end_idx]['gt']}")
            return tracks[stop_start_idx].get("gt")
        else:
            logger.debug(
                f"[ФИНАЛЬНАЯ ОСТАНОВКА ОТКЛОНЕНА] слишком близко: {delta_sec} сек < {min_distance_from_event} сек"
            )

    logger.warning("[ОСТАНОВКА НЕ НАЙДЕНА]")
    return None


def extract_before_after_segments(tracks, first_switch_index,
                                  last_switch_index, sec_before=30,
                                  sec_after=30):
    first_switch_time = parse_time(tracks[first_switch_index].get("gt"))
    last_switch_time = parse_time(tracks[last_switch_index].get("gt"))

    photo_before_start_time = first_switch_time - datetime.timedelta(
        seconds=sec_before)
    photo_after_start_time = last_switch_time + datetime.timedelta(seconds=5)
    photo_after_end_time = last_switch_time + datetime.timedelta(
        seconds=sec_after)

    tracks_before = [t for t in tracks if
                     photo_before_start_time <= parse_time(
                         t.get("gt")) < first_switch_time]
    tracks_after = [t for t in tracks if photo_after_start_time <= parse_time(
        t.get("gt")) <= photo_after_end_time]

    return tracks_before, tracks_after


def find_photo_before_timestamp(tracks_before_lifting, stop_speed=2, min_stop_points=3):
    count = 0
    for i, track in enumerate(tracks_before_lifting):
        spd = track.get("sp") or 0
        if spd <= stop_speed:
            count += 1
            if count >= min_stop_points:
                return track.get("gt")  # ← здесь просто возвращаем текущий gt
        else:
            count = 0
    return None



def find_photo_after_timestamp(tracks_after_lifting, stop_speed=2,
                               move_speed=10, min_stop_points=3,
                               min_move_points=3):
    stop_count = 0
    move_count = 0
    last_stop_idx = None
    for i, track in enumerate(tracks_after_lifting):
        spd = track.get("sp") or 0
        if spd <= stop_speed:
            stop_count += 1
            move_count = 0
            last_stop_idx = i
        elif stop_count >= min_stop_points and spd >= move_speed:
            move_count += 1
            if move_count >= min_move_points and last_stop_idx is not None:
                return tracks_after_lifting[last_stop_idx].get("gt")
        else:
            stop_count = 0
            move_count = 0
    return None


def parse_time(gt_str):
    return datetime.datetime.strptime(gt_str, "%Y-%m-%d %H:%M:%S")


def find_by_lifting_switches_depr(tracks, sec_before=30, sec_after=30):
    loading_intervals = []
    start_time = None
    last_alarm_time = None
    logger.info(
        "Анализ треков на остановку по концевикам подъемного механизма")
    for track in tracks:
        speed = track.get("sp", 0)  # Скорость машины
        s1_analyze = analyze_s1(track["s1"])
        switch = s1_analyze["io3"] or s1_analyze["io4"]
        current_time = datetime.datetime.strptime(track.get("gt"),
                                                  "%Y-%m-%d %H:%M:%S")  # Время события

        # Если сработал концевик (alarm 3 или alarm 4)
        if switch:
            if start_time is None:
                # Начало - 30 секунд до первой сработки
                start_time = current_time - datetime.timedelta(
                    seconds=sec_before)

            # Запоминаем последнюю сработку концевика
            last_alarm_time = current_time

        # Если машина поехала (speed > 0) и ранее была фиксация загрузки
        if speed > 0 and start_time:
            # Завершаем текущий интервал загрузки
            end_time = last_alarm_time + datetime.timedelta(seconds=sec_after)
            loading_intervals.append(
                get_interest_from_track(
                    track,
                    start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    end_time.strftime("%Y-%m-%d %H:%M:%S"))
            )

            # Сбрасываем переменные для нового интервала
            start_time = None
            last_alarm_time = None

    # Добавляем последний интервал, если машина так и не поехала
    if start_time and last_alarm_time:
        end_time = last_alarm_time + datetime.timedelta(seconds=30)
        loading_intervals.append(
            get_interest_from_track(
                tracks[-1],
                start_time.strftime("%Y-%m-%d %H:%M:%S"),
                end_time.strftime("%Y-%m-%d %H:%M:%S"))
        )

    return loading_intervals


def analyze_tracks_get_interests(tracks, by_stops=False,
                                 continuous=False,
                                 by_lifting_limit_switch=False,
                                 start_tracks_search_time=None,
                                 reg_id=None):
    # was_stop = None
    interests = []
    if by_stops:
        interests = find_stops(tracks)
        return interests[1:-1] if len(interests) > 2 else []
    elif by_lifting_limit_switch:
        interests = find_interests_by_lifting_switches(tracks, start_tracks_search_time=start_tracks_search_time, reg_id=reg_id)
        return interests
    elif continuous:
        interests = get_interest_from_track(
            tracks[-1], tracks[0]["gt"], tracks[-1]["gt"])
    if "interests" in interests:
        logger.debug(f"Get interests: {interests['interests']}")
    return interests


def split_time(start_time, end_time, split=30):
    # Проверка, чтобы начало было меньше конца
    if start_time >= end_time:
        return []
    intervals = []
    current_time = start_time
    while current_time + split < end_time:
        intervals.append((current_time, current_time + split))
        current_time += split
    # Добавляем последний неполный интервал, если он есть
    if current_time <= end_time:
        intervals.append((current_time, end_time))
    return intervals


def seconds_since_midnight(dt: datetime.datetime) -> int:
    midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    delta = dt - midnight
    return int(delta.total_seconds())


def cms_data_get_decorator_async(max_retries=3, delay=1):
    """
    Декоратор для повторного выполнения запросов к CMS серверу в случае ошибок.
    :param max_retries: Максимальное количество попыток.
    :param delay: Задержка между попытками (в секундах).
    """

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    # Выполняем асинхронную функцию
                    result = await func(*args, **kwargs)
                    # Проверяем ответ (предполагаем, что ответ — это JSON)
                    if isinstance(result, dict) and result.get(
                            "result") == 24:
                        raise ValueError("Invalid response from CMS server")

                    # Если ответ корректен, возвращаем его
                    return result
                except (ValueError, Exception) as e:
                    retries += 1
                    logger.warning(
                        f"Attempt {retries} failed: {e}. Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)

            # Если все попытки исчерпаны, вызываем исключение
            raise Exception(f"Failed after {max_retries} retries")

        return wrapper

    return decorator


def cms_data_get_decorator(tag='execute func'):
    def decorator(func):
        def wrapper(*args, **kwargs):
            backoff = 1.0  # сек
            while True:
                try:
                    response = func(*args, **kwargs)

                    # если HTTP не 200 — тоже ретраим
                    if response.status_code != 200:
                        logger.warning(f"CMS HTTP {response.status_code}; retry in {backoff:.1f}s")
                        time.sleep(backoff)
                        backoff = min(backoff * 1.5, 10.0)
                        continue

                    # попытка распарсить JSON
                    try:
                        data = response.json()
                    except Exception as je:
                        logger.warning(f"CMS JSON parse error: {je}; retry in {backoff:.1f}s")
                        time.sleep(backoff)
                        backoff = min(backoff * 1.5, 10.0)
                        continue

                    # Специфичные коды CMS
                    result = data.get("result")
                    # 24 — «busy/processing» у части прошивок — просто ждём ещё
                    if result == 24:
                        logger.debug(f"CMS busy (24); retry in {backoff:.1f}s")
                        time.sleep(backoff)
                        backoff = min(backoff * 1.5, 10.0)
                        continue

                    # Успех или осмысленный ответ — отдаём наверх;
                    # пусть уже вызывающая логика решает, что делать с result=0 и т.п.
                    return response

                except (requests.exceptions.ReadTimeout,
                        requests.exceptions.ConnectTimeout,
                        requests.exceptions.ConnectionError) as err:
                    logger.warning(f"Connection problem with CMS: {err}; retry in {backoff:.1f}s")
                    time.sleep(backoff)
                    backoff = min(backoff * 1.5, 10.0)

        return wrapper
    return decorator


@cms_data_get_decorator()
def get_mdvr_by_car_number_from_cms(jsession, car_number=None):
    response = requests.get(f"{settings.cms_host}/"
                            f"StandardApiAction_getDeviceByVehicle.action?",
                            params={'jsession': jsession,
                                    "vehiIdno": car_number})
    return response
