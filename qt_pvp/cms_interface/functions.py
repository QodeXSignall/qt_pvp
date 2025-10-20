from typing import Optional, Dict, Any, List, Tuple
from pygments.lexers import load_lexer_from_file
from qt_pvp.functions import get_reg_info
from qt_pvp.logger import logger
from qt_pvp.data import settings
from bisect import bisect_left
import datetime
import functools
import asyncio
import httpx

io_to_reg_map = {1: 20, 2: 21, 3: 22, 4: 23}


class LoadingInProgress(RuntimeError):
    """ Погрузка еще в процессе, прерываем поиск интересов """
    pass

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
        "reg_id": track['vid'],
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


def _safe_fromtimestamp_sec(ts_sec: float) -> datetime:
    # на входе секунды (float/int)
    dt_utc = datetime.datetime.fromtimestamp(ts_sec, tz=datetime.timezone.utc)
    return dt_utc.astimezone().replace(tzinfo=None)


def _parse_alarm_time(a: Dict[str, Any], key_ms: str, key_str: str) -> Tuple[datetime.datetime | None, str | None]:
    """
    Пытается достать время из ms-поля (UTC ms), если нет — из строкового поля '%Y-%m-%d %H:%M:%S'.
    Возвращает (dt_local_naive, formatted_str) или (None, None).
    """
    ms = a.get(key_ms)
    if isinstance(ms, (int, float)):
        try:
            dt_local = _safe_fromtimestamp_sec(ms / 1000.0)
            return dt_local, dt_local.strftime(settings.TIME_FMT)
        except Exception:
            pass

    s = a.get(key_str)
    if isinstance(s, str) and s.strip():
        try:
            # строки обычно уже в локальном формате без TZ
            dt_local = datetime.datetime.strptime(s.strip(), settings.TIME_FMT)
            return dt_local, s.strip()
        except Exception:
            pass

    return None, None

def _atp_to_io_index(atp_str: Optional[str]) -> Optional[int]:
    # Пример: atp=22, atpStr='IO_4报警' → вернём 4
    if atp_str and "IO_" in atp_str:
        try:
            return int(atp_str.split("IO_")[1].split("报警")[0])
        except Exception:
            pass
    # если строка не пришла — можно маппить по числам, если знаешь соответствие
    return None

def _resolve_cluster_cargo(types: set[str]) -> str:
    # приоритет: kgo > euro > unknown
    if "kgo" in types:
        return "kgo"
    if "euro" in types:
        return "euro"
    return "unknown"

def _dedupe_sw(events: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for e in events:
        key = (e.get("datetime"), e.get("switch"))
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out

def _cluster_merge_stationary(norm: list[dict], min_stop_kmh: float, merge_gap_sec: int) -> list[dict]:
    """
    Сливает подряд идущие алармы (ЛЮБОГО IO) в один кластер, если:
      - один и тот же девайс
      - стояли в момент старта
      - промежуток между алармами не больше merge_gap_sec
    Возвращает список кластеров; каждый кластер — как "alarms"-запись, но с полем switch_events/ io_indices_set.
    """
    if not norm:
        return []

    clusters: list[dict] = []
    cur = None

    for a in norm:
        a.setdefault("switch_events", [
            {"datetime": a["start_str"], "switch": a.get("io_index")}
        ])
        a.setdefault("io_indices_set", set([a.get("io_index")] if a.get("io_index") is not None else []))
        a_stopped = a.get("ssp_kmh", 999) <= min_stop_kmh

        if cur is None:
            cur = a.copy()
            cur["io_indices_set"] = set(cur.get("io_indices_set", set()))
            continue

        same_dev = (cur.get("dev_idno") == a.get("dev_idno"))
        gap_ok = (a["start_ts"] - cur["end_ts"] <= merge_gap_sec)
        if same_dev and a_stopped and gap_ok:
            # сливаем в текущий кластер
            cur["end_dt"]  = max(cur["end_dt"], a["end_dt"])
            cur["end_ts"]  = max(cur["end_ts"], a["end_ts"])
            cur["end_str"] = cur["end_dt"].strftime(settings.TIME_FMT)
            cur["esp_kmh"] = a.get("esp_kmh", cur.get("esp_kmh"))
            # объединяем типы
            cur_types = set([cur.get("cargo_type", "unknown")])
            cur_types |= set([a.get("cargo_type", "unknown")])
            cur["cargo_type"] = _resolve_cluster_cargo({t for t in cur_types if t})
            # объединяем IO индексы и события
            cur["io_indices_set"] |= a.get("io_indices_set", set())
            cur["switch_events"] = _dedupe_sw((cur.get("switch_events") or []) + (a.get("switch_events") or []))
        else:
            # завершаем предыдущий кластер и начинаем новый
            clusters.append(cur)
            cur = a.copy()
            cur["io_indices_set"] = set(cur.get("io_indices_set", set()))

    if cur is not None:
        clusters.append(cur)

    return clusters

def _merge_or_append(loading_intervals, new_interval, epsilon_sec=30):
    """Если в списке уже есть интерес с тем же стартом (±epsilon) и тем же cargo_type — расширяем его конец и события."""
    from datetime import datetime
    fmt = "%Y-%m-%d %H:%M:%S"

    new_start = datetime.strptime(new_interval["start_time"], fmt)
    new_end   = datetime.strptime(new_interval["end_time"], fmt)
    new_cargo = new_interval.get("report", {}).get("cargo_type")

    for cur in loading_intervals:
        cur_cargo = cur.get("report", {}).get("cargo_type")
        if cur_cargo != new_cargo:
            continue
        cur_start = datetime.strptime(cur["start_time"], fmt)

        if abs((cur_start - new_start).total_seconds()) <= epsilon_sec:
            # расширяем конец, если новый длиннее
            cur_end = datetime.strptime(cur["end_time"], fmt)
            if new_end > cur_end:
                cur["end_time"] = new_interval["end_time"]
                cur["photo_after_timestamp"] = new_interval.get("photo_after_timestamp", cur.get("photo_after_timestamp"))

            # мерджим события переключателей
            cur_sw = cur.get("report", {}).get("switch_events", [])
            new_sw = new_interval.get("report", {}).get("switch_events", [])
            # простая дедупликация по (datetime, switch, source)
            seen = { (e.get("datetime"), e.get("switch"), e.get("source")) for e in cur_sw }
            for e in new_sw:
                key = (e.get("datetime"), e.get("switch"), e.get("source"))
                if key not in seen:
                    cur_sw.append(e); seen.add(key)
            cur["report"]["switch_events"] = cur_sw
            return True  # поглотили

    # похожего не нашли — добавляем как новый
    loading_intervals.append(new_interval)
    return False

def prepare_alarms(raw_alarms: List[Dict[str, Any]],
                   reg_cfg: Dict[str, Any],
                   allowed_atp: set[int] = frozenset({19,20,21,22}),
                   min_stop_speed_kmh: float = 5.0,
                   merge_gap_sec: int = 15) -> Dict[str, Any]:
    """
    Превращает raw alarms из API в удобный список для поиска «в разрывах».
    Возвращает:
      {
        "alarms": [ ... нормализованные ... ],
        "starts": [ start_ts ... ]  # для bisect
      }
    """
    euro_alarm = int(reg_cfg.get("euro_container_alarm", 4)) if reg_cfg else None
    kgo_alarm_val = reg_cfg.get("kgo_container_alarm") if reg_cfg else None
    kgo_alarm = int(kgo_alarm_val) if kgo_alarm_val is not None else None

    # 1) фильтр нужных типов
    filtered = [a for a in raw_alarms if (a.get("atp") in allowed_atp)]

    # 2) нормализация
    norm: List[Dict[str, Any]] = []
    for a in filtered:

        start_dt, start_str = _parse_alarm_time(a, "stm", "bTimeStr")
        if not start_dt:
            try:
                logger.warning(f"[ALARMS] Пропуск записи без stm: guid={a.get('guid')}")
            except NameError:
                pass
            continue

        # Время конца (если нет etm → fallback от stm)
        end_dt, end_str = _parse_alarm_time(a, "etm", "eTimeStr")
        if not end_dt:
            fb_after = 30  # можно взять из settings.config.getint("Interests", "AFTER_FALLBACK_SEC", fallback=30)
            end_dt = start_dt + datetime.timedelta(seconds=fb_after)
            end_str = end_dt.strftime(settings.TIME_FMT)
        ssp_kmh = (a.get("ssp") or 0) / 10.0
        esp_kmh = (a.get("esp") or 0) / 10.0

        io_idx = _atp_to_io_index(a.get("atpStr") or "")
        if io_idx == euro_alarm:
            cargo = "euro"
        elif kgo_alarm is not None and io_idx == kgo_alarm:
            cargo = "kgo"
        else:
            cargo = "unknown"

        norm.append({
            "guid": a.get("guid"),
            "dev_idno": a.get("did"),
            "plate": a.get("vid"),
            "atp": a.get("atp"),
            "atpStr": a.get("atpStr"),
            "io_index": io_idx,
            "cargo_type": cargo,

            "start_dt": start_dt,
            "end_dt": end_dt,
            "start_ts": start_dt.timestamp(),
            "end_ts": end_dt.timestamp(),
            "start_str": start_str,
            "end_str": end_str,

            "ssp_kmh": ssp_kmh,
            "esp_kmh": esp_kmh,
            "start_stopped": (ssp_kmh <= min_stop_speed_kmh),

            # координаты и пр. можно оставить при необходимости:
            "slng": a.get("slng"), "slat": a.get("slat"),
            "elng": a.get("elng"), "elat": a.get("elat"),
            "smlng": a.get("smlng"), "smlat": a.get("smlat"),
            "emlng": a.get("emlng"), "emlat": a.get("emlat"),
        })

    # 3) сортировка
    norm.sort(key=lambda x: x["start_ts"])

    """
    ## 4) (опционально) объединить «дрожание» одного IO в один блок
    merged: List[Dict[str, Any]] = []
    for a in norm:
        if not merged:
            merged.append(a); continue
        prev = merged[-1]
        same_io = (a["io_index"] == prev["io_index"]) and (a["cargo_type"] == prev["cargo_type"])
        close_enough = (a["start_ts"] - prev["end_ts"] <= merge_gap_sec)
        if same_io and close_enough:
            # расширяем конец и переносим esp/флаги по необходимости
            prev["end_dt"] = a["end_dt"]
            prev["end_ts"] = a["end_ts"]
            prev["end_str"] = a["end_str"]
            prev["esp_kmh"] = a["esp_kmh"]
        else:
            merged.append(a)

    starts = [a["start_ts"] for a in norm]
    """
    merged = _cluster_merge_stationary(norm, min_stop_speed_kmh, merge_gap_sec)

    starts = [a["start_ts"] for a in merged]
    return {"alarms": merged, "starts": starts}


def estimate_move_start_kmhps(
    t0,                 # datetime: последний наблюдаемый «стоп»-трек
    t1,                 # datetime: первый «едем»-трек
    v1_kmh,             # скорость на t1 (в тех же единицах, что MIN_MOVE_SPEED)
    min_move_speed,     # порог «едем» (км/ч)
    small_gap_sec=5,    # «малый разрыв»
    max_gap_sec=30,     # «большой разрыв»
    A_KMHPS=1.26,       # типовое ускорение (км/ч за секунду), 1.26 ~= 0.35 м/с²
    clamp_eps=0.1       # небольшой зазор от границ окна
):
    """
    Возвращает оценку момента старта движения (datetime) в интервале (t0, t1).
    Всё в километрах/час и секундах, без перевода в м/с.
    """
    dt = (t1 - t0).total_seconds()

    # 1) Малый разрыв — консервативно ближе к t1
    if dt <= small_gap_sec:
        t_move = t1 - datetime.timedelta(seconds=min(1.0, dt / 2.0))
    else:
        # 2) Оценка по «физике» разгона в км/ч/сек
        A = max(A_KMHPS, 1e-6)
        tau_sec = max(0.0, v1_kmh) / A
        t_move = t1 - datetime.timedelta(seconds=tau_sec)

    # 3) Очень большая дырка — смягчим, если еле тронулись
    if dt > max_gap_sec and v1_kmh <= min_move_speed * 2:
        t_move = max(t1 - datetime.timedelta(seconds=1.0), t0 + datetime.timedelta(seconds=clamp_eps))

    # 4) Зажать в рамки окна
    lo = t0 + datetime.timedelta(seconds=clamp_eps)
    hi = t1 - datetime.timedelta(seconds=clamp_eps)
    if t_move < lo: t_move = lo
    if t_move > hi: t_move = hi

    return t_move

def find_interests_by_lifting_switches(
        tracks, sec_before=30, sec_after=30, start_tracks_search_time=None, reg_id=None, alarms=None):
    """
    tracks – список треков CMS (gt, s1, sp, ps и т.д.)
    alarms – ПОДГОТОВЛЕННЫЕ алармы: {"alarms": [...], "starts": [...]}, см. prepare_alarms(...)
             Если формат иной или None — логика по алармам будет пропущена (ничего не ломаем).
    """
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

    last_stop_started_at: datetime.datetime | None = None
    prev_speed: int | None = None
    min_stop_speed = settings.config.getint("Interests", "MIN_STOP_SPEED")
    min_move_speed = settings.config.getint("Interests", "MIN_MOVE_SPEED")
    min_stop_duration = settings.config.getint("Interests", "MIN_STOP_DURATION_SEC")

    def _update_stop_state(cur_dt: datetime.datetime, cur_speed: int):
        nonlocal last_stop_started_at, prev_speed
        # старт остановки при переходе через порог вниз
        if cur_speed <= min_stop_speed and (prev_speed is None or prev_speed > min_stop_speed):
            last_stop_started_at = cur_dt
        # любая устойчивая «поехали» сбрасывает маркер остановки
        if cur_speed >= min_move_speed:
            last_stop_started_at = None
        prev_speed = cur_speed

    # --- локальная утилита для быстрого поиска алармов в окне разрыва ---
    def _alarms_in_gap(prepared, gap_start_ts, gap_end_ts):
        """prepared == {"alarms": [...], "starts": [...]}"""
        try:
            from bisect import bisect_left
            _alarms = prepared.get("alarms") or []
            _starts = prepared.get("starts") or []
            idx = bisect_left(_starts, gap_start_ts)
            out = []
            while idx < len(_alarms) and _alarms[idx].get("start_ts", 0) <= gap_end_ts:
                a = _alarms[idx]
                if a.get("end_ts", 0) >= gap_start_ts:
                    out.append(a)
                idx += 1
            return out
        except Exception as e:
            logger.warning(f"{reg_id}: [ALARM GAP] Не удалось выбрать алармы в разрыве: {e}")
            return []

    while i < len(tracks) - 1:
        # Защита от выхода за границы для next_track
        if i + 1 >= len(tracks):
            break

        track = tracks[i]
        next_track = tracks[i+1]
        cur_speed = track.get("sp")
        t_curr_dt = track.get("gt")
        t_next_dt = next_track.get("gt")

        # --- вычисление разрыва между текущим треком и следующим ---
        t_curr = datetime.datetime.strptime(t_curr_dt, "%Y-%m-%d %H:%M:%S")
        t_next = datetime.datetime.strptime(t_next_dt, "%Y-%m-%d %H:%M:%S")

        _update_stop_state(t_curr_dt, cur_speed)

        gap_sec = (t_next - t_curr).total_seconds()
        GAP_THRESHOLD = settings.config.getint("Interests", "GAP_THRESHOLD_SEC", fallback=10)
        if gap_sec > GAP_THRESHOLD:
            logger.debug(f"Обнаружен разрыв в треках: {t_curr} → {t_next} = {gap_sec:.1f}s")

        # === Новая вставка: обработка "разрыва" через алармы (если они переданы и подготовлены) ===
        if alarms and isinstance(alarms, dict) and "alarms" in alarms and "starts" in alarms and gap_sec > GAP_THRESHOLD:
            gap_start_ts = t_curr.timestamp()
            gap_end_ts = t_next.timestamp()
            gap_alarms = _alarms_in_gap(alarms, gap_start_ts, gap_end_ts)

            if gap_alarms:
                logger.debug(f"{reg_id}:[ALARM GAP] Разрыв {t_curr_dt} → {t_next_dt} ({int(gap_sec)}s), найдено алармов: {len(gap_alarms)}")

            for a in gap_alarms:
                alarm_dt = a.get("start_dt")
                alarm_ts_str = a.get("start_str")
                #gap_start_dt = datetime.datetime.strptime(gap_start_ts, "%Y-%m-%d %H:%M:%S")

                logger.info(f"{reg_id}: Обработка аларма в {alarm_ts_str}")

                # Принимаем только «стоял в начале события» и известный тип груза
                start_stopped = a.get("start_stopped")

                if not start_stopped:
                    continue

                cargo_key = a.get("cargo_type", "unknown")
                if cargo_key == "unknown":
                    continue
                cargo_type_alarm = "КГО" if cargo_key == "kgo" else "Контейнер"

                delta_last_track_to_alarm_seconds =  (alarm_dt - t_curr).total_seconds()
                delta_alarm_to_first_track_seconds = (t_next - alarm_dt).total_seconds()

                if delta_last_track_to_alarm_seconds > 30:
                    logger.warning(
                        f"{reg_id}:[BEFORE] Разрыв {delta_last_track_to_alarm_seconds:.1f}с до аларма {alarm_dt}. "
                        f"Оцениваем время ДО внутри разрыва."
                    )
                    # оценим «последнюю секунду устойчивой остановки» внутри окна (t_curr..alarm_dt)
                    # кейс 1: мы и так стояли на последнем треке — берём почти у самого аларма
                    min_stop_speed = settings.config.getint("Interests", "MIN_STOP_SPEED")
                    v_prev = int(track.get("sp") or 0)
                    eps = 0.1
                    lo = t_curr + datetime.timedelta(seconds=eps)
                    hi = alarm_dt - datetime.timedelta(seconds=eps)

                    if v_prev <= min_stop_speed or a.get("start_stopped"):
                        t_before_dt = max(lo, alarm_dt - datetime.timedelta(seconds=1))
                        time_before = t_before_dt.strftime("%Y-%m-%d %H:%M:%S")
                        logger.debug(f"Кейс 1. Оцененный time_before: {time_before}")
                    else:
                        # кейс 2: на последнем треке мы НЕ стояли → пробуем обычный поиск по времени (он уже устойчив к дыркам)
                        time_before = find_first_stable_stop(
                            tracks, i, alarm_dt, settings, first_interest, start_tracks_search_time, reg_id
                        )
                        logger.debug(f"Кейс 2. Оцененный time_after: {time_before}")
                else:
                    time_before = find_first_stable_stop(tracks, i, alarm_dt, settings, first_interest,
                                                         start_tracks_search_time, reg_id)

                if delta_alarm_to_first_track_seconds > 30:
                    logger.warning(
                        f"{reg_id}:[AFTER] Пропуск поиска движения — первый трек после разрыва ({t_next}) опережает момент срабатывания ({alarm_dt}) на {delta_alarm_to_first_track_seconds:.1f}с. Используем физическую оценку."
                    )
                    v1_kmh = float(next_track.get("sp") or 0)
                    t_move = estimate_move_start_kmhps(
                        t0=t_curr,
                        t1=t_next,
                        v1_kmh=v1_kmh,
                        min_move_speed=settings.config.getint("Interests", "MIN_MOVE_SPEED"),
                        small_gap_sec=settings.config.getint("Interests", "INTERESTS_MOVE_SMALL_GAP_SEC", fallback=5),
                        max_gap_sec=settings.config.getint("Interests", "INTERESTS_MOVE_MAX_GAP_SEC", fallback=30),
                        A_KMHPS=float(settings.config.get("Interests", "INTERESTS_MOVE_A_KMHPS", fallback="1.26")),
                        clamp_eps=0.1,
                    )
                    time_after = t_move.strftime("%Y-%m-%d %H:%M:%S")
                    logger.info(f"Оцененный time_after: {time_after}")
                else:
                    time_after, last_stop_idx = find_stop_after_lifting(tracks, i + 1, settings, logger, reg_id)


                # Сдвиг фото ПОСЛЕ
                raw_time_after = datetime.datetime.strptime(time_after, "%Y-%m-%d %H:%M:%S")
                adjusted_time_after = raw_time_after - datetime.timedelta(
                    seconds=settings.config.getint("Interests", "PHOTO_AFTER_SHIFT_SEC"))
                time_after_adj = adjusted_time_after.strftime("%Y-%m-%d %H:%M:%S")

                # «окно интереса» для выгрузки (как у тебя ниже: end_time — это time_30_after)
                time_30_after = (alarm_dt + datetime.timedelta(seconds=sec_after)).strftime("%Y-%m-%d %H:%M:%S")

                interval = get_interest_from_track(
                    tracks[-1],
                    start_time=time_before,
                    end_time=time_30_after,
                    photo_before_timestamp=time_before,
                    photo_after_timestamp=time_after_adj,
                )
                interval["report"] = {
                    "cargo_type": cargo_type_alarm,
                    "geo": track.get("ps"),
                    "switches_amount": 1,
                    "switch_events": [{
                        "datetime": alarm_ts_str,
                        "switch": a.get("io_index"),
                        "source": "alarm-gap"
                    }],
                }
                #_ = _merge_or_append(loading_intervals, interval, epsilon_sec=30)
                loading_intervals.append(interval)
                first_interest = False
                logger.info(f"{reg_id}: [ALARM GAP] Добавлен интерес по alarm {alarm_ts_str}: {time_before} → {time_after_adj}")

        # === Старая логика концевиков — без изменений ===
        s1 = track.get("s1")
        timestamp = track.get("gt")
        current_dt = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        s1_int = int(s1)

        bits = list(bin(s1_int & 0xFFFFFFFF)[2:].zfill(32))
        bits.reverse()
        min_speed_for_switch_detect = settings.config.getint("Interests", "MIN_SPEED_FOR_SWITCH_DETECT")
        euro_on = bits[euro_bit_idx] == '1'
        kgo_on = (kgo_bit_idx is not None) and (bits[kgo_bit_idx] == '1')
        if euro_on or kgo_on:
            cargo_type = "КГО" if kgo_on else "Контейнер"
            logger.info(f"{reg_id}: [SWITCH] Срабатывание концевика в {timestamp}, EuroIO(bit {euro_bit_idx})={bits[euro_bit_idx]}" + (f", KGOIO(bit {kgo_bit_idx})={bits[kgo_bit_idx]}" if kgo_bit_idx is not None else ""))

            if track.get("sp") > min_speed_for_switch_detect:
                logger.debug(f"{reg_id}: [SWITCH] Игнор: скорость {track.get('sp')} > {min_speed_for_switch_detect}")
                i += 1

            # 2) Проверяем, что перед концевиком фактически успели постоять
            if last_stop_started_at is None or (t_curr_dt - last_stop_started_at).total_seconds() < min_stop_duration:
                dur = 0 if last_stop_started_at is None else (t_curr_dt - last_stop_started_at).total_seconds()
                logger.info(
                    f"{reg_id} [SWITCH] Недостаточная длительность остановки перед концевиком ({dur:.1f}s < {min_stop_duration}s) — игнорируем."
                )
                i += 1
                continue

            logger.debug(f"{reg_id}: [SWITCH] Принято: {cargo_type} в {timestamp}")
            switch_events = []

            if i >= len(tracks):
                logger.warning(f"{reg_id}: [SWITCH] Индекс {i} вне диапазона треков. Прерывание.")
                break

            # Находим время для фото ДО (Последнее время в окне стабильных остановок)
            time_before = find_first_stable_stop(
                tracks, i, current_dt, settings, first_interest, start_tracks_search_time, reg_id)
            if not time_before:
                logger.warning(f"{reg_id}: [BEFORE] Не найдена остановка до сработки концевика в {timestamp}")
                if first_interest:
                    logger.warning(f"{reg_id}: [BEFORE] Это был первый интерес, возвращаемся для получения дополнительных треков")
                    return {"error": "No stop before switch found for first interest"}

            lifting_end_idx = i
            last_switch_index = i

            if euro_on:
                switch_events.append({"datetime": timestamp, "switch": euro_bit_idx})
            if kgo_on:
                switch_events.append({"datetime": timestamp, "switch": kgo_bit_idx})

            # В этом цикле мы перебираем треки и ищем трек, когда погрузка закочена (по скорости и концевику)
            logger.debug(f"{reg_id}: [Конец интереса]  Теперь ищем когда машина поехала после погрузки.")

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

                logger.debug(f"{reg_id}: [Конец интереса] Ищем движение после погрузки. {next_track.get('gt')}, EuroIO(bit {euro_bit_idx})={next_bits[euro_bit_idx]}" + (f", KGOIO(bit {kgo_bit_idx})={next_bits[kgo_bit_idx]}" if kgo_bit_idx is not None else "") + f", sp={next_spd}")

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

                # 2) если скорость низкая — расширяем окно, но сбрасываем накопление «движения»
                elif next_spd < min_move_speed:
                    lifting_end_idx += 1
                    move_started_at = None

                # 3) скорость выше порога — проверяем длительность устойчивого движения
                else:
                    if move_started_at is None:
                        move_started_at = ts
                        move_started_at_str = sw_time
                        logger.debug(f"{reg_id}: Убедились, что машина поехала на {move_started_at_str}.")
                    lifting_end_idx += 1
                    if (ts - move_started_at) >= min_move_duration:
                        break

            logger.debug(f"{reg_id}: [Конец интереса] Вышли из цикла поиска движения после последнего концевика")
            time_after, last_stop_idx = find_stop_after_lifting(tracks, last_switch_index + 1, settings, logger, reg_id)
            used_fallback = False

            logger.debug(f"{reg_id}: [Конец интереса] Вышли из цикла поиска движения после последнего концевика")
            time_after, last_stop_idx = find_stop_after_lifting(tracks, last_switch_index + 1, settings, logger, reg_id)
            used_fallback = False

            if not time_after:
                # Фоллбэк ТОЛЬКО если давно нет новых треков (последний трек старше 30 минут)
                fmt = "%Y-%m-%d %H:%M:%S"
                last_track_dt = datetime.datetime.strptime(tracks[-1]['gt'], fmt)
                age_sec = (datetime.datetime.now() - last_track_dt).total_seconds()

                if age_sec > 30 * 60:
                    # Телеметрия не обновляется ≥30 мин — применяем страховку
                    time_after = fallback_photo_after_time(tracks, last_switch_index, settings, logger)
                    if not time_after:
                        i = lifting_end_idx + 1
                        continue
                    used_fallback = True
                else:
                    # Треки свежие — погрузка, вероятно, ещё идёт. Ждём обновлений, конец не фиксируем.
                    age_min = age_sec / 60.0
                    logger.info(
                        f"{reg_id}: [AFTER] Пропускаем fallback: последний трек свежий ({age_min:.1f} мин назад, {last_track_dt}). Ждём движения.")
                    if loading_intervals:
                        return {"interests": loading_intervals}
                    else:
                        raise LoadingInProgress
                    #i = lifting_end_idx + 1
                    #continue

            if (last_stop_idx is None) and (not used_fallback):
                logger.warning(f"{reg_id}: [AFTER] Нет last_stop_idx — остановка не найдена после {timestamp}")
                i = lifting_end_idx + 1
                continue

            # Применяем сдвиг - фото ПОСЛЕ за несколько секунд до движения
            adjust_secs = settings.config.getint("Interests", "PHOTO_AFTER_SHIFT_SEC")
            raw_time_after = datetime.datetime.strptime(time_after, "%Y-%m-%d %H:%M:%S")
            adjusted_time_after = raw_time_after - datetime.timedelta(seconds=adjust_secs)
            logger.debug(f"{reg_id}: Двигаем время конца интереса на {adjust_secs}с")
            time_after = adjusted_time_after.strftime("%Y-%m-%d %H:%M:%S")

            last_alarm_dt = datetime.datetime.strptime(tracks[last_switch_index].get("gt"), "%Y-%m-%d %H:%M:%S")
            time_30_after_dt = last_alarm_dt + datetime.timedelta(seconds=sec_after)
            time_30_after = time_30_after_dt.strftime("%Y-%m-%d %H:%M:%S")

            if time_before and time_after:
                logger.info(f"{reg_id}: Интерес найден! {time_before} до {time_after}")
                interval = get_interest_from_track(
                    tracks[-1],
                    start_time=time_before,
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
                logger.info(f"{reg_id}: [SKIP] Пропуск: нет {'time_before' if not time_before else ''}{' и ' if not time_before and not time_after else ''}{'time_after' if not time_after else ''}")

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


def find_stop_after_lifting(tracks, start_idx, settings, logger=None, reg_id=None):
    """
    Ищем момент НАЧАЛА ДВИЖЕНИЯ после lifting:
    - Считаем длительности по времени (а не по количеству точек).
    - Возвращаем (t_move_str, last_stop_idx), где t_move_str — оценка старта движения (для твоего последующего сдвига).
    - Устойчиво к разреженной телеметрии (дыркам).
    """
    if logger is None:
        class _N:
            def debug(self, *a, **k): pass
            def info(self, *a, **k): pass
            def warning(self, *a, **k): pass
        logger = _N()

    # Пороги из настроек (в тех же единицах, что и sp в треках!)
    min_stop_speed = settings.config.getint("Interests", "MIN_STOP_SPEED")
    min_stop_duration = settings.config.getint("Interests", "MIN_STOP_DURATION_SEC")
    min_move_speed = settings.config.getint("Interests", "MIN_MOVE_SPEED")
    min_move_duration = settings.config.getint("Interests", "MIN_MOVE_DURATION_SEC")


    def ts(i: int) -> datetime:
        return datetime.datetime.strptime(tracks[i]["gt"], "%Y-%m-%d %H:%M:%S")
    def spd(i: int) -> float:
        return float(tracks[i].get("sp") or 0)

    n = len(tracks)
    if not (0 <= start_idx < n):
        logger.warning(f"{reg_id}: [AFTER] start_idx {start_idx} вне диапазона треков")
        return None, None

    # Скан от последнего концевика вправо
    i = start_idx
    stop_active = False
    stop_low_dur = 0.0
    stop_end_idx = None         # последний индекс, где мы ещё «стояли»
    prev_t = ts(i)

    move_active = False
    move_dur = 0.0
    move_start_idx = None       # первый индекс, где скорость >= min_move_speed

    while i < n:
        t = ts(i)
        v = spd(i)
        dt = (t - prev_t).total_seconds() if i > start_idx else 0.0

        is_stop = (v <= min_stop_speed)
        is_move = (v >= min_move_speed)

        # --- зона остановки ---
        if is_stop:
            if not stop_active:
                stop_active = True
                stop_low_dur = 0.0
            stop_low_dur += dt
            stop_end_idx = i      # пока считаем, что «конец остановки» — текущий i
            # Любая низкая скорость сбрасывает накопление движения
            move_active = False
            move_dur = 0.0
            move_start_idx = None

        # --- не стоп ---
        else:
            # закончилась зона стопа — фиксируем её валидность по длительности
            if stop_active and stop_low_dur < min_stop_duration:
                # короткая "ложная" остановка — просто забываем
                stop_active = False
                stop_low_dur = 0.0
                stop_end_idx = None

            # если пошла скорость — начинаем/продолжаем «подтверждаемое движение»
            if is_move:
                if not move_active:
                    move_active = True
                    move_dur = 0.0
                    move_start_idx = i
                move_dur += dt
            else:
                # скорость промежуточная (между stop и move) — это "ничего": сбрасываем подтверждение движения
                move_active = False
                move_dur = 0.0
                move_start_idx = None

        # --- проверка условия «движение подтверждено» ---
        if move_active and (move_dur >= min_move_duration) and (stop_end_idx is not None):
            # У нас есть «последний стоп» (stop_end_idx) и «первый едем» (move_start_idx)
            t0 = ts(stop_end_idx)
            v0 = spd(stop_end_idx)
            t1 = ts(move_start_idx)
            v1 = spd(move_start_idx)

            # Если разрыв точек большой — оценим t_move внутри разрыва
            dt_gap = (t1 - t0).total_seconds()
            if dt_gap > 1.0:
                t_move = estimate_move_start_kmhps(t0=t0, t1=t1, v1_kmh=v1, min_move_speed=min_move_speed)
                t_move_str = t_move.strftime("%Y-%m-%d %H:%M:%S")
                logger.debug(f"{reg_id}: [AFTER] Оценили момент старта движения с разрывом {dt_gap:.1f}s → {t_move_str}")
                return t_move_str, stop_end_idx
            else:
                # Плотная телеметрия — «старт движения» почти на move_start_idx
                t_move_str = t1.strftime("%Y-%m-%d %H:%M:%S")
                return t_move_str, stop_end_idx

        prev_t = t
        i += 1

    logger.warning(f"{reg_id}: [PHOTO AFTER] Не удалось подтвердить движение после lifting (start_idx={start_idx})")
    return None, None


def find_stop_after_lifting_depr(tracks, start_idx, settings, logger=None, reg_id=None):
    # В этом блоке мы ищем время для фото ПОСЛЕ погрузки — после последнего срабатывания концевика
    stop_count = 0          # Количество точек с низкой скоростью (стоп)
    move_count = 0          # Количество точек подряд с высокой скоростью (движение)
    last_stop_idx = None    # Индекс последней точки с "настоящей" остановкой

    # Читаем пороги из настроек
    min_stop_speed = settings.config.getint("Interests", "MIN_STOP_SPEED")
    min_stop_duration = settings.config.getint("Interests", "MIN_STOP_DURATION_SEC")
    min_move_speed = settings.config.getint("Interests", "MIN_MOVE_SPEED")
    min_move_duration = settings.config.getint("Interests", "MIN_MOVE_DURATION_SEC")

    logger.debug("Мы обнаружили когда машина поехала, теперь ищем последний момент остановки перед движением.")
    # Начинаем проходить треки сразу после последнего концевика
    k = start_idx
    while k < len(tracks):
        spd = tracks[k].get("sp") or 0  # Текущая скорость
        time_curr = tracks[k].get("gt")
        logger.debug(
            f"{reg_id}: [Конец интереса] Обрабатываем треки. time - {time_curr}, stop_count - {stop_count}, "
            f"move_count - {move_count}, spd - {spd}")
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
                    logger.debug(f"{reg_id}: [Конец интереса] Найдена стабильная остановка на idx={last_stop_idx}, gt={tracks[last_stop_idx].get('gt')}")
                return tracks[last_stop_idx].get("gt"), last_stop_idx  # Возвращаем и время, и индекс

        # Иначе — сбрасываем всё, потому что последовательность нарушена
        else:
            if logger:
                logger.debug(f"{reg_id}: [Конец интереса] Сброс счётчиков на idx={k} (spd={spd}, stop={stop_count}, move={move_count})")
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
    start_tracks_search_time=None,  # для совместимости
    reg_id=None,
):
    """
    Ищем первую (от конца) стабильную остановку перед срабатыванием,
    устойчивую к кратковременным всплескам скорости.

    Условия:
    - Остановка: скорость <= MIN_STOP_SPEED, суммарная длительность низкой скорости >= MIN_STOP_DURATION_SEC.
    - Подтверждение выхода из остановки: скорость >= MIN_MOVE_SPEED непрерывно (по времени) >= MIN_MOVE_DURATION_SEC.
      Всплески короче этого порога считаем шумом и НЕ завершаем остановку.
    """


    logger.debug(f"{reg_id}: Ищем движение и остановку до первого срабатывания концевика")

    cfg = settings.config
    cutoff_time = current_dt - datetime.timedelta(
        seconds=cfg.getint("Interests", "MAX_LOOKBACK_SECONDS")
    )
    min_stop_speed = cfg.getint("Interests", "MIN_STOP_SPEED")
    min_stop_duration_sec = cfg.getint("Interests", "MIN_STOP_DURATION_SEC")
    min_move_speed = cfg.getint("Interests", "MIN_MOVE_SPEED")  # БЕЗ смещений и гистерезиса
    min_move_duration_sec = int(cfg.get("Interests", "MIN_MOVE_DURATION_SEC",
                                        fallback=str(min_stop_duration_sec)))

    # Защитный лимит для больших разрывов между точками при накоплении длительностей (опционально)
    sample_gap_cap = cfg.getint("Interests", "SAMPLE_GAP_CAP_SEC", fallback=20)

    def ts(idx: int) -> datetime.datetime:
        return datetime.datetime.strptime(tracks[idx]["gt"], "%Y-%m-%d %H:%M:%S")

    def spd(idx: int) -> int:
        return int(tracks[idx].get("sp") or 0)

    if not tracks:
        logger.warning(f"{reg_id}: [ОСТАНОВКА НЕ НАЙДЕНА] пустой массив треков")
        return None

    # Ограничим окно поиска по cutoff_time и start_index
    # найдём первый индекс, чей ts >= cutoff_time
    # (линейный проход тоже подойдёт; этот — O(log N))
    times = [ts(i) for i in range(len(tracks))]
    start_window_idx = bisect_left(times, cutoff_time)
    end_window_idx = max(0, min(start_index, len(tracks) - 1))
    if start_window_idx > end_window_idx:
        logger.warning(f"{reg_id}: [ОСТАНОВКА НЕ НАЙДЕНА] окно пусто")
        return None

    # Скан вперёд по времени: start_window_idx..end_window_idx
    stop_active = False
    stop_start_i = None          # индекс начала текущей остановки (по низкой скорости)
    stop_low_dur = 0.0           # суммарная длительность низкой скорости в текущей остановке (сек)

    move_after_stop_dur = 0.0    # длительность подтверждённого движения (скорость >= min_move_speed) после остановки
    candidate_start_gt = None    # старт последней валидной остановки (которую подтвердили движением)

    prev_t = times[start_window_idx]

    for i in range(start_window_idx, end_window_idx + 1):
        t = times[i]
        dt_raw = (t - prev_t).total_seconds() if i > start_window_idx else 0.0

        dt_eff = min(max(dt_raw, 0.0), sample_gap_cap)  # ограничиваем вклад одного шага
        v = spd(i)

        is_stop = (v <= min_stop_speed)
        is_move_confirm = (v >= min_move_speed)

        logger.debug(
            f"{reg_id}: [Начало интереса] i={i}, t={t}, v={v}, "
            f"stop_active={stop_active}, stop_low_dur={stop_low_dur:.1f}, "
            f"move_after_stop_dur={move_after_stop_dur:.1f}"
        )

        if is_stop:
            # в зоне остановки — накапливаем низкоскоростную длительность
            if not stop_active:
                stop_active = True
                stop_start_i = i
                stop_low_dur = 0.0
            stop_low_dur += dt_eff
            # любая низкая скорость обнуляет подтверждение движения
            move_after_stop_dur = 0.0

        else:
            # вне зоны остановки (скорость > min_stop_speed)
            if stop_active:
                if is_move_confirm:
                    # накапливаем подтверждённое движение
                    move_after_stop_dur += dt_eff
                    # если и сама остановка была достаточной, и движение держится — фиксируем кандидата
                    if move_after_stop_dur >= min_move_duration_sec and stop_low_dur >= min_stop_duration_sec:
                        candidate_start_gt = tracks[stop_start_i]["gt"]
                        logger.debug(
                            f"{reg_id}: [ДВИЖЕНИЕ ДО ОСТАНОВКИ] подтверждено: "
                            f"stop_low_dur={stop_low_dur:.1f}s, move_dur={move_after_stop_dur:.1f}s, "
                            f"start={candidate_start_gt}"
                        )
                        # КЛЮЧЕВОЕ: закрываем текущую остановку, чтобы следующая низкая скорость создала новую
                        stop_active = False
                        stop_start_i = None
                        stop_low_dur = 0.0
                        move_after_stop_dur = 0.0
                        # Не выходим из цикла: ищем более поздние кандидаты, чтобы взять самый «свежий»
                else:
                    # «полудвижение» — сбрасываем подтверждение, но остановку ещё не закрываем
                    move_after_stop_dur = 0.0
            # если остановки ещё не было — ничего не делаем

        prev_t = t

    best = None

    if stop_active and stop_low_dur >= min_stop_duration_sec:
        # текущая остановка идет до конца окна — берем её начало
        best = tracks[stop_start_i]["gt"]

    if candidate_start_gt is not None:
        # если подтвержденный кандидат позже текущего best — берем его; иначе оставляем best
        if best is None or candidate_start_gt > best:
            best = candidate_start_gt

    if best is not None:
        logger.debug(f"{reg_id}: [ДВИЖЕНИЕ ДО ОСТАНОВКИ] Лучший кандидат выбран - {best}")
        return best

    logger.warning(f"{reg_id}:  [ОСТАНОВКА НЕ НАЙДЕНА]")
    return None


def find_first_stable_stop_depr(
    tracks,
    start_index,
    current_dt,
    settings,
    first_interest=False,
    start_tracks_search_time=None,  # оставляем для совместимости
):
    logger.debug("Ищем движение и остановку до первого срабатывания концевика")

    cutoff_time = current_dt - datetime.timedelta(
        seconds=settings.config.getint("Interests", "MAX_LOOKBACK_SECONDS")
    )
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


def seconds_since_midnight(dt: datetime.datetime) -> int:
    midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    delta = dt - midnight
    return int(delta.total_seconds())


def cms_data_get_decorator_async(
    max_retries: int = 30,
    delay: float = 1.0,
    return_json: bool = False,
    retry_results: tuple[int, ...] = (22, 24),   # «временные» коды
):
    """
    Универсальный декоратор для CMS-запросов.
    - Разбирает JSON, чтобы решить — ретраить или нет.
    - По умолчанию возвращает httpx.Response; если return_json=True — dict.
    - НЕ ретраит код 32 (offline) — отдаём наверх как есть.
    """

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            attempt = 0
            last_exc: Exception | None = None

            while attempt < max_retries:
                attempt += 1
                try:
                    result = await func(*args, **kwargs)

                    # Поддержим обе ветки: функция вернула Response или сразу dict
                    data = None
                    if isinstance(result, httpx.Response):
                        # сетевые/HTTP ошибки → сразу исключение
                        result.raise_for_status()
                        try:
                            data = result.json()
                        except Exception as je:
                            # кривой JSON — можно сделать ещё одну попытку
                            logger.warning(f"[CMS] JSON parse failed on attempt {attempt}/{max_retries}: {je}")
                            raise
                    elif isinstance(result, dict):
                        data = result
                    else:
                        # неизвестный тип — вернём как есть
                        return result

                    # Если JSON получен — смотрим бизнес-код
                    res_code = data.get("result")
                    if res_code in retry_results:
                        # временная ошибка → ждём и повторяем
                        logger.warning(f"[CMS] result={res_code} → retry {attempt}/{max_retries} after {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    # 32 (offline) — не ретраим, отдаём как есть
                    # остальные коды — считаем «ок» и возвращаем

                    return data if return_json else result

                except Exception as e:
                    last_exc = e
                    if attempt >= max_retries:
                        break
                    logger.warning(f"[CMS] attempt {attempt}/{max_retries} failed: {e}; retry after {delay}s")
                    await asyncio.sleep(delay)

            # все попытки исчерпаны
            if last_exc:
                raise last_exc
            raise RuntimeError(f"[CMS] Failed after {max_retries} attempts without specific exception")

        return wrapper
    return decorator
