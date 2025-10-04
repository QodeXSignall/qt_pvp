from typing import Optional, Dict, Any, List, Tuple
from qt_pvp.functions import get_reg_info
from datetime import datetime, timezone
from bisect import bisect_left
from qt_pvp.logger import logger
from qt_pvp import settings
import inspect
import datetime
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

def get_device_alarms(
    jsession: str,
    dev_idno: Optional[str] = None,
    vehi_idno: Optional[str] = None,
    begintime: str = "",
    endtime: str = "",
    arm_type: str = "19,20,21,22",
    handle: Optional[int] = None,
    to_map: Optional[int] = None,
    timeout: int = 30,
    page_records: int = 50,
    start_page: int = 1,
    max_pages: Optional[int] = None,        # ограничение по кол-ву страниц (для отладки)
    sleep_between: float = 0.0,             # пауза между запросами (анти-DDOS)
    log_each: bool = False,                 # печатать прогресс по страницам
) -> Dict[str, Any]:
    """
    Тянет все страницы StandardApiAction_queryAlarmDetail.action и склеивает alarms.
    Возвращает словарь с result=0, alarms=[...], pagination={...} (агрегированная).

    Примечания:
    - Если API вернуло result != 0 на любой странице — выбрасываем RuntimeError.
    - Если totalPages/hasNextPage отсутствуют/подозрительны — выходим, когда страница вернула < page_records записей.
    - etm/stm как есть (их нормализация делается дальше в prepare_alarms).
    """
    url = f"{settings.cms_host}/StandardApiAction_queryAlarmDetail.action"

    base_params = {
        "jsession": jsession,
        "begintime": begintime,
        "endtime": endtime,
        "armType": arm_type,
        "pageRecords": page_records,
    }
    if dev_idno:
        base_params["devIdno"] = dev_idno
    if vehi_idno:
        base_params["vehiIdno"] = vehi_idno
    if handle is not None:
        base_params["handle"] = str(handle)
    if to_map is not None:
        base_params["toMap"] = str(to_map)

    alarms: List[Dict[str, Any]] = []
    page = start_page
    pages_seen = 0

    total_pages_reported: Optional[int] = None
    total_records_reported: Optional[int] = None

    while True:
        params = dict(base_params)
        params["currentPage"] = page

        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json()

        if data.get("result") != 0:
            raise RuntimeError(f"API error result={data.get('result')}: {data}")

        page_alarms = data.get("alarms") or []
        alarms.extend(page_alarms)

        pag = data.get("pagination") or {}
        total_pages_reported = pag.get("totalPages", total_pages_reported)
        total_records_reported = pag.get("totalRecords", total_records_reported)
        has_next = pag.get("hasNextPage")
        next_page = pag.get("nextPage")

        pages_seen += 1
        if log_each:
            print(f"[ALARMS] page {page} got {len(page_alarms)} items; "
                  f"total so far {len(alarms)}; "
                  f"reported totalPages={total_pages_reported}, totalRecords={total_records_reported}")

        # условия выхода:
        stop = False

        # 1) лимит по страницам (для отладки)
        if max_pages is not None and pages_seen >= max_pages:
            stop = True

        # 2) если API корректно сообщает флаги пагинации — используем их
        elif has_next is False:
            stop = True
        elif isinstance(next_page, int) and next_page <= page:
            # защита от зацикливания, если nextPage сломан
            stop = True

        # 3) fallback: если пришло меньше, чем page_records — вероятно последняя
        elif len(page_alarms) < page_records:
            stop = True

        if stop:
            break

        # готовим следующую страницу
        page = (next_page if isinstance(next_page, int) and next_page > page else page + 1)
        if sleep_between > 0:
            time.sleep(sleep_between)

    # Собираем «сводную» пагинацию (информативно)
    pagination_out = {
        "currentPage": page,
        "totalPages": total_pages_reported if total_pages_reported is not None else pages_seen,
        "pageRecords": page_records,
        "totalRecords": total_records_reported if total_records_reported is not None else len(alarms),
        "hasNextPage": False,
    }

    return {"result": 0, "alarms": alarms, "pagination": pagination_out}


def _safe_fromtimestamp_sec(ts_sec: float) -> datetime:
    # на входе секунды (float/int)
    dt_utc = datetime.datetime.fromtimestamp(ts_sec, tz=timezone.utc)
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
            dt_local = datetime.strptime(s.strip(), settings.TIME_FMT)
            return dt_local, s.strip()
        except Exception:
            pass

    return None, None

def _atp_to_io_index(atp: int, atp_str: Optional[str]) -> Optional[int]:
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
    from datetime import datetime, timedelta
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
            try:
                logger.debug(f"[ALARMS] etm отсутствует, используем fallback {fb_after}с для guid={a.get('guid')}")
            except NameError:
                pass
        ssp_kmh = (a.get("ssp") or 0) / 10.0
        esp_kmh = (a.get("esp") or 0) / 10.0

        io_idx = _atp_to_io_index(a.get("atp"), a.get("atpStr") or "")
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

def alarms_in_gap(prepared: Dict[str, Any], gap_start_ts: float, gap_end_ts: float) -> List[Dict[str, Any]]:
    """Вернёт все подготовленные алармы, попавшие во временной разрыв [gap_start_ts, gap_end_ts]."""
    alarms = prepared["alarms"]
    starts = prepared["starts"]
    i = bisect_left(starts, gap_start_ts)
    out = []
    while i < len(alarms) and alarms[i]["start_ts"] <= gap_end_ts:
        # пересекается ли вообще с окном
        if alarms[i]["end_ts"] >= gap_start_ts:
            out.append(alarms[i])
        i += 1
    return out

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
            logger.warning(f"[ALARM GAP] Не удалось выбрать алармы в разрыве: {e}")
            return []

    while i < len(tracks) - 1:
        # Защита от выхода за границы для next_track
        if i + 1 >= len(tracks):
            break

        track = tracks[i]
        next_track = tracks[i+1]
        cur_speed = track.get("sp")

        # --- вычисление разрыва между текущим треком и следующим ---
        t_curr = datetime.datetime.strptime(track["gt"], "%Y-%m-%d %H:%M:%S")
        t_next = datetime.datetime.strptime(next_track["gt"], "%Y-%m-%d %H:%M:%S")
        gap_sec = (t_next - t_curr).total_seconds()

        GAP_THRESHOLD = settings.config.getint("Interests", "GAP_THRESHOLD_SEC", fallback=10)

        if gap_sec > GAP_THRESHOLD:
            logger.debug(f"gap: {t_curr} → {t_next} = {gap_sec:.1f}s")
            logger.debug(f"[TRACE] gt={track.get('gt')} sp={cur_speed}")

        # === Новая вставка: обработка "разрыва" через алармы (если они переданы и подготовлены) ===
        if alarms and isinstance(alarms, dict) and "alarms" in alarms and "starts" in alarms and gap_sec > GAP_THRESHOLD:
            gap_start_ts = t_curr.timestamp()
            gap_end_ts = t_next.timestamp()
            gap_alarms = _alarms_in_gap(alarms, gap_start_ts, gap_end_ts)

            if gap_alarms:
                logger.debug(f"[ALARM GAP] Разрыв {track['gt']} → {next_track['gt']} ({int(gap_sec)}s), найдено алармов: {len(gap_alarms)}")

            for a in gap_alarms:
                # Принимаем только «стоял в начале события» и известный тип груза
                start_stopped = a.get("start_stopped")
                if start_stopped is None:
                    # подстраховка, если вдруг нет поля — считаем из ssp_kmh
                    ssp_kmh = a.get("ssp_kmh")
                    if ssp_kmh is None:
                        ssp_kmh = (a.get("ssp") or 0) / 10.0
                    min_stop_kmh = settings.config.getint("Interests", "MIN_STOP_SPEED") / 10.0
                    start_stopped = ssp_kmh <= min_stop_kmh

                if not start_stopped:
                    continue

                cargo_key = a.get("cargo_type", "unknown")
                if cargo_key == "unknown":
                    continue
                cargo_type_alarm = "КГО" if cargo_key == "kgo" else "Контейнер"

                alarm_dt = a.get("start_dt")
                alarm_ts_str = a.get("start_str")
                if not alarm_dt:
                    # подстраховка на случай, если передали raw-формат
                    if a.get("bTimeStr"):
                        alarm_dt = datetime.datetime.strptime(a["bTimeStr"], "%Y-%m-%d %H:%M:%S")
                        alarm_ts_str = a["bTimeStr"]
                    elif a.get("stm"):
                        alarm_dt = datetime.datetime.fromtimestamp(a["stm"] / 1000.0)
                        alarm_ts_str = alarm_dt.strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        # если совсем нет времени — пропускаем
                        continue

                # ---- BEFORE: ищем остановку до события (как обычно), иначе fallback ----
                time_before = find_first_stable_stop(tracks, i, alarm_dt, settings, first_interest,
                                                     start_tracks_search_time)
                if not time_before:
                    logger.warning(f"[BEFORE] Не найдена остановка до alarm {alarm_ts_str}")
                    # fallback ДО: 120 c для КГО, иначе базовый sec_before
                    fb_before = 30
                    time_before = (alarm_dt - datetime.timedelta(seconds=fb_before)).strftime("%Y-%m-%d %H:%M:%S")
                    logger.warning(f"[BEFORE-FALLBACK] alarm-gap: {fb_before}с до {alarm_ts_str} => {time_before}")

                # ---- AFTER: стандартный поиск, иначе fallback от конца аларма ----
                time_after, last_stop_idx = find_stop_after_lifting(tracks, i + 1, settings, logger)
                if not time_after:
                    logger.warning(f"[FALLBACK AFTER] используем fallback для alarm {alarm_ts_str}")
                    end_dt = a.get("end_dt") or (alarm_dt + datetime.timedelta(seconds=sec_after))
                    fb_after = settings.config.getint("Interests", "AFTER_FALLBACK_SEC", fallback=30)
                    time_after = (end_dt + datetime.timedelta(seconds=fb_after)).strftime("%Y-%m-%d %H:%M:%S")

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
                logger.info(f"[ALARM GAP] Добавлен интерес по alarm {alarm_ts_str}: {time_before} → {time_after_adj}")

        # === Старая логика концевиков — без изменений ===
        s1 = track.get("s1")
        timestamp = track.get("gt")
        s1_int = int(s1)

        bits = list(bin(s1_int & 0xFFFFFFFF)[2:].zfill(32))
        bits.reverse()
        #i += 1

        min_speed_for_switch_detect = settings.config.getint("Interests", "MIN_SPEED_FOR_SWITCH_DETECT")
        euro_on = bits[euro_bit_idx] == '1'
        kgo_on = (kgo_bit_idx is not None) and (bits[kgo_bit_idx] == '1')
        if euro_on or kgo_on:
            cargo_type = "КГО" if kgo_on else "Контейнер"
            logger.info(f"[SWITCH] Срабатывание концевика в {timestamp}, EuroIO(bit {euro_bit_idx})={bits[euro_bit_idx]}" + (f", KGOIO(bit {kgo_bit_idx})={bits[kgo_bit_idx]}" if kgo_bit_idx is not None else ""))
            if track.get("sp") > min_speed_for_switch_detect:
                logger.debug(f"[SWITCH] Игнор: скорость {track.get('sp')} > {min_speed_for_switch_detect}")
                i += 1
                continue

            logger.debug(f"[SWITCH] Принято: {cargo_type} в {timestamp}")
            switch_events = []
            current_dt = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

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

                # 2) если скорость низкая — расширяем окно, но сбрасываем накопление «движения»
                elif next_spd < min_move_speed:
                    lifting_end_idx += 1
                    move_started_at = None

                # 3) скорость выше порога — проверяем длительность устойчивого движения
                else:
                    if move_started_at is None:
                        move_started_at = ts
                    lifting_end_idx += 1
                    if (ts - move_started_at) >= min_move_duration:
                        break

            time_after, last_stop_idx = find_stop_after_lifting(tracks, last_switch_index + 1, settings, logger)
            used_fallback = False
            if not time_after:
                time_after = fallback_photo_after_time(tracks, last_switch_index, settings, logger)
                if not time_after:
                    i = lifting_end_idx + 1
                    continue
                used_fallback = True

            if (last_stop_idx is None) and (not used_fallback):
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



def find_interests_by_lifting_switches_depr(
        tracks, sec_before=30, sec_after=30, start_tracks_search_time=None, reg_id=None, alarms=None):
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
        next_track = tracks[i+1]

        t_curr = datetime.datetime.strptime(track["gt"], "%Y-%m-%d %H:%M:%S")
        t_next = datetime.datetime.strptime(next_track["gt"], "%Y-%m-%d %H:%M:%S")
        gap_sec = (t_next - t_curr).total_seconds()

        GAP_THRESHOLD = settings.config.getint("Interests", "GAP_THRESHOLD_SEC", fallback=10)

        s1 = track.get("s1")
        timestamp = track.get("gt")
        s1_int = int(s1)

        bits = list(bin(s1_int & 0xFFFFFFFF)[2:].zfill(32))
        bits.reverse()
        print(timestamp, track.get("sp"))
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
            used_fallback = False
            if not time_after:
                time_after = fallback_photo_after_time(tracks, last_switch_index, settings, logger)
                if not time_after:
                    i = lifting_end_idx + 1
                    continue
                used_fallback = True

            if (last_stop_idx is None) and (not used_fallback):
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
        seconds=settings.config.getint("Interests", "MAX_LOOKBACK_SECONDS")
    )
    min_stop_speed = settings.config.getint("Interests", "MIN_STOP_SPEED")
    min_stop_duration_sec = settings.config.getint("Interests", "MIN_STOP_DURATION_SEC")

    stop_start_idx = None
    stop_end_idx = None  # край «позже» в серии
    j = start_index

    def ts(idx: int) -> datetime.datetime:
        return datetime.datetime.strptime(tracks[idx]["gt"], "%Y-%m-%d %H:%M:%S")

    # --- РАННЯЯ ПРОВЕРКА ШУМА (мягкая) ---
    # Шумом считаем только если есть РОВНО 5 подряд точек (j, j-1, ..., j-4),
    # и у всех скорость > min_stop_speed. Если точек меньше 5 — не помечаем как шум.
    if start_index >= 4:
        all_fast = True
        for k in range(start_index, start_index - 5, -1):
            spd_k = int(tracks[k].get("sp") or 0)
            if spd_k <= min_stop_speed:
                all_fast = False
                break
        if all_fast:
            logger.debug(
                f"[ШУМ] В первых 5 точках после срабатывания все скорости > {min_stop_speed}. "
                f"Считаем срабатывание шумом и возвращаем None."
            )
            return None
    # --- конец мягкой проверки ---

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



# опционально: если httpx/requests могут отсутствовать в окружении
try:
    import httpx
    HTTPX_ERRORS = (httpx.RequestError,)
except Exception:  # модуль может быть не установлен
    HTTPX_ERRORS = tuple()

try:
    import requests
    REQUESTS_ERRORS = (
        requests.exceptions.ReadTimeout,
        requests.exceptions.ConnectTimeout,
        requests.exceptions.ConnectionError,
    )
except Exception:
    REQUESTS_ERRORS = tuple()


def cms_data_get_decorator(tag: str = "execute func"):
    """
    Универсальный декоратор: поддерживает и sync, и async функции.
    Повторяет запрос при:
      - HTTP status != 200
      - ошибке парсинга JSON
      - data.get("result") == 24 (устройства «заняты»)
      - сетевых ошибках httpx/requests
    Экспоненциальный backoff: старт 1.0с, множитель 1.5, максимум 10.0с
    """
    def decorator(func):
        if inspect.iscoroutinefunction(func):
            # ==== ASYNC ВЕТКА ====
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                backoff = 1.0
                while True:
                    try:
                        response = await func(*args, **kwargs)

                        # HTTP-код
                        if getattr(response, "status_code", 0) != 200:
                            logger.warning(f"[{tag}] CMS HTTP {getattr(response, 'status_code', '???')}; "
                                           f"retry in {backoff:.1f}s")
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 1.5, 10.0)
                            continue

                        # JSON
                        try:
                            data = response.json()
                        except Exception as je:
                            logger.warning(f"[{tag}] CMS JSON parse error: {je}; retry in {backoff:.1f}s")
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 1.5, 10.0)
                            continue

                        # Спец-код прошивки
                        if data.get("result") == 24:
                            logger.debug(f"[{tag}] CMS busy (24); retry in {backoff:.1f}s")
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 1.5, 10.0)
                            continue

                        return response

                    except (*HTTPX_ERRORS, *REQUESTS_ERRORS, asyncio.TimeoutError) as err:
                        logger.warning(f"[{tag}] Connection problem with CMS: {err}; retry in {backoff:.1f}s")
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 1.5, 10.0)

            return async_wrapper

        else:
            # ==== SYNC ВЕТКА (как раньше) ====
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                backoff = 1.0
                while True:
                    try:
                        response = func(*args, **kwargs)

                        if getattr(response, "status_code", 0) != 200:
                            logger.warning(f"[{tag}] CMS HTTP {getattr(response, 'status_code', '???')}; "
                                           f"retry in {backoff:.1f}s")
                            time.sleep(backoff)
                            backoff = min(backoff * 1.5, 10.0)
                            continue

                        try:
                            data = response.json()
                        except Exception as je:
                            logger.warning(f"[{tag}] CMS JSON parse error: {je}; retry in {backoff:.1f}s")
                            time.sleep(backoff)
                            backoff = min(backoff * 1.5, 10.0)
                            continue

                        if data.get("result") == 24:
                            logger.debug(f"[{tag}] CMS busy (24); retry in {backoff:.1f}s")
                            time.sleep(backoff)
                            backoff = min(backoff * 1.5, 10.0)
                            continue

                        return response

                    except REQUESTS_ERRORS as err:
                        logger.warning(f"[{tag}] Connection problem with CMS: {err}; retry in {backoff:.1f}s")
                        time.sleep(backoff)
                        backoff = min(backoff * 1.5, 10.0)

            return sync_wrapper

    return decorator


@cms_data_get_decorator()
def get_mdvr_by_car_number_from_cms(jsession, car_number=None):
    response = requests.get(f"{settings.cms_host}/"
                            f"StandardApiAction_getDeviceByVehicle.action?",
                            params={'jsession': jsession,
                                    "vehiIdno": car_number})
    return response
