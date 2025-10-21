from typing import List, Optional
from qt_pvp import functions
import datetime
import logging

logger = logging.getLogger(__name__)

SWITCH_TOL_SEC = 10   # допуск, чтобы считать два интереса одной «switch-группой»
CLIP_EPS_SEC   = 1    # сдвиг старта следующего интереса после конца предыдущего

DT_FMT = "%Y-%m-%d %H:%M:%S"

def _try_parse_dt(s: str) -> Optional[datetime.datetime]:
    if not s:
        return None
    try:
        # ожидаемый формат логов/отчётов
        return datetime.datetime.strptime(s, DT_FMT)
    except Exception:
        try:
            # на всякий — ISO 8601
            return datetime.datetime.fromisoformat(s)
        except Exception:
            return None

def _anchor_switch_time(interest: dict) -> Optional[datetime.datetime]:
    """
    Пытаемся достать «якорное» время переключения для интереса:
    - если есть report.switch_events — берём ближайшее к start_time событие,
      либо первое попавшееся, если start_time нет.
    """
    rep = interest.get("report") or {}
    events = rep.get("switch_events") or []
    if not events:
        return None

    # Нормализуем в datetimes
    parsed = []
    for e in events:
        t = _try_parse_dt(e.get("datetime"))
        if t:
            parsed.append(t)
    if not parsed:
        return None

    st = interest.get("start_time")
    if isinstance(st, str):
        st = _try_parse_dt(st)

    if isinstance(st, datetime.datetime):
        # ближайшее к start_time
        return min(parsed, key=lambda x: abs((x - st).total_seconds()))
    else:
        # иначе самое раннее
        return min(parsed)

def _same_switch_bucket(a: dict, b: dict, tol_sec: int = SWITCH_TOL_SEC) -> bool:
    """
    Считаем интересы из одной «switch-группы», если:
    - reg_id совпадает (если присутствует), И
    - их якорные switch-времена есть и отличаются не более, чем на tol_sec.
    Если якорей нет — СЧИТАЕМ, ЧТО ГРУППЫ РАЗНЫЕ (перестраховка — не склеиваем).
    """
    if a.get("reg_id") and b.get("reg_id") and a["reg_id"] != b["reg_id"]:
        return False

    ta = _anchor_switch_time(a)
    tb = _anchor_switch_time(b)
    if not ta or not tb:
        return False  # нет уверенности — не склеиваем

    return abs((ta - tb).total_seconds()) <= tol_sec

def _clip_next_to_current_end(current: dict, nxt: dict, eps_sec: float = CLIP_EPS_SEC) -> bool:
    """
    Сдвигаем начало next-интервала сразу после current.end.
    Обновляем привязанные поля и имя интереса.
    Возвращаем True, если получилось откорректировать валидный интервал; False — если интервал выродился.
    """
    # Новый beg_sec
    new_beg = max(nxt["beg_sec"], current["end_sec"] + eps_sec)

    # Защита от вырождения (пересечение могло быть почти «в ноль»)
    if new_beg >= nxt["end_sec"]:
        # Попробуем минимально сохранить длительность:
        tiny = max(0.1, eps_sec * 0.5)
        new_beg = min(nxt["end_sec"] - tiny, new_beg)
        if new_beg >= nxt["end_sec"]:
            logger.warning(f"{nxt.get('reg_id')}: после клипа интервал выродился, интерес будет отброшен: {nxt.get('name')}")
            return False

    # Обновляем секунды
    nxt["beg_sec"] = new_beg
    # Обновляем timestamps
    if isinstance(current.get("end_time"), str):
        cur_end_time = _try_parse_dt(current["end_time"])
    else:
        cur_end_time = current.get("end_time")

    if isinstance(nxt.get("start_time"), str):
        nxt_start_time = _try_parse_dt(nxt["start_time"])
    else:
        nxt_start_time = nxt.get("start_time")

    # Если можем — выставим start_time = max(старого, current.end_time + eps)
    if cur_end_time:
        new_start_time = cur_end_time + datetime.timedelta(seconds=eps_sec)
        if not nxt_start_time or new_start_time > nxt_start_time:
            nxt["start_time"] = new_start_time
    # Фиксируем фото-«до»
    nxt["photo_before_sec"] = max(nxt.get("photo_before_sec", nxt["beg_sec"]), nxt["beg_sec"])
    st = nxt.get("start_time")
    if isinstance(st, datetime.datetime):
        nxt["photo_before_timestamp"] = st.strftime(DT_FMT)
    elif isinstance(st, str):
        # уже строка — оставим
        nxt["photo_before_timestamp"] = st
    else:
        # fallback: используем end_time - безопасно, но лучше, чем пусто
        et = nxt.get("end_time")
        if isinstance(et, datetime.datetime):
            nxt["photo_before_timestamp"] = et.strftime(DT_FMT)

    # Пересоберём имя на основании нового старта
    try:
        plate, date, _, end_str = functions.parse_interest_name(nxt["name"])
        # формируем start из start_time, если есть, иначе из photo_before_timestamp
        st_dt = nxt["start_time"] if isinstance(nxt.get("start_time"), datetime.datetime) else _try_parse_dt(nxt.get("photo_before_timestamp"))
        if st_dt:
            start_str = st_dt.strftime("%H.%M.%S")
            nxt["name"] = functions.build_interest_name(plate, date, start_str, end_str)
    except Exception:
        # если парсер имени упал — не критично, просто оставим как есть
        pass

    return True

def merge_overlapping_interests(interests: List[dict]) -> List[dict]:
    if not interests:
        return []

    sorted_interests = sorted(interests, key=lambda x: x['beg_sec'])
    merged: List[dict] = []

    current = sorted_interests[0].copy()
    for nxt in sorted_interests[1:]:
        # Пересечение по времени?
        if nxt['beg_sec'] <= current['end_sec']:
            # Проверяем: это одна switch-группа или нет?
            if _same_switch_bucket(current, nxt):
                logger.info(
                    f"{current.get('reg_id')}: Пересечение интересов одной switch-группы "
                    f"{current.get('name')} и {nxt.get('name')} → объединяем"
                )
                # ОБЪЕДИНЕНИЕ (как было)
                current['beg_sec'] = min(current['beg_sec'], nxt['beg_sec'])
                current['end_sec'] = max(current['end_sec'], nxt['end_sec'])

                current['start_time'] = min(current['start_time'], nxt['start_time'])
                current['end_time'] = max(current['end_time'], nxt['end_time'])

                current['photo_before_timestamp'] = min(
                    current.get('photo_before_timestamp', current['start_time']),
                    nxt.get('photo_before_timestamp', nxt['start_time'])
                )
                current['photo_after_timestamp'] = max(
                    current.get('photo_after_timestamp', current['end_time']),
                    nxt.get('photo_after_timestamp', nxt['end_time'])
                )

                current['photo_before_sec'] = min(
                    current.get('photo_before_sec', current['beg_sec']),
                    nxt.get('photo_before_sec', nxt['beg_sec'])
                )
                current['photo_after_sec'] = max(
                    current.get('photo_after_sec', current['end_sec']),
                    nxt.get('photo_after_sec', nxt['end_sec'])
                )

                # Объединяем события переключателей
                if 'report' in current and 'report' in nxt:
                    cur_sw = current['report'].get('switch_events', []) or []
                    nxt_sw = nxt['report'].get('switch_events', []) or []
                    merged_sw = cur_sw + nxt_sw
                    merged_sw.sort(key=lambda x: x.get('datetime'))
                    current['report']['switch_events'] = merged_sw
                    current['report']['switches_amount'] = len(merged_sw)

                # Имя из min(start) .. max(end)
                try:
                    plate, date, start_str, _ = functions.parse_interest_name(current['name'])
                    _, _, _, end_str = functions.parse_interest_name(nxt['name'])
                    current['name'] = functions.build_interest_name(plate, date, start_str, end_str)
                except Exception:
                    pass

                logger.info(f"{current.get('reg_id')}: Объединенный интерес — {current.get('name')}")
            else:
                # РАЗНЫЕ switch-группы — НЕ объединяем, а клипуем следующий
                logger.info(
                    f"{current.get('reg_id')}: Пересечение интересов разных switch-групп "
                    f"{current.get('name')} и {nxt.get('name')} → клип следующего"
                )
                nxt = nxt.copy()
                ok = _clip_next_to_current_end(current, nxt, eps_sec=CLIP_EPS_SEC)
                if ok:
                    # текущий остаётся, следующий поправлен — кладём current и переходим к nxt
                    merged.append(current)
                    current = nxt
                else:
                    # следующий выродился — просто пропускаем его
                    logger.warning(f"{nxt.get('reg_id')}: следующий интерес удалён из-за нулевой длительности после клипа")
                    # current без изменений
        else:
            # Нет пересечения — фиксируем текущий и идём дальше
            merged.append(current)
            current = nxt.copy()

    merged.append(current)
    return merged
