import requests
from typing import Optional, Dict, Any
from main_operator import Main


def get_device_alarms(
    base_url: str,
    jsession: str,
    dev_idno: Optional[str] = None,
    vehi_idno: Optional[str] = None,
    begintime: str = "",
    endtime: str = "",
    arm_type: str = "19,20,21,22",
    handle: Optional[int] = None,
    current_page: int = 1,
    page_records: int = 50,
    to_map: Optional[int] = None,
    timeout: int = 30,
) -> Dict[str, Any]:
    """
    Получение списка алармов через StandardApiAction_queryAlarmDetail.action
    base_url: например, "http://82.146.45.88:8080"
    jsession: токен сессии (из StandardApiAction_login.action)
    dev_idno: ID устройства (DevIDNO), можно несколько через запятую
    vehi_idno: госномер, если dev_idno не указан
    begintime, endtime: 'YYYY-MM-DD HH:MM:SS'
    arm_type: строка с типами алармов через запятую, обязательно!
    handle: 0 - необработанные, 1 - обработанные, None - все
    """
    url = f"{base_url.rstrip('/')}/StandardApiAction_queryAlarmDetail.action"

    params = {
        "jsession": jsession,
        "begintime": begintime,
        "endtime": endtime,
        "armType": arm_type,
        "currentPage": current_page,
        "pageRecords": page_records,
    }
    if dev_idno:
        params["devIdno"] = dev_idno
    if vehi_idno:
        params["vehiIdno"] = vehi_idno
    if handle is not None:
        params["handle"] = str(handle)
    if to_map is not None:
        params["toMap"] = str(to_map)

    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if data.get("result") != 0:
        raise RuntimeError(f"API error result={data.get('result')}: {data}")

    return data

# === пример использования ===
if __name__ == "__main__":
    BASE_URL = "http://82.146.45.88:8080"
    DEV_IDNO = "108411"  # твой регистратор
    START = "2025-08-17 10:33:00"
    END = "2025-08-17 10:45:59"
    ARM_TYPES = "19,20,21,22"

    inst = Main()
    JSESSION  = inst.jsession
    alarms = get_device_alarms(BASE_URL, JSESSION,
                               dev_idno=DEV_IDNO,
                               begintime=START, endtime=END,
                               arm_type=ARM_TYPES,
                               handle=0,
                               page_records=50)

    print("Всего записей:", alarms.get("pagination", {}).get("totalRecords"))
    for alarm in alarms.get("alarms", []):
        print(alarm)
