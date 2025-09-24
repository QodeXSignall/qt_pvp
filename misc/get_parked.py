from main_operator import Main
from typing import Optional, Dict, Any
import requests


def get_vehicle_parks(
    base_url: str,
    jsession: str,
    vehi_idno: Optional[str],
    begintime: str,
    endtime: str,
    park_time: int = 0,
    to_map: int = 1,
    current_page: int = 1,
    page_records: int = 50,
    geoaddress: Optional[int] = None,
    timeout: int = 30,
) -> Dict[str, Any]:
    """
    Вызов StandardApiAction_parkDetail.action
    base_url: например, "http://82.146.45.88:8080"
    jsession: токен из StandardApiAction_login.action
    vehi_idno: госномер/ID устройства (строка)
    begintime, endtime: 'YYYY-MM-DD HH:MM:SS'
    park_time: минимальная длительность парковки (в секундах)
    to_map: 1 = Google, 2 = Baidu
    """
    url = f"{base_url.rstrip('/')}/StandardApiAction_parkDetail.action"

    params = {
        "jsession": jsession,
        "begintime": begintime,
        "endtime": endtime,
        "parkTime": park_time,
        "toMap": to_map,
        "currentPage": current_page,
        "pageRecords": page_records,
    }
    if vehi_idno:
        params["vehiIdno"] = vehi_idno
    if geoaddress is not None:
        params["geoaddress"] = geoaddress

    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if data.get("result") != 0:
        raise RuntimeError(f"API error result={data.get('result')}: {data}")
    return data


# === пример использования ===
if __name__ == "__main__":
    inst = Main()
    JSESSION = inst.jsession
    BASE = "http://82.146.45.88:8080"
    VEHI_IDNO = "A939CA702"  # номер машины или devIdno
    START = "2025-08-17 10:30:00"
    END = "2025-08-17 10:46:59"

    parks = get_vehicle_parks(BASE, JSESSION, VEHI_IDNO, START, END, park_time=0)
    print("Всего парковок:", parks.get("pagination", {}).get("totalRecords"))
    for park in parks.get("infos", []):
        print(park)
