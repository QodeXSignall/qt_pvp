import os
import asyncio
import datetime
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, validator
from webdav3.client import Client
from webdav3.exceptions import RemoteResourceNotFound

from qt_pvp import functions as main_funcs
from qt_pvp.interest_merge_funcs import merge_overlapping_interests
from qt_pvp.cms_interface import functions as cms_funcs
from main_operator import Main


WEBDAV_OPTIONS = {
    "webdav_hostname": os.environ.get("webdav_hostname"),
    "webdav_login": os.environ.get("webdav_login"),
    "webdav_password": os.environ.get("webdav_password"),
}

TIME_FMT_FN = "%Y.%m.%d %H.%M.%S"   # для имён папок
TIME_FMT_DAY = "%Y.%m.%d"          # входной формат даты с точками
TIME_FMT = "%Y-%m-%d %H:%M:%S"     # для запросов CMS


def _validate_webdav_options():
    missing = [k for k, v in WEBDAV_OPTIONS.items() if not v]
    if missing:
        raise HTTPException(status_code=500, detail=f"Missing WebDAV env vars: {', '.join(missing)}")


def list_interest_folders(client: Client, base_path: str, plate: str, day_str: str) -> List[str]:
    day_path = f"{base_path}/{plate}/{day_str}"
    items = client.list(day_path)
    names = []
    for item in items:
        name = item.rstrip("/").split("/")[-1]
        if "_" in name and "-" in name and "." in name:
            names.append(name)
    return sorted(names)


def parse_folder_name(name: str):
    plate, rest = name.split("_", 1)
    left, right = rest.split("-")
    start_dt = datetime.datetime.strptime(left.strip(), TIME_FMT_FN)
    date_prefix = start_dt.strftime("%Y.%m.%d")
    end_dt = datetime.datetime.strptime(f"{date_prefix} {right.strip()}", TIME_FMT_FN)
    return plate, start_dt, end_dt


def fuzzy_equal(n1: str, n2: str, eps_sec: int = 10) -> bool:
    try:
        p1, s1, e1 = parse_folder_name(n1)
        p2, s2, e2 = parse_folder_name(n2)
    except Exception:
        return False
    return (
        p1 == p2 and
        abs((s1 - s2).total_seconds()) <= eps_sec and
        abs((e1 - e2).total_seconds()) <= eps_sec
    )


def diff_sets(expected, detected, eps_sec: int = 0):
    new = set(detected)
    missing = set(expected)
    if eps_sec <= 0:
        return new - expected, missing - detected

    matched_exp = set()
    matched_det = set()

    for e in expected:
        for d in detected:
            if d in matched_det:
                continue

            if e == d:
                matched_exp.add(e)
                matched_det.add(d)
                break

            if fuzzy_equal(e, d, eps_sec=eps_sec):
                matched_exp.add(e)
                matched_det.add(d)
                break

    new -= matched_det
    missing -= matched_exp
    return new, missing


class CompareRequest(BaseModel):
    reg_id: str = Field(..., description="DevIDNO регистратора")
    day: str = Field(..., description="Дата в формате YYYY.MM.DD")
    base_path: str = Field("/Tracker/Видео выгрузок", description="Базовый путь на WebDAV")

    @validator("day")
    def _check_day(cls, v):
        try:
            datetime.datetime.strptime(v, TIME_FMT_DAY)
        except Exception as e:
            raise ValueError(f"day must be YYYY.MM.DD: {e}")
        return v


class InterestRequest(BaseModel):
    reg_id: str
    start_time: str = Field(..., description="YYYY-MM-DD HH:MM:SS")
    end_time: str = Field(..., description="YYYY-MM-DD HH:MM:SS")
    merge_overlaps: bool = True

    @validator("start_time", "end_time")
    def _check_ts(cls, v):
        try:
            datetime.datetime.strptime(v, TIME_FMT)
        except Exception as e:
            raise ValueError(f"time must be {TIME_FMT}: {e}")
        return v


class SiteItem(BaseModel):
    id: str
    lat: float
    lon: float


class StopsRequest(BaseModel):
    reg_id: str
    date: str = Field(..., description="Дата в формате YYYY-MM-DD")
    sites: List[SiteItem]
    radius_m: float = 120.0

    @validator("date")
    def _check_date(cls, v):
        try:
            datetime.datetime.strptime(v, "%Y-%m-%d")
        except Exception as e:
            raise ValueError(f"date must be YYYY-MM-DD: {e}")
        return v


app = FastAPI(title="qt_pvp API")


async def _get_main_logged_in() -> Main:
    m = Main()
    await m.login()
    return m


@app.post("/compare-interests")
async def compare_interests(req: CompareRequest):
    _validate_webdav_options()
    client = Client(WEBDAV_OPTIONS)

    reg_info = main_funcs.get_reg_info(req.reg_id) or {}
    plate = reg_info.get("plate") or req.reg_id

    try:
        folder_names = list_interest_folders(client, req.base_path, plate, req.day)
    except RemoteResourceNotFound:
        raise HTTPException(status_code=404, detail=f"Day {req.day} not found in cloud for plate {plate}")

    # Конвертация даты в формат CMS
    day_dt = datetime.datetime.strptime(req.day, TIME_FMT_DAY).date()
    start_time = f"{day_dt.strftime('%Y-%m-%d')} 00:00:00"
    stop_time = f"{day_dt.strftime('%Y-%m-%d')} 23:59:59"

    m = await _get_main_logged_in()
    reg_info_full = main_funcs.get_reg_info(req.reg_id)
    interests = await m.get_interests_async(
        reg_id=req.reg_id,
        reg_info=reg_info_full,
        start_time=start_time,
        stop_time=stop_time,
    )
    interests = merge_overlapping_interests(interests)
    detected_names = set(i["name"] for i in interests)
    expected_names = set(folder_names)

    new_fuzzy, missing_fuzzy = diff_sets(expected_names, detected_names, eps_sec=30)

    return {
        "cloud_total": len(folder_names),
        "detected_total": len(interests),
        "new_not_in_cloud": sorted(new_fuzzy),
        "missing_in_detected": sorted(missing_fuzzy),
    }


@app.post("/get-interests")
async def get_interests_api(req: InterestRequest):
    m = await _get_main_logged_in()
    reg_info_full = main_funcs.get_reg_info(req.reg_id)
    interests = await m.get_interests_async(
        reg_id=req.reg_id,
        reg_info=reg_info_full,
        start_time=req.start_time,
        stop_time=req.end_time,
    )
    if req.merge_overlaps:
        interests = merge_overlapping_interests(interests)
    return {"count": len(interests), "interests": interests}


@app.post("/find-stops")
async def find_stops_api(req: StopsRequest):
    m = await _get_main_logged_in()
    res = await cms_funcs.find_stops_near_sites_by_date(
        reg_id=req.reg_id,
        sites=[s.dict() for s in req.sites],
        date=req.date,
        radius_m=req.radius_m,
        jsession=m.jsession,
    )
    return res


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("qt_pvp.api:app", host="0.0.0.0", port=8001, reload=False)

