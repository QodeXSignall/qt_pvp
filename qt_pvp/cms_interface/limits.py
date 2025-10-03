from qt_pvp.settings import config
from qt_pvp.logger import logger
from typing import Dict
import asyncio


def _safe_int(section: str, key: str, default: int) -> int:
    try:
        v = int(config.get(section, key, fallback=str(default)))
        if v < 1:
            logger.warning(f"[{section}] {key}={v} < 1, принудительно -> 1")
            return 1
        return v
    except Exception as e:
        logger.warning(f"[{section}] {key}: ошибка чтения ({e}), fallback -> {default}")
        return default

# ЛЕНИВАЯ инициализация, чтобы не зависеть от порядка импорта настроек
_global_cms_sem: asyncio.Semaphore | None = None
_device_sems: Dict[str, asyncio.Semaphore] = {}

_frame_sem: asyncio.Semaphore | None = None


def get_cms_global_sem() -> asyncio.Semaphore:
    global _global_cms_sem
    if _global_cms_sem is None:
        max_conc = _safe_int("Process", "MAX_CMS_CONCURRENT", 8)
        _global_cms_sem = asyncio.BoundedSemaphore(max_conc)
    return _global_cms_sem


def get_device_sem(device_id: str) -> asyncio.Semaphore:
    """
    Пер-устройство семафор. Если устройств очень много, подумай об удалении
    старых ключей вручную (например, по событию «устройство исчезло»),
    чтобы не раздувать словарь.
    """
    sem = _device_sems.get(device_id)
    if sem is None:
        per_dev = _safe_int("Process", "MAX_CMS_PER_DEVICE", 2)
        sem = asyncio.BoundedSemaphore(per_dev)
        _device_sems[device_id] = sem
    return sem


def get_frame_sem() -> asyncio.Semaphore:
    global _frame_sem
    if _frame_sem is None:
        max_frames = _safe_int("Process", "MAX_FRAME_EXTRACT", 4)
        _frame_sem = asyncio.BoundedSemaphore(max_frames)
    return _frame_sem
