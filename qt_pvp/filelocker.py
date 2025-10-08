import os
import json
import tempfile
import logging

logger = logging.getLogger(__name__)

# --- Resolve settings.states path -------------------------------------------------
# Пытаемся взять путь к states из твоего settings. Если не выйдет — используем env/дефолт.

from qt_pvp.data import settings as _settings

if _settings and getattr(_settings, "states", None):
    STATES_PATH = _settings.states
else:
    STATES_PATH = os.environ.get("QT_PVP_STATES_PATH", os.path.abspath("data/states.json"))

LOCK_PATH = STATES_PATH + ".lock"

# --- Cross-platform file lock -----------------------------------------------------
# Пытаемся использовать portalocker. Если его нет — fallback на msvcrt/fcntl.
_HAVE_PORTALOCKER = False
try:
    import portalocker  # type: ignore
    _HAVE_PORTALOCKER = True
except Exception:
    portalocker = None  # type: ignore

if os.name == "nt":
    # Windows fallback
    try:
        import msvcrt  # type: ignore
    except Exception as e:
        raise RuntimeError("On Windows you need either portalocker or msvcrt") from e
else:
    # POSIX fallback
    try:
        import fcntl  # type: ignore
    except Exception as e:
        raise RuntimeError("On POSIX you need either portalocker or fcntl") from e


class FileLock:
    """
    Межпроцессный эксклюзивный лок на отдельный lock-файл.
    Использует:
      - portalocker (если установлен), иначе
      - msvcrt.locking на Windows, иначе
      - fcntl.flock на POSIX.
    """
    def __init__(self, path: str):
        self.path = path
        self._fh = None

    def __enter__(self):
        # бинарный режим, чтобы одинаково работать с msvcrt/portalocker
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self._fh = open(self.path, "a+b")
        if _HAVE_PORTALOCKER:
            portalocker.lock(self._fh, portalocker.LOCK_EX)
        else:
            if os.name == "nt":
                # Лочим 1 байт; для lock-файла этого достаточно
                self._fh.seek(0)
                try:
                    msvcrt.locking(self._fh.fileno(), msvcrt.LK_LOCK, 1)
                except OSError:
                    # создаём файл, если пустой
                    self._fh.write(b"\0")
                    self._fh.flush()
                    self._fh.seek(0)
                    msvcrt.locking(self._fh.fileno(), msvcrt.LK_LOCK, 1)
            else:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if _HAVE_PORTALOCKER:
                portalocker.unlock(self._fh)
            else:
                if os.name == "nt":
                    self._fh.seek(0)
                    try:
                        msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
                    except OSError:
                        pass
                else:
                    fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        finally:
            try:
                self._fh.close()
            except Exception:
                pass


# --- Safe load/save of states -----------------------------------------------------

def _load_states() -> dict:
    """
    Безопасная загрузка JSON-состояний.
    Если файл отсутствует — вернём минимальную структуру.
    """
    try:
        with open(STATES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"regs": {}}
    except json.JSONDecodeError as e:
        # Коррупт. Логируем и пробуем не дать упасть — отдаём пустую структуру.
        logger.error("states.json is corrupted: %s", e)
        return {"regs": {}}


def _atomic_save_states(states: dict) -> None:
    """
    Атомарная запись JSON:
      1) пишем во временный файл в той же директории
      2) fsync
      3) os.replace -> атомарная подмена целевого файла
    """
    dir_ = os.path.dirname(STATES_PATH) or "."
    os.makedirs(dir_, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(prefix=".states.", suffix=".tmp", dir=dir_)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as tmp:
            json.dump(states, tmp, indent=4, ensure_ascii=False)
            tmp.flush()
            os.fsync(tmp.fileno())
        os.replace(tmp_path, STATES_PATH)
    finally:
        # если replace не сработал — подчистим tmp
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass


__all__ = ["FileLock", "_load_states", "_atomic_save_states", "LOCK_PATH", "STATES_PATH"]
