import os, json, tempfile, fcntl, logging
from qt_pvp import settings

logger = logging.getLogger(__name__)

STATES_PATH = settings.states
LOCK_PATH = STATES_PATH + ".lock"

class FileLock:
    def __init__(self, path): self.path = path; self.fd = None
    def __enter__(self):
        self.fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o666)
        fcntl.flock(self.fd, fcntl.LOCK_EX)
        return self
    def __exit__(self, exc_type, exc, tb):
        fcntl.flock(self.fd, fcntl.LOCK_UN)
        os.close(self.fd)

def _load_states():
    with open(STATES_PATH, "r") as f:
        return json.load(f)

def _atomic_save_states(states: dict):
    d = os.path.dirname(STATES_PATH) or "."
    with tempfile.NamedTemporaryFile("w", dir=d, delete=False) as tmp:
        json.dump(states, tmp, indent=4, ensure_ascii=False)
        tmp.flush(); os.fsync(tmp.fileno())
        tmp_name = tmp.name
    os.replace(tmp_name, STATES_PATH)  # атомарно
