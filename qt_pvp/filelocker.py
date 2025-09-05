import logging
import portalocker   # вместо fcntl

logger = logging.getLogger(__name__)
LOCK_PATH = "states.lock"

class FileLock:
    def __init__(self, path):
        self.path = path
        self.fd = None

    def __enter__(self):
        self.fd = open(self.path, "a+")
        portalocker.lock(self.fd, portalocker.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        portalocker.unlock(self.fd)
        self.fd.close()
