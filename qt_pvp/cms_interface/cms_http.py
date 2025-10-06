import httpx
from httpx import Timeout

_client_async: httpx.AsyncClient | None = None

def get_cms_async_client() -> httpx.AsyncClient:
    global _client_async
    if _client_async is None:
        _client_async = httpx.AsyncClient(
            limits=httpx.Limits(
                max_connections=32,
                max_keepalive_connections=16,
            ),
            timeout=Timeout(360.0, connect=5.0),
            headers={"User-Agent": "qt_pvp/1.0", "Connection": "close"},
            http2=False,
        )
    return _client_async


async def close_cms_async_client():
    global _client_async
    if _client_async is not None:
        await _client_async.aclose()
        _client_async = None