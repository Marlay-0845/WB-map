"""Источник WB Карта ПВЗ (map.wb.ru): можно ли открыть ПВЗ в точке и отчёт по точке.
 
Как работает:
1. Сначала пробуем обычные запросы httpx (вдруг токен не нужен).
2. Если сайт отвечает 401/403/498 — получаем токен через настоящий Chrome
   (patchright), сохраняем в файл и повторяем запрос. Один раз на запрос.
3. Дальше данные берём быстрыми запросами httpx с этим токеном.
 
Что умеет:
    check_point(lat, lon)      — можно ли открыть ПВЗ в точке
    report(lat, lon, radius)   — отчёт по точке (пока сырой JSON)
    check_many(points)         — много точек подряд, с паузами"""

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode
 
import httpx
 
from .base import BlockedError, SourceError
 
log = logging.getLogger(__name__)
 
SOURCE = "wb_map"
SITE = "https://map.wb.ru"
CHECK_POINT = "/pvz-map-backend/api/v2/map/check-point"
REPORT = "/pvz-map-backend/api/v1/get-report-data"
 
TOKEN_COOKIE = "x_wbaas_token"
DATA_REQUEST_MARK = "/pvz-map-backend/api/"
BLOCKED_STATUSES = {401, 403, 498}
 
DEFAULT_AREA = 70
DEFAULT_TARIFF = "BasicTariff"
DEFAULT_RADIUS = 300
 
DEFAULT_CACHE_TTL = 24 * 3600
BLOCK_COOLDOWN = 15 * 60
DEFAULT_PAUSE = (3.0, 6.0)
 
SKIP_HEADERS = {"cookie", "host", "content-length"}
 
 
# --- разбор ответов (без сети) ---
 
@dataclass(frozen=True)
class PointCheck:
    lat: float
    lon: float
    can_open: bool
    description: str
    min_area: int | None
    subsidies_info: str | None
    raw: dict
 
 
def parse_check(data: dict, lat: float, lon: float) -> PointCheck:
    area_info = (data.get("point_info") or {}).get("area_info") or {}
    return PointCheck(
        lat=lat,
        lon=lon,
        can_open=bool(data.get("can_open")),
        description=data.get("description", ""),
        min_area=area_info.get("min_area"),
        subsidies_info=area_info.get("subsidies_info"),
        raw=data,
    )
 
 
# --- кэш ---
 
class ResponseCache:
    def __init__(self, ttl: float = DEFAULT_CACHE_TTL, file: Path | None = None):
        self._ttl = ttl
        self._file = file
        self._items: dict[str, list] = {}
        if file is not None:
            try:
                self._items = json.loads(file.read_text(encoding="utf-8"))
            except (FileNotFoundError, json.JSONDecodeError):
                self._items = {}
 
    def get(self, key: str) -> dict | None:
        item = self._items.get(key)
        if item is None:
            return None
        saved_at, data = item
        if time.time() - saved_at > self._ttl:
            del self._items[key]
            return None
        return data
 
    def put(self, key: str, data: dict) -> None:
        self._items[key] = [time.time(), data]
        self._save()
 
    def _save(self) -> None:
        if self._file is None:
            return
        self._file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._file.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._items, ensure_ascii=False), encoding="utf-8")
        tmp.replace(self._file)  
 
 
# --- работа с сайтом ---
 
class WBMapSource:
    name = SOURCE
 
    def __init__(self, session_file: Path, profile_dir: Path,
                 cache_file: Path | None = None,
                 cache_ttl: float = DEFAULT_CACHE_TTL,
                 transport: httpx.AsyncBaseTransport | None = None,
                 pause: tuple[float, float] = DEFAULT_PAUSE):
        self.session_file = session_file
        self.profile_dir = profile_dir
        self._cache = ResponseCache(cache_ttl, cache_file)
        self._gate = asyncio.Lock()
        self._last_request = float("-inf")
        self._blocked_until = 0.0
        # transport — «подмена сети» для тестов (httpx.MockTransport). В работе None
        self._transport = transport
        self._pause = pause
        self._retry_delay = 3 if pause[1] > 0 else 0
        self.session = self._load_session()
        self._refresh_lock = asyncio.Lock()
 
    # --- сессия (токен + заголовки) ---
 
    def _load_session(self) -> dict | None:
        try:
            return json.loads(self.session_file.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            return None
 
    def _save_session(self, session: dict) -> None:
        self.session_file.parent.mkdir(parents=True, exist_ok=True)
        self.session_file.write_text(json.dumps(session, ensure_ascii=False, indent=2),
                                     encoding="utf-8")
 
    async def _refresh_session(self, page_url: str, stale: dict | None) -> None:
        async with self._refresh_lock:
            # Пока ждали блокировку, сессию мог уже обновить другой запрос
            if self.session is not stale and self.session is not None:
                return
            log.info("WB Map: получаю новый антибот-токен через браузер")
            self.session = await self._session_from_browser(page_url)
            self._save_session(self.session)
 
    async def _session_from_browser(self, page_url: str) -> dict:
        from patchright.async_api import async_playwright
 
        headers: dict = {}
 
        def on_request(request):
            if not headers and DATA_REQUEST_MARK in request.url:
                headers.update({k: v for k, v in request.headers.items()
                                if k.lower() not in SKIP_HEADERS and not k.startswith(":")})
 
        async with async_playwright() as p:
            context = await p.chromium.launch_persistent_context(
                user_data_dir=str(self.profile_dir),
                channel="chrome",
                headless=False,
                no_viewport=True,
            )
            try:
                page = context.pages[0] if context.pages else await context.new_page()
                page.on("request", on_request)
                await page.goto(page_url, wait_until="domcontentloaded", timeout=60_000)
                cookies = {}
                for _ in range(90): 
                    cookies = {c["name"]: c["value"] for c in await context.cookies()
                               if c["domain"].endswith("wb.ru")}
                    if TOKEN_COOKIE in cookies:
                        break
                    await asyncio.sleep(1)
                if not headers:
                    user_agent = await page.evaluate("navigator.userAgent")
                    headers.update({
                        "user-agent": user_agent,
                        "accept": "application/json, text/plain, */*",
                        "accept-language": "ru-RU,ru;q=0.9",
                        "origin": SITE,
                        "referer": SITE + "/",
                    })
            finally:
                await context.close()
 
        if TOKEN_COOKIE not in cookies:
            raise BlockedError("WB Map не выдал антибот-токен: браузер не прошёл проверку")
        return {"cookies": cookies, "headers": headers}
 
    # --- запросы ---
 
    @staticmethod
    def _point_params(lat: float, lon: float, **extra) -> dict:
        """Координаты в порядке и с именами, как требует WB (долгота первой)"""
        return {"point.longitude": str(round(lon, 5)),
                "point.latitude": str(round(lat, 5)), **extra}
 
    def _default_headers(self) -> dict:
        return {
            "user-agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"),
            "accept": "application/json, text/plain, */*",
            "accept-language": "ru-RU,ru;q=0.9",
            "origin": SITE,
            "referer": SITE + "/",
        }
 
    async def _send(self, path: str, params: dict) -> httpx.Response:
        """Отправить запрос с текущей сессией. Каждый раз - новое соединение:
        после отказа сайта старое соединение может испортиться"""
        session = self.session
        if session is None:
            headers = self._default_headers()
        else:
            cookie = "; ".join(f"{k}={v}" for k, v in session["cookies"].items())
            headers = {**session["headers"], "cookie": cookie}
        for attempt in (1, 2):
            try:
                async with self._client() as client:
                    return await client.get(SITE + path, params=params, headers=headers)
            except httpx.TransportError as e:
                if attempt == 2:
                    raise SourceError(f"WB Map: ошибка сети ({e.__class__.__name__})") from e
                log.info("WB Map: сбой соединения (%s), повторяю", e.__class__.__name__)
                await asyncio.sleep(self._retry_delay)
 
    def cooldown_left(self) -> float:
        """Сколько секунд ещё не стоит трогать сайт после блокировки"""
        return max(0.0, self._blocked_until - time.monotonic())
 
    async def _get_json(self, path: str, params: dict) -> dict:
        """Ответ по запросу: из кэша, а если там нет - с сайта, спокойным темпом"""
        key = f"{path}?{urlencode(sorted(params.items()))}"
 
        cached = self._cache.get(key)
        if cached is not None:
            return cached
         
        async with self._gate:
            cached = self._cache.get(key)
            if cached is not None:
                return cached
 
            if self.cooldown_left() > 0:
                raise BlockedError(f"WB Map: пауза после блокировки, "
                                   f"ещё {self.cooldown_left():.0f} с")
 
            low, high = self._pause
            wait = self._last_request + random.uniform(low, high) - time.monotonic()
            if high > 0 and wait > 0:
                await asyncio.sleep(wait)
 
            try:
                data = await self._fetch_json(path, params)
            except BlockedError:
                self._blocked_until = time.monotonic() + BLOCK_COOLDOWN
                raise
            finally:
                self._last_request = time.monotonic()
 
            self._cache.put(key, data) 
            return data
 
    async def _fetch_json(self, path: str, params: dict) -> dict:
        """Один запрос к сайту с обработкой протухшего токена
 
        Сначала пробуем без браузера
        При отказе открываем браузер на самом адресе запроса, получаем токен
        и повторяем
        """
        page_url = f"{SITE}{path}?{urlencode(params)}"
 
        for attempt in (1, 2):
            session = self.session
            response = await self._send(path, params)
            if response.status_code not in BLOCKED_STATUSES:
                break
            if attempt == 2:
                raise BlockedError(f"WB Map не пускает даже с новым токеном "
                                   f"({response.status_code})")
            log.info("WB Map: отказ (%s), получаю токен", response.status_code)
            await self._refresh_session(page_url, stale=session)
 
        if response.status_code != 200:
            raise SourceError(f"WB Map ответил {response.status_code}")
        try:
            return response.json()
        except ValueError:
            raise SourceError("WB Map прислал не JSON - возможно, изменился формат ответа")
 
    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(timeout=15, transport=self._transport)
 
    # --- что умеет ---
 
    async def check_point(self, lat: float, lon: float,
                          area: int = DEFAULT_AREA,
                          tariff_type: str = DEFAULT_TARIFF) -> PointCheck:
        """Можно ли открыть ПВЗ в этой точке"""
        params = self._point_params(lat, lon, area=str(area), tariff_type=tariff_type)
        data = await self._get_json(CHECK_POINT, params)
        return parse_check(data, lat, lon)
 
    async def report(self, lat: float, lon: float, radius: int = DEFAULT_RADIUS) -> dict:
        """Отчёт по точке"""
        params = self._point_params(lat, lon, radius=str(radius))
        return await self._get_json(REPORT, params)
 
    async def check_many(self, points: list[tuple[float, float]]) -> list[PointCheck]:
        """Много точек подряд"""
        return [await self.check_point(lat, lon) for lat, lon in points]
 
 
# --- пример: обработка очереди ---
 
async def run_worker(source: WBMapSource, queue: "asyncio.Queue[tuple[float, float]]",
                     handle) -> None:
    while True:
        lat, lon = await queue.get()
        try:
            await handle(await source.check_point(lat, lon))
        except BlockedError:
            await queue.put((lat, lon))
            await asyncio.sleep(source.cooldown_left() + 1)
        except SourceError as e:
            log.warning("WB Map: точка %s, %s пропущена: %s", lat, lon, e)
        finally:
            queue.task_done()


# --- ручная проверка ---
 
async def _demo(lat, lon) -> None:
    source = WBMapSource(Path("data/wb_map_session.json"), Path("data/wb_map_browser_profile"))
 
    check = await source.check_point(lat, lon)
    print(f"Можно открыть: {'да' if check.can_open else 'нет'}")
    print(f"Описание: {check.description}")
    print(f"Минимальная площадь: {check.min_area}")
    print(f"Subsidies: {check.subsidies_info}")
    # print(check.raw)
 
    report = await source.report(lat, lon)
    print("Отчёт:")
    # print(json.dumps(report, ensure_ascii=False, indent=2)[:2000])
    print(report["count_people"])
    print(report["point_data"]["features"])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    asyncio.run(_demo(55.771722, 37.525756))
