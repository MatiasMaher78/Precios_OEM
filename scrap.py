import os
import time
import argparse
import random
import string
import re
import base64
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Optional, Set, List, Tuple
from pathlib import Path
import shutil
import json
import asyncio
import threading

import pandas as pd

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
except Exception:
    sync_playwright = None  # type: ignore
    PlaywrightTimeoutError = Exception  # type: ignore


DEFAULT_BATCH = 1000


# ----------------------------
# Folder structure management
# ----------------------------
def setup_scrap_folders(base_folder: str) -> Dict[str, str]:
    base = Path(base_folder)
    folders = {"input": base / "Input", "output": base / "Output", "done": base / "Done"}
    for _, path in folders.items():
        path.mkdir(parents=True, exist_ok=True)
    return {k: str(v) for k, v in folders.items()}


def locate_input_excel(input_folder: str) -> str:
    """
    DEPRECATED: se mantiene por compatibilidad, pero el flujo principal
    debe usar load_input_df(scrap_folder) que prioriza CSV.
    """
    input_path = Path(input_folder)
    candidates = [f for f in input_path.glob("*.xlsx") if not f.name.startswith("~$") and not f.name.startswith(".")]
    candidates.extend(
        [f for f in input_path.glob("*.xls") if not f.name.startswith("~$") and not f.name.startswith(".")]
    )

    if not candidates:
        raise FileNotFoundError(f"No se encontró ningún archivo Excel en {input_folder}")

    if len(candidates) > 1:
        raise FileExistsError(
            f"Se encontraron varios archivos Excel en {input_folder}: {[f.name for f in candidates]}. "
            "Debe haber exactamente 1 archivo."
        )

    return str(candidates[0])


def extract_date_from_filename(filename: str) -> Optional[str]:
    match = re.search(r"(\d{8})", filename)
    if match:
        return match.group(1)

    match = re.search(r"(\d{4})[-_](\d{2})[-_](\d{2})", filename)
    if match:
        return f"{match.group(1)}{match.group(2)}{match.group(3)}"

    return None


def generate_output_filename(input_filename: str) -> str:
    date_str = extract_date_from_filename(input_filename)
    if not date_str:
        date_str = datetime.now().strftime("%Y%m%d")
    return f"Output_{date_str}.xlsx"


# ----------------------------
# Input helpers (CSV-first)
# ----------------------------
def _detect_delimiter(sample_path: Path) -> str:
    """
    Detección simple de delimitador para CSV (',' vs ';').
    """
    try:
        head = sample_path.read_text(encoding="utf-8", errors="ignore")[:4096]
    except Exception:
        head = sample_path.read_text(encoding="latin-1", errors="ignore")[:4096]

    # Heurística: en ES es común ';' en CSV exportados por Excel
    if head.count(";") > head.count(","):
        return ";"
    return ","


def pick_input_file(input_dir: Path) -> Path:
    """
    Prioriza CSV para evitar dependencia openpyxl en entornos sin red.
    Si no hay CSV, usa XLSX/XLS.
    """
    if not input_dir.exists():
        raise FileNotFoundError(f"No existe la carpeta Input: {input_dir}")

    csvs = sorted([p for p in input_dir.glob("*.csv") if p.is_file() and not p.name.startswith("~$")])
    if csvs:
        return csvs[0]

    xlsxs = sorted([p for p in input_dir.glob("*.xlsx") if p.is_file() and not p.name.startswith("~$")])
    if xlsxs:
        return xlsxs[0]

    xls = sorted([p for p in input_dir.glob("*.xls") if p.is_file() and not p.name.startswith("~$")])
    if xls:
        return xls[0]

    raise FileNotFoundError(f"No encontré archivos .csv/.xlsx/.xls en {input_dir}")


def load_input_df(scrap_folder: str) -> pd.DataFrame:
    """
    Carga el input desde <scrap_folder>/Input.
    - Si hay CSV: pd.read_csv (sin openpyxl)
    - Si hay XLSX/XLS: intenta pd.read_excel (requiere openpyxl/xlrd)
    """
    base = Path(scrap_folder)
    input_dir = base / "Input"
    input_file = pick_input_file(input_dir)

    suffix = input_file.suffix.lower()

    if suffix == ".csv":
        sep = _detect_delimiter(input_file)
        # Intentar utf-8, fallback latin-1
        try:
            return pd.read_csv(input_file, sep=sep, dtype=str, keep_default_na=False)
        except UnicodeDecodeError:
            return pd.read_csv(input_file, sep=sep, dtype=str, keep_default_na=False, encoding="latin-1")

    # Excel: requiere openpyxl (xlsx) o xlrd (xls)
    try:
        return pd.read_excel(input_file, sheet_name=0, dtype=str).fillna("")
    except ImportError as e:
        raise ImportError(
            f"Falta dependencia para leer Excel ({input_file.name}). "
            f"Solución recomendada: dejar un CSV en Input/ (ya soportado) "
            f"o instalar openpyxl/xlrd según corresponda."
        ) from e


# ----------------------------
# Input I/O (compat)
# ----------------------------
def read_workbook(path: str) -> pd.DataFrame:
    """
    Compatibilidad retro: si alguien llama read_workbook(), intentamos leer.
    Pero para Codex/CI se debe usar load_input_df(scrap_folder), que prioriza CSV.
    """
    p = Path(path)
    suf = p.suffix.lower()

    if suf == ".csv":
        sep = _detect_delimiter(p)
        try:
            return pd.read_csv(p, sep=sep, dtype=str, keep_default_na=False)
        except UnicodeDecodeError:
            return pd.read_csv(p, sep=sep, dtype=str, keep_default_na=False, encoding="latin-1")

    # Excel (requiere openpyxl/xlrd)
    try:
        return pd.read_excel(p, sheet_name=0, dtype=str).fillna("")
    except ImportError as e:
        raise ImportError(
            f"Falta dependencia para leer Excel ({p.name}). "
            f"Solución: dejar un CSV en Input/ (ya soportado) o instalar openpyxl/xlrd."
        ) from e


def get_col(df: pd.DataFrame, names, fallback_idx: int) -> pd.Series:
    cols = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in cols:
            return df[cols[n.lower()]].astype(str).fillna("")
    if df.shape[1] > fallback_idx:
        return df.iloc[:, fallback_idx].astype(str).fillna("")
    return pd.Series([""] * len(df))


# ----------------------------
# Ecooparts URL builder
# ----------------------------
def _b64(s: str) -> str:
    return base64.b64encode(s.encode("utf-8")).decode("ascii")


def build_ecooparts_search_url(query_text: str, *, page: int = 1, per_page: int = 30) -> str:
    c = str(query_text).strip()
    token = "".join(random.choices(string.ascii_lowercase + string.digits, k=22))

    params = {
        "pag": "pro",
        "busval": _b64(f"|{c}|ninguno|producto|-1|0|0|0|0||0|0|0|0"),
        "filval": "",
        "panu": _b64(str(page)),
        "tebu": _b64(c),
        "ord": _b64("ninguno"),
        "valo": _b64("-1"),
        "ubic": "",
        "toen": _b64(token),
        "veid": _b64("0"),
        "qregx": _b64(str(per_page)),
        "tmin": _b64("1"),
        "ttseu": "",
        "txbu": _b64(c),
        "ivevh": "",
        "ivevhmat": "",
        "ivevhsel": "",
        "ivevhcsver": "",
        "ivevhse": "",
        "oem": "",
        "vin": "",
    }

    ordered_keys = [
        "pag",
        "busval",
        "filval",
        "panu",
        "tebu",
        "ord",
        "valo",
        "ubic",
        "toen",
        "veid",
        "qregx",
        "tmin",
        "ttseu",
        "txbu",
        "ivevh",
        "ivevhmat",
        "ivevhsel",
        "ivevhcsver",
        "ivevhse",
        "oem",
        "vin",
    ]
    query = "&".join(f"{k}={params[k]}" for k in ordered_keys)
    return f"https://ecooparts.com/recambios-automovil-segunda-mano/?{query}"


# ----------------------------
# Price extraction utilities
# ----------------------------
def extract_price_from_text(text: str) -> Optional[float]:
    """
    Esperado en ficha: "40,00€ (sin IVA)" -> 40.00
    Captura decimales.
    """
    if not text:
        return None

    text = text.strip()

    patterns = [
        r"(\d{1,3}(?:\.\d{3})+,\d{2})",  # 1.234,56
        r"(\d{1,3}(?:,\d{3})+\.\d{2})",  # 1,234.56
        r"(\d+,\d{2})",  # 47,74
        r"(\d+\.\d{2})",  # 47.74
        r"(\d+,\d{1})",  # 47,7
        r"(\d+\.\d{1})",  # 47.7
    ]

    for pattern in patterns:
        m = re.search(pattern, text)
        if not m:
            continue

        price_str = m.group(1)

        if "," in price_str and "." in price_str:
            last_comma = price_str.rfind(",")
            last_dot = price_str.rfind(".")
            if last_comma > last_dot:
                price_str = price_str.replace(".", "").replace(",", ".")
            else:
                price_str = price_str.replace(",", "")
        elif "," in price_str:
            price_str = price_str.replace(",", ".")

        try:
            price = float(price_str)
            if 0.01 <= price <= 50000:
                return price
        except ValueError:
            continue

    return None


@dataclass
class SearchResult:
    count: int
    prices: List[float]

    @property
    def min_price(self) -> Optional[float]:
        return min(self.prices) if self.prices else None

    @property
    def max_price(self) -> Optional[float]:
        return max(self.prices) if self.prices else None


def _run_coro_safely(coro):
    """Run an awaitable safely even if an event loop is already running.

    If there's a running loop, execute the coroutine in a new thread with
    its own event loop to avoid ``asyncio.run()`` errors.

    DEPRECATED: Currently unused in the main flow. Kept for potential future
    async detail fetches; prefer explicit async orchestration when needed.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        result = {}

        def _thread_target():
            new_loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(new_loop)
                res = new_loop.run_until_complete(coro)
                result["res"] = res
            except Exception as e:
                result["exc"] = e
            finally:
                try:
                    new_loop.close()
                except Exception:
                    pass

        th = threading.Thread(target=_thread_target)
        th.daemon = True
        th.start()
        th.join()

        if "exc" in result:
            raise result["exc"]
        return result.get("res")

    return asyncio.run(coro)


# ----------------------------
# Playwright config + selectors
# ----------------------------
_PRODUCT_LINK_SELECTORS = (
    'a[href*="recambio-automovil-segunda-mano/"],'
    'a[href*="/en/used-auto-part/"],'
    'a[href*="/used-auto-part/"],'
    'a[href*="/pt/peca-auto-usada/"],'
    'a[href*="/peca-auto-usada/"]'
)

_PRICE_SELECTORS = [
    ".product__price--siniva",
    ".product__price--siniva *",
    ".product__price",
    ".product__price *",
    ".product__price--iva",
    ".product__price--iva *",
]


@dataclass
class CounterConfig:
    headless: bool = True
    timeout_ms: int = 10000  # OPTIMIZADO: era 30000
    proxy: Optional[str] = None
    max_pages: int = 5  # OPTIMIZADO: era 20
    per_page: int = 30
    scroll_rounds: int = 3  # OPTIMIZADO: era 10
    scroll_wait_ms: int = 500  # OPTIMIZADO: era 800
    block_resources: bool = True

    # NUEVO: ficha exacta
    detail_delay_s: float = 0.25
    detail_retries: int = 2

    # Número de pestañas concurrentes a usar en el fetch asíncrono (1 = secuencial)
    detail_workers: int = 2

    # NUEVO: útil para TEST (0 = sin límite, exacto sobre todos los links recolectados)
    max_details_per_query: int = 0
    # Preferir precio publicado con descuento en listado (True = usar precio con descuento cuando exista)
    prefer_discounted: bool = True


class EcoopartsCounter:
    """
    Exactitud: precios SIN IVA se extraen desde la FICHA (.product__price--siniva).
    El listado se usa solo para recolectar links.
    """

    def __init__(self, cfg: CounterConfig):
        self.cfg = cfg
        self._pw = None
        self._browser = None
        self._context = None
        self._page = None
        self._detail_page = None
        self.cache: Dict[str, SearchResult] = {}
        self._route_handler = None
        self._blocking_enabled = False

    def start(self):
        if sync_playwright is None:
            raise RuntimeError(
                "Playwright no está instalado. Ejecuta: "
                "pip install playwright && python -m playwright install chromium"
            )

        if self._pw:
            return

        self._pw = sync_playwright().start()
        launch_args = {"headless": self.cfg.headless}
        if self.cfg.proxy:
            launch_args["proxy"] = {"server": self.cfg.proxy}

        self._browser = self._pw.chromium.launch(**launch_args)
        self._context = self._browser.new_context(
            locale="es-ES",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 768},
        )

        # Request-blocking: abortar recursos que no afectan extracción
        if self.cfg.block_resources:
            try:
                blocked_resource_types = {"image", "stylesheet", "font", "media"}
                blocked_domains = [
                    "googlesyndication.com",
                    "doubleclick.net",
                    "google-analytics.com",
                    "analytics",
                    "ads",
                    "adservice",
                    "facebook.net",
                    "facebook.com",
                    "twitter.com",
                    "scorecardresearch.com",
                ]

                def _route_handler(route, request):
                    try:
                        rtype = request.resource_type
                        if rtype in blocked_resource_types:
                            return route.abort()

                        url = (request.url or "").lower()
                        for d in blocked_domains:
                            if d in url:
                                return route.abort()

                        return route.continue_()
                    except Exception:
                        try:
                            return route.continue_()
                        except Exception:
                            return None

                self._context.route("**/*", _route_handler)
                self._route_handler = _route_handler
                self._blocking_enabled = True
            except Exception:
                pass

        self._page = self._context.new_page()
        self._page.set_default_timeout(self.cfg.timeout_ms)

        # Segunda pestaña para fichas
        self._detail_page = self._context.new_page()
        self._detail_page.set_default_timeout(self.cfg.timeout_ms)

    def close(self):
        try:
            if self._detail_page:
                self._detail_page.close()
        finally:
            self._detail_page = None

        try:
            if self._context:
                self._context.close()
        finally:
            self._context = None

        try:
            if self._browser:
                self._browser.close()
        finally:
            self._browser = None

        try:
            if self._pw:
                self._pw.stop()
        finally:
            self._pw = None

        self._page = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def _ensure_page(self):
        if self._page is None or self._detail_page is None:
            self.start()

    def _disable_blocking(self):
        if not self._context or not self._route_handler:
            return
        if not self._blocking_enabled:
            return
        try:
            self._context.unroute("**/*", self._route_handler)
            self._blocking_enabled = False
        except Exception:
            pass

    async def _fetch_details_async(self, links: List[str]) -> List[Optional[float]]:
        """Usa async_playwright para abrir un solo navegador y extraer precios en múltiples pestañas concurrentes.

        DEPRECATED in current architecture: prices are taken from listing;
        detail fetch retained for future exactness checks or diagnostics.
        """
        results: List[Optional[float]] = []
        try:
            from playwright.async_api import async_playwright
        except Exception:
            return results

        try:
            async with async_playwright() as apw:
                launch_args = {"headless": self.cfg.headless}
                if self.cfg.proxy:
                    launch_args["proxy"] = {"server": self.cfg.proxy}

                browser = await apw.chromium.launch(**launch_args)
                context = await browser.new_context(
                    locale="es-ES",
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0 Safari/537.36"
                    ),
                    viewport={"width": 1366, "height": 768},
                )

                sem = asyncio.Semaphore(self.cfg.detail_workers or 4)

                async def _fetch(url: str) -> Optional[float]:
                    async with sem:
                        page = await context.new_page()
                        try:
                            await page.goto(url, wait_until="domcontentloaded", timeout=self.cfg.timeout_ms)

                            # aceptar cookies si aparece
                            try:
                                for sel in [
                                    'button:has-text("Aceptar")',
                                    'button:has-text("ACEPTAR")',
                                    'button:has-text("Acepto")',
                                ]:
                                    try:
                                        el = await page.query_selector(sel)
                                        if el:
                                            await el.click()
                                    except Exception:
                                        pass
                            except Exception:
                                pass

                            # intentar selectores
                            for sel in _PRICE_SELECTORS:
                                try:
                                    el = await page.query_selector(sel)
                                    if el:
                                        cand = (await el.text_content() or "").strip()
                                        if cand:
                                            price = extract_price_from_text(cand)
                                            if price and price > 0:
                                                return price
                                except Exception:
                                    continue

                            # fallback: texto body
                            try:
                                body = await page.query_selector("body")
                                if body:
                                    body_text = await body.inner_text() or ""
                                    price = extract_price_from_text(body_text)
                                    if price and price > 0:
                                        return price
                            except Exception:
                                pass

                            return None
                        except Exception:
                            return None
                        finally:
                            try:
                                await page.close()
                            except Exception:
                                pass

                tasks = [_fetch(u) for u in links]
                gathered = await asyncio.gather(*tasks)
                results = list(gathered)

                try:
                    await context.close()
                except Exception:
                    pass
                try:
                    await browser.close()
                except Exception:
                    pass

        except Exception:
            return results

        return results

    def get_search_page_html(self, query_text: str, *, page: int = 1) -> str:
        """Navega la URL de búsqueda para `query_text` y devuelve el HTML de la página (útil para debug)."""
        self._ensure_page()
        assert self._page is not None
        url = build_ecooparts_search_url(query_text, page=page, per_page=self.cfg.per_page)
        try:
            self._page.goto(url, wait_until="domcontentloaded")
            self._try_accept_cookies(self._page)
            try:
                return self._page.content()
            except Exception:
                return ""
        except Exception:
            try:
                return self._page.content()
            except Exception:
                return ""

    def _try_accept_cookies(self, page=None):
        page = page or self._page
        if not page:
            return
        candidates = [
            'button:has-text("Aceptar")',
            'button:has-text("ACEPTAR")',
            'button:has-text("Acepto")',
            'button:has-text("Entendido")',
            'button:has-text("Accept")',
        ]
        for sel in candidates:
            try:
                loc = page.locator(sel).first
                if loc.is_visible():
                    loc.click(timeout=2000)
                    break
            except Exception:
                continue

    def _collect_links(self) -> Set[str]:
        assert self._page is not None
        loc = self._page.locator(_PRODUCT_LINK_SELECTORS)
        hrefs: Set[str] = set()
        try:
            urls = loc.evaluate_all("els => els.map(e => e.href)")
            for u in urls:
                if isinstance(u, str) and u:
                    hrefs.add(u)
        except Exception:
            pass
        return hrefs

    def _scroll_to_load_more_links(self, *, verbose: bool = False) -> Set[str]:
        assert self._page is not None
        # Wait for initial JS to load products (increased from 800ms for better reliability)
        self._page.wait_for_timeout(1500)
        all_links = self._collect_links()
        stable_rounds = 0

        for i in range(self.cfg.scroll_rounds):
            before = len(all_links)

            self._page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            self._page.wait_for_timeout(self.cfg.scroll_wait_ms)
            self._try_accept_cookies(self._page)

            all_links |= self._collect_links()
            after = len(all_links)

            if verbose:
                print(f"[verbose] scroll {i+1}/{self.cfg.scroll_rounds}: links {before}->{after}")

            if after == before:
                stable_rounds += 1
                if stable_rounds >= 2:
                    break
            else:
                stable_rounds = 0

        return all_links

    def _extract_siniva_from_detail(self, url: str, *, verbose: bool = False) -> Optional[float]:
        assert self._detail_page is not None

        last_err = None
        for attempt in range(1, self.cfg.detail_retries + 2):
            try:
                self._detail_page.goto(url, wait_until="domcontentloaded")
                self._try_accept_cookies(self._detail_page)

                txt = ""
                for sel in _PRICE_SELECTORS:
                    try:
                        self._detail_page.wait_for_selector(sel, timeout=min(self.cfg.timeout_ms, 5000))
                        cand = (self._detail_page.locator(sel).first.text_content() or "").strip()
                        if cand:
                            txt = cand
                            if verbose:
                                print(f"[verbose] ficha price raw='{txt}' selector='{sel}' url={url}")
                            price = extract_price_from_text(txt)
                            if price and price > 0:
                                return price
                    except Exception:
                        continue

                # Fallback: buscar precios en el texto visible de la página
                try:
                    body_text = self._detail_page.locator("body").inner_text() or ""
                    price = extract_price_from_text(body_text)
                    if price and price > 0:
                        if verbose:
                            print(f"[verbose] ficha price fallback body url={url}")
                        return price
                except Exception:
                    pass

                return None

            except Exception as e:
                last_err = e
                if verbose:
                    print(f"[verbose] intento {attempt} falló en ficha: {e} url={url}")
                time.sleep(0.5)

        if verbose and last_err:
            print(f"[verbose] ficha FAIL definitivo: {last_err} url={url}")
        return None

    def _collect_list_prices(self, *, verbose: bool = False) -> List[float]:
        """Extrae precios directamente del listado."""
        assert self._page is not None
        prices: List[float] = []

        # Nueva estrategia: iterar por cada tarjeta/product-card y
        # preferir el precio con descuento (p. ej. `.product-card__price--new` o `.product-card__price--current`)
        # Si no existe precio 'new' usar el `--old` o selector genérico.
        # Conservamos la estrategia anterior como comentario por compatibilidad.
        # Anteriormente se usaban selectores globales y se extraían todos los textos:
        # selectors = [
        #     ".product-card__price--current",
        #     ".product-card__prices .product-card__price",
        #     ".product-card__price",
        # ]
        # for sel in selectors:
        #     texts = self._page.locator(sel).all_inner_texts()
        #     for t in texts: ...

        # Selector para cada card (captura variantes de estructura)
        card_selector = "div.products-list__content > div.products-list__item, div.product-card"

        try:
            cards = self._page.query_selector_all(card_selector)
        except Exception:
            # Fallback a la estrategia antigua si la API difiere
            try:
                selectors = [
                    ".product-card__price--current",
                    ".product-card__prices .product-card__price",
                    ".product-card__price",
                ]
                for sel in selectors:
                    texts = self._page.locator(sel).all_inner_texts()
                    if verbose:
                        print(f"[verbose] listado selector '{sel}' -> {len(texts)} textos (fallback)")
                    for t in texts:
                        p = extract_price_from_text(t)
                        if p and p > 0:
                            prices.append(p)
            except Exception:
                pass
            return prices

        for c in cards:
            try:
                # Prioridad: precio 'new' o 'current' (descuento publicado)
                if self.cfg.prefer_discounted:
                    el = c.query_selector(
                        ".product-card__price--new, .product-card__price--current, .product-card__price--current"
                    )
                else:
                    el = None

                # Si no se encontró precio 'new' o preferencia desactivada, buscar old/generic
                if not el:
                    el = c.query_selector(
                        ".product-card__price--old, .product-card__prices .product-card__price, .product-card__price"
                    )

                if not el:
                    continue

                text = el.inner_text().strip()
                p = extract_price_from_text(text)
                if p and p > 0:
                    prices.append(p)
            except Exception:
                # ignorar errores por tarjeta
                continue

        return prices

    def search(self, query_text: str, *, verbose: bool = False) -> SearchResult:
        """
        1) Navega el listado paginado.
        2) Extrae precios DIRECTAMENTE del listado (sin entrar a fichas).
        """
        q = str(query_text or "").strip()
        if q == "":
            return SearchResult(count=0, prices=[])

        if q in self.cache:
            cached = self.cache[q]
            # No reutilizar caché con 0 resultados: puede quedar obsoleto
            if cached.count > 0 or (cached.prices and len(cached.prices) > 0):
                if verbose:
                    print(f"[verbose] Cache hit para: '{q}'")
                return cached
            if verbose:
                print(f"[verbose] Cache miss (0 resultados) para: '{q}' -> reconsultando")

        self._ensure_page()
        assert self._page is not None

        all_prices: List[float] = []
        all_links: Set[str] = set()

        for page_num in range(1, self.cfg.max_pages + 1):
            url = build_ecooparts_search_url(q, page=page_num, per_page=self.cfg.per_page)
            if verbose:
                print(f"\n[verbose] ===== LISTADO PÁGINA {page_num} =====")
                print(f"[verbose] URL: {url}")

            try:
                self._page.goto(url, wait_until="domcontentloaded")
                self._try_accept_cookies(self._page)

                # scroll para cargar más cards y recoger links visibles
                page_links = self._scroll_to_load_more_links(verbose=verbose)
                if page_links:
                    all_links |= page_links

                page_prices = self._collect_list_prices(verbose=verbose)

                if verbose:
                    print(f"[verbose] Página {page_num}: precios encontrados={len(page_prices)}")

                if not page_prices:
                    if verbose:
                        print(f"[verbose] Página {page_num}: 0 precios. Stop.")
                    break

                all_prices.extend(page_prices)

                if len(all_prices) >= 50:
                    if verbose:
                        print(
                            f"[verbose] Página {page_num}: Ya tenemos {len(all_prices)} precios. "
                            "Suficiente para exactitud."
                        )
                    break

                if len(page_prices) < self.cfg.per_page:
                    if verbose:
                        print(f"[verbose] Página {page_num}: < per_page ({self.cfg.per_page}). Fin.")
                    break

            except Exception as ex:
                if verbose:
                    print(f"[verbose] Error listado página {page_num}: {ex}")
                break

        result_count = len(all_links) if all_links else len(all_prices)

        prices_for_minmax: List[float]
        if all_prices and all_links and len(all_links) < len(all_prices):
            seen = set()
            unique_prices: List[float] = []
            for p in all_prices:
                if p in seen:
                    continue
                seen.add(p)
                unique_prices.append(p)
            prices_for_minmax = unique_prices
        else:
            prices_for_minmax = all_prices

        result = SearchResult(count=result_count, prices=prices_for_minmax)
        self.cache[q] = result

        if verbose:
            print("\n[verbose] ===== RESUMEN QUERY =====")
            print(f"[verbose] precios en listado: {len(result.prices)}")
            if result.prices:
                print(f"[verbose] min: €{result.min_price:.2f}")
                print(f"[verbose] max: €{result.max_price:.2f}")
            print("[verbose] ==========================\n")

        return result


# ----------------------------
# Main processing
# ----------------------------
def _format_price(price: Optional[float]) -> str:
    if price is None:
        return "0.00"
    return f"{price:.2f}"


def process(
    df: pd.DataFrame,
    *,
    batch: int = DEFAULT_BATCH,
    start: int = 0,
    delay: float = 0.2,  # OPTIMIZADO: era 0.5
    counter_cfg: Optional[CounterConfig] = None,
    verbose: bool = False,
    initial_cache: Optional[Dict[str, SearchResult]] = None,
    debug_folder: Optional[str] = None,
) -> Tuple[pd.DataFrame, Dict[str, SearchResult]]:
    oem_search = get_col(df, ["OEM"], 1)

    n = len(df)
    end = min(n, start + batch)

    out_count = pd.Series([0] * n)
    out_min = pd.Series(["0.00"] * n)
    out_max = pd.Series(["0.00"] * n)

    if counter_cfg is None:
        counter_cfg = CounterConfig()

    with EcoopartsCounter(counter_cfg) as counter:
        if initial_cache:
            counter.cache.update(initial_cache)

        for i in range(start, end):
            search_text = str(oem_search.iloc[i]).strip()

            if not search_text:
                out_count.iloc[i] = 0
                print(f"Fila {i+1}/{n} -> SKIP (vacío)")
                continue

            try:
                search_query = re.sub(r"\bmandos\b", "mando", search_text, flags=re.I)
            except Exception:
                search_query = search_text

            if verbose and search_query != search_text:
                print(f"[verbose] Ajustando query: '{search_text}' -> '{search_query}'")

            result = counter.search(search_query, verbose=verbose)

            if result.count == 0:
                tokens = re.findall(r"\b[A-Za-z0-9]{5,}\b", search_text)
                if tokens:
                    # Filtrar tokens: preferir alfanuméricos mixtos (con letras Y números)
                    # para evitar fallback a palabras genéricas como "CATALIZADOR", "ANTENA", etc.
                    # que generan falsos positivos.
                    mixed_tokens = [t for t in tokens if re.search(r"\d", t) and re.search(r"[A-Za-z]", t)]
                    
                    # Si hay tokens mixtos, usar el más largo de esos; sino usar el más largo general
                    candidates = mixed_tokens if mixed_tokens else tokens
                    best_token = max(candidates, key=len)
                    
                    if verbose:
                        if mixed_tokens:
                            print(
                                f"[verbose] Fila {i+1}: Sin resultados. "
                                f"Intentando variante con código alfanumérico: '{best_token}'"
                            )
                        else:
                            print(
                                f"[verbose] Fila {i+1}: Sin resultados. "
                                f"Intentando variante con token: '{best_token}' (sin códigos alfanuméricos mixtos)"
                            )

                    vres = counter.search(best_token, verbose=verbose)
                    if vres.count > 0:
                        result = vres
                        if verbose:
                            print(f"[verbose] Variante exitosa con token '{best_token}' -> links={vres.count}")

                if result.count == 0 and debug_folder:
                    try:
                        html = counter.get_search_page_html(search_query)
                        safe_q = re.sub(r"[^A-Za-z0-9]+", "_", search_text)[:80]
                        fname = f"debug_row{i+1}_{safe_q}.html"
                        fpath = os.path.join(debug_folder, fname)
                        with open(fpath, "w", encoding="utf-8") as fh:
                            fh.write(html)
                        print(f"[!] Fila {i+1} sin resultados. HTML debug: {fpath}")
                    except Exception as e:
                        if verbose:
                            print(f"[verbose] Error guardando HTML debug: {e}")

            out_count.iloc[i] = result.count
            out_min.iloc[i] = _format_price(result.min_price)
            out_max.iloc[i] = _format_price(result.max_price)

            print(
                f"Fila {i+1}/{n} -> "
                f"cantidad_links={out_count.iloc[i]} | "
                f"min_sinIVA={out_min.iloc[i]} | "
                f"max_sinIVA={out_max.iloc[i]} | "
                f"OEM='{search_text}'"
            )

            time.sleep(delay)

    df_out = df.copy()

    if "ID" in df_out.columns:
        id_col = df_out["ID"]
    else:
        id_col = pd.Series(range(1, len(df_out) + 1), index=df_out.index)

    df_out["OEM"] = oem_search

    def _merge_price_column(existing: pd.Series, computed: pd.Series) -> pd.Series:
        out = computed.copy()
        for idx, val in existing.astype(str).fillna("").items():
            v = val.strip()
            if not v:
                continue
            price = extract_price_from_text(v)
            if price is None:
                if re.fullmatch(r"\d+", v):
                    try:
                        price = float(v)
                    except Exception:
                        price = None
            if price is not None and price > 0:
                out.iloc[idx] = v
        return out

    def _merge_units_column(existing: pd.Series, computed: pd.Series) -> pd.Series:
        out = computed.copy()
        for idx, val in existing.items():
            try:
                num = pd.to_numeric(val, errors="coerce")
            except Exception:
                num = None
            if num is not None and pd.notna(num) and float(num) > 0:
                out.iloc[idx] = int(num)
        return out

    if "PRECIO MAXIMO" in df_out.columns:
        max_col = _merge_price_column(df_out["PRECIO MAXIMO"], out_max)
    else:
        max_col = out_max

    if "PRECIO MINIMO" in df_out.columns:
        min_col = _merge_price_column(df_out["PRECIO MINIMO"], out_min)
    else:
        min_col = out_min

    if "CANTIDAD" in df_out.columns:
        units_col = _merge_units_column(df_out["CANTIDAD"], out_count)
    else:
        units_col = out_count.astype(int)

    df_final = pd.DataFrame(
        {
            "ID": id_col,
            "OEM": df_out["OEM"],
            "Units": units_col,
            "Max Price": max_col,
            "Min Price": min_col,
        },
        index=df_out.index,
    )

    return df_final, counter.cache


def _load_cache_file(path: str) -> Dict[str, SearchResult]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except Exception:
        return {}

    out: Dict[str, SearchResult] = {}
    for k, v in raw.items():
        try:
            cnt = int(v.get("count", 0))
            prices = [float(x) for x in v.get("prices", [])]
            out[k] = SearchResult(count=cnt, prices=prices)
        except Exception:
            continue
    return out


def _save_cache_file(path: str, cache: Dict[str, SearchResult]):
    serial: Dict[str, Dict] = {}
    for k, v in cache.items():
        serial[k] = {"count": int(v.count), "prices": v.prices}
    try:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(serial, fh, indent=2, ensure_ascii=False)
    except Exception:
        pass


def main():
    parser = argparse.ArgumentParser(description="Scraping de precios SIN IVA (confirmados en ficha) desde Ecooparts")
    parser.add_argument("--scrap-folder", default=None, help="Carpeta base Scrap")
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH, help="Cantidad de filas a procesar")
    parser.add_argument("--start", type=int, default=0, help="Fila inicial (0-indexed)")
    parser.add_argument("--delay", type=float, default=0.2, help="Delay entre filas en segundos")  # OPTIMIZADO
    parser.add_argument("--timeout", type=int, default=10000, help="Timeout Playwright en ms")  # OPTIMIZADO
    parser.add_argument("--proxy", type=str, default=None, help="Proxy HTTP(S)")
    parser.add_argument("--max-pages", type=int, default=5, help="Máximo de páginas por búsqueda")  # OPTIMIZADO
    parser.add_argument("--per-page", type=int, default=30, help="Resultados por página")
    parser.add_argument("--verbose", action="store_true", help="Logs detallados de depuración")
    parser.add_argument("--headful", action="store_true", help="Muestra el navegador")
    parser.add_argument("--move-to-done", action="store_true", help="Mover archivo procesado a Done/")
    parser.add_argument("--no-blocking", action="store_true", help="Desactivar bloqueo de recursos")

    # NUEVOS FLAGS para exactitud en ficha
    parser.add_argument("--detail-delay", type=float, default=0.25, help="Delay entre fichas (seg)")
    parser.add_argument("--detail-retries", type=int, default=2, help="Reintentos por ficha")
    parser.add_argument(
        "--max-details-per-query", type=int, default=0, help="Limitar fichas por búsqueda (0 = sin límite)"
    )
    parser.add_argument("--capture-xhr", default=None, help="Comma-separated queries to capture XHR/fetch and exit")
    parser.add_argument(
        "--no-prefer-discounted",
        action="store_false",
        dest="prefer_discounted",
        help="No priorizar precios con descuento en el listado (usar precio 'old' si existe)",
    )
    parser.set_defaults(prefer_discounted=True)

    args = parser.parse_args()

    if args.scrap_folder:
        scrap_folder = args.scrap_folder
    else:
        home = os.path.expanduser("~")
        candidates = [
            os.path.join(home, "Desktop", "Precios_OEM"),
            os.path.join(home, "OneDrive", "Desktop", "Precios_OEM"),
        ]
        one = os.environ.get("OneDrive")
        if one:
            candidates.insert(0, os.path.join(one, "Desktop", "Precios_OEM"))

        scrap_folder = next((c for c in candidates if os.path.isdir(c)), None)
        if scrap_folder is None:
            raise FileNotFoundError(
                "No se encontró la carpeta 'Precios_OEM' en Desktop. "
                "Créala o indica la ruta con --scrap-folder <ruta>"
            )

    print(f"Usando carpeta Scrap: {scrap_folder}")

    folders = setup_scrap_folders(scrap_folder)
    print(f"  Input:  {folders['input']}")
    print(f"  Output: {folders['output']}")
    print(f"  Done:   {folders['done']}")

    # 1) Cargar input (CSV-first para evitar openpyxl en Codex)
    df = load_input_df(scrap_folder)
    print(f"\nInput cargado desde: {os.path.join(scrap_folder, 'Input')}")
    total_rows = len(df)
    print(f"Filas totales: {total_rows}")

    # 2) Para naming del output, elegimos el archivo detectado (si existe)
    #    (solo para fecha en el nombre; la lectura real fue con load_input_df)
    try:
        input_dir = Path(scrap_folder) / "Input"
        detected = pick_input_file(input_dir)
        input_file = str(detected)
        input_filename = detected.name
    except Exception:
        input_file = ""
        input_filename = f"Input_{datetime.now().strftime('%Y%m%d')}.csv"

    print(f"Archivo detectado (para nombre): {input_filename}")

    cfg = CounterConfig(
        headless=not args.headful,
        timeout_ms=args.timeout,
        proxy=args.proxy,
        max_pages=args.max_pages,
        per_page=args.per_page,
        detail_delay_s=args.detail_delay,
        detail_retries=args.detail_retries,
        max_details_per_query=args.max_details_per_query,
        block_resources=not args.no_blocking,
        prefer_discounted=args.prefer_discounted,
    )

    print("\n" + "=" * 70)
    print("INICIANDO EXTRACCIÓN EXACTA (SIN IVA CONFIRMADO EN FICHA)")
    print("=" * 70 + "\n")

    cache_path = os.path.join(folders["output"], "cache_oem.json")
    initial_cache = _load_cache_file(cache_path)

    if args.capture_xhr:
        queries = [q.strip() for q in args.capture_xhr.split(",") if q.strip()]
        with EcoopartsCounter(cfg) as counter:  # noqa: F841
            for q in queries:
                print(f"Capturando XHR para: '{q}'")
                print("  [AVISO] Función capture_xhr_for_query no disponible en versión optimizada")
        return

    df_out, merged_cache = process(
        df,
        batch=args.batch,
        start=args.start,
        delay=args.delay,
        counter_cfg=cfg,
        verbose=args.verbose,
        initial_cache=initial_cache,
        debug_folder=folders["output"],
    )

    output_filename = generate_output_filename(input_filename)
    output_path = os.path.join(folders["output"], output_filename)

    saved_path = output_path

    # Intentar guardar XLSX (requiere openpyxl). Si falla, guardar CSV.
    try:
        df_out.to_excel(output_path, index=False)
    except Exception as e:
        base, _ = os.path.splitext(output_path)
        csv_path = f"{base}.csv"
        df_out.to_csv(csv_path, index=False, encoding="utf-8")
        saved_path = csv_path
        print(f"⚠️ No pude guardar XLSX ({e}). Guardado como CSV: {csv_path}")

    print("\n" + "=" * 70)
    print(f"✓ Resultados guardados en: {saved_path}")
    print("=" * 70)

    # Mover a Done solo si se completó el procesamiento de todas las filas
    processed_end = min(total_rows, args.start + args.batch)
    processed_all = processed_end >= total_rows
    if args.move_to_done and input_file:
        if processed_all:
            done_path = os.path.join(folders["done"], input_filename)
            try:
                shutil.move(input_file, done_path)
                print(f"✓ Archivo de entrada movido a: {done_path}")
            except Exception as e:
                print(f"⚠️ No pude mover a Done/: {e}")
        else:
            print("↷ Archivo NO movido a Done (procesamiento parcial).")

    try:
        _save_cache_file(cache_path, merged_cache)
        print(f"✓ Caché guardada en: {cache_path}")
    except Exception:
        pass


if __name__ == "__main__":
    main()
