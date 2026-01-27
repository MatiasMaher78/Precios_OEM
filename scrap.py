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


DEFAULT_BATCH = 500


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
# Excel I/O
# ----------------------------
def read_workbook(path: str) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=0, dtype=str).fillna("")


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

    # no running loop -> safe to use asyncio.run
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

# Selector exacto visto en tu ficha (deprecated: use `_PRICE_SELECTORS`):
_SINIVA_SELECTOR = ".product__price--siniva"  # DEPRECATED
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
    timeout_ms: int = 30000
    proxy: Optional[str] = None
    max_pages: int = 20
    per_page: int = 30
    scroll_rounds: int = 10
    scroll_wait_ms: int = 800
    block_resources: bool = True

    # NUEVO: ficha exacta
    detail_delay_s: float = 0.25
    detail_retries: int = 2

    # Número de pestañas concurrentes a usar en el fetch asíncrono (1 = secuencial)
    detail_workers: int = 2

    # NUEVO: útil para TEST (0 = sin límite, exacto sobre todos los links recolectados)
    max_details_per_query: int = 0


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
                # En caso de que la versión de Playwright no soporte route en context
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
                            try:
                                await page.wait_for_load_state("networkidle", timeout=min(self.cfg.timeout_ms, 20000))
                            except Exception:
                                pass
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
            try:
                self._page.wait_for_load_state("networkidle", timeout=min(self.cfg.timeout_ms, 20000))
            except Exception:
                pass
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

    def _interactive_search_links(self, query_text: str, *, verbose: bool = False) -> Set[str]:
        """Realiza la búsqueda simulando interacción (llenar input + click) y recolecta links."""
        self._ensure_page()
        assert self._page is not None

        # Ir a la home para asegurar que los elementos de búsqueda existen
        try:
            self._page.goto("https://ecooparts.com/", wait_until="domcontentloaded")
            try:
                self._page.wait_for_load_state("networkidle", timeout=min(self.cfg.timeout_ms, 8000))
            except Exception:
                pass
        except Exception:
            pass

        # Intentar ubicar y llenar el input de búsqueda
        input_selectors = ["input.search__input", "#ctlbuscprinheabone", "#buscar_mob"]
        filled = False
        for sel in input_selectors:
            try:
                loc = self._page.locator(sel).first
                if loc and loc.is_visible():
                    try:
                        loc.fill(str(query_text))
                    except Exception:
                        # fallback: use evaluate to set value
                        try:
                            self._page.evaluate(
                                "(s,v)=>{const el=document.querySelector(s); if(el) el.value=v}", sel, str(query_text)
                            )
                        except Exception:
                            pass
                    filled = True
                    break
            except Exception:
                continue

        # Intentar clicar el botón de búsqueda
        btn_selectors = [
            "button.search__button--end",
            "button.mobile-search__button",
            "button.search__button.search__button--end",
        ]
        clicked = False
        for b in btn_selectors:
            try:
                bl = self._page.locator(b).first
                if bl and bl.is_visible():
                    try:
                        bl.click()
                        clicked = True
                        break
                    except Exception:
                        try:
                            self._page.evaluate("s=>document.querySelector(s).click()", b)
                            clicked = True
                            break
                        except Exception:
                            continue
            except Exception:
                continue

        # Si no se pudo llenar/clickear, intentar ejecutar la función JS que dispara la búsqueda
        if not (filled and clicked):
            try:
                self._page.evaluate("() => { if(typeof timeBuscPag === 'function') timeBuscPag(); }")
            except Exception:
                pass

        # Esperar a que aparezcan los enlaces y recolectar
        try:
            self._page.wait_for_selector(_PRODUCT_LINK_SELECTORS, timeout=min(self.cfg.timeout_ms, 15000))
        except Exception:
            # dejar que _scroll_to_load_more_links haga varios scrolls y reintentos
            pass

        try:
            links = self._scroll_to_load_more_links(verbose=verbose)
        except Exception:
            links = set()

        return links

    def capture_xhr_for_query(self, query_text: str, debug_folder: str, *, verbose: bool = False) -> List[Dict]:
        """Captura requests XHR/fetch durante una búsqueda interactiva y guarda resumen/response.

        Devuelve una lista de diccionarios con datos de cada request/response capturado.
        """
        self._ensure_page()
        assert self._page is not None

        captured: List[Dict] = []

        def _on_request(request):
            try:
                if request.resource_type in ("xhr", "fetch"):
                    captured.append(
                        {
                            "id": len(captured),
                            "url": request.url,
                            "method": request.method,
                            "post_data": request.post_data or None,
                            "headers": dict(request.headers or {}),
                            "ts": time.time(),
                            "response": None,
                        }
                    )
            except Exception:
                pass

        def _on_response(response):
            try:
                req = response.request
                if req.resource_type in ("xhr", "fetch"):
                    # find matching captured entry by URL+method
                    for c in captured:
                        if c.get("url") == req.url and c.get("method") == req.method and c.get("response") is None:
                            try:
                                status = response.status
                                c["response"] = {"status": status}
                                # intentar leer texto (puede ser grande)
                                try:
                                    txt = response.text()
                                except Exception:
                                    txt = ""
                                # guardar body a archivo para inspección
                                safe_q = re.sub(r"[^A-Za-z0-9]+", "_", query_text)[:80]
                                fname = f"xhr_{len(captured)}_{safe_q}.response.txt"
                                fpath = os.path.join(debug_folder, fname)
                                try:
                                    with open(fpath, "w", encoding="utf-8") as fh:
                                        fh.write(txt)
                                    c["response"]["body_file"] = fpath
                                except Exception:
                                    c["response"]["body_file"] = None
                            except Exception:
                                pass
                            break
            except Exception:
                pass

        # registrar handlers
        self._page.on("request", _on_request)
        self._page.on("response", _on_response)

        # Ejecutar búsqueda interactiva (simula llenado y click)
        try:
            _ = self._interactive_search_links(query_text, verbose=verbose)
            # esperar un poco para que las respuestas lleguen
            time.sleep(1.0)
        except Exception:
            pass

        # quitar handlers
        try:
            self._page.off("request", _on_request)
            self._page.off("response", _on_response)
        except Exception:
            pass

        # Guardar resumen en debug_folder
        try:
            safe_q = re.sub(r"[^A-Za-z0-9]+", "_", query_text)[:80]
            out_name = f"debug_xhr_{safe_q}.json"
            out_path = os.path.join(debug_folder, out_name)
            with open(out_path, "w", encoding="utf-8") as fh:
                json.dump(captured, fh, indent=2, ensure_ascii=False)
            if verbose:
                print(f"[verbose] XHR summary saved: {out_path}")
        except Exception:
            pass

        return captured

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
                try:
                    self._detail_page.wait_for_load_state("networkidle", timeout=min(self.cfg.timeout_ms, 20000))
                except Exception:
                    pass

                self._try_accept_cookies(self._detail_page)

                txt = ""
                for sel in _PRICE_SELECTORS:
                    try:
                        self._detail_page.wait_for_selector(sel, timeout=min(self.cfg.timeout_ms, 8000))
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
        selectors = [
            ".product-card__price--current",
            ".product-card__prices .product-card__price",
            ".product-card__price",
        ]
        prices: List[float] = []

        for sel in selectors:
            try:
                texts = self._page.locator(sel).all_inner_texts()
                if verbose:
                    print(f"[verbose] listado selector '{sel}' -> {len(texts)} textos")
                for t in texts:
                    p = extract_price_from_text(t)
                    if p and p > 0:
                        prices.append(p)
            except Exception:
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
                try:
                    self._page.wait_for_load_state("networkidle", timeout=min(self.cfg.timeout_ms, 20000))
                except Exception:
                    pass

                self._try_accept_cookies(self._page)

                # scroll para cargar más cards y recoger links visibles
                page_links = self._scroll_to_load_more_links(verbose=verbose)
                if page_links:
                    all_links |= page_links

                page_prices = self._collect_list_prices(verbose=verbose)

                if verbose:
                    print(f"[verbose] Página {page_num}: precios encontrados={len(page_prices)}")

                if not page_prices:
                    # fallback: búsqueda interactiva
                    try:
                        if verbose:
                            print(f"[verbose] Página {page_num}: sin precios. Intentando búsqueda interactiva...")
                        self._disable_blocking()
                        inter_links = self._interactive_search_links(q, verbose=verbose)
                        if inter_links:
                            all_links |= inter_links
                        page_prices = self._collect_list_prices(verbose=verbose)
                        if verbose:
                            print(f"[verbose] Interactiva precios={len(page_prices)}")
                    except Exception:
                        pass

                if not page_prices:
                    # sin precios -> cortar
                    if verbose:
                        print(f"[verbose] Página {page_num}: 0 precios. Stop.")
                    break

                all_prices.extend(page_prices)

                # Heurística de fin (si trae menos que per_page)
                if len(page_prices) < self.cfg.per_page:
                    if verbose:
                        print(f"[verbose] Página {page_num}: < per_page ({self.cfg.per_page}). Fin.")
                    break

            except Exception as ex:
                if verbose:
                    print(f"[verbose] Error listado página {page_num}: {ex}")
                break

        # Preferir contar unidades a partir de los links HTML si están disponibles
        result_count = len(all_links) if all_links else len(all_prices)

        # Si detectamos que hay más precios que links (cards repetidas), calcular min/max
        # sobre precios únicos para evitar sesgo por duplicados.
        prices_for_minmax: List[float]
        if all_prices and all_links and len(all_links) < len(all_prices):
            # mantener orden pero quitar repeticiones exactas de precio
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
    return f"€{price:.2f}"


def process(
    df: pd.DataFrame,
    *,
    batch: int = DEFAULT_BATCH,
    start: int = 0,
    delay: float = 0.5,
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
        # Cargar cache inicial (persistente) si fue provista
        if initial_cache:
            counter.cache.update(initial_cache)
        for i in range(start, end):
            search_text = str(oem_search.iloc[i]).strip()

            if not search_text:
                out_count.iloc[i] = 0
                print(f"Fila {i+1}/{n} -> SKIP (vacío)")
                continue

            # Ajustes específicos para mejorar coincidencias en Ecooparts
            try:
                search_query = re.sub(r"\bmandos\b", "mando", search_text, flags=re.I)
            except Exception:
                search_query = search_text

            if verbose and search_query != search_text:
                print(f"[verbose] Ajustando query: '{search_text}' -> '{search_query}'")

            result = counter.search(search_query, verbose=verbose)

            # Si no hay resultados, probar una única variante: el token alfanumérico más largo (código OEM)
            if result.count == 0:
                # extraer tokens alfanuméricos largos (ej. 9640795280)
                tokens = re.findall(r"\b[A-Za-z0-9]{5,}\b", search_text)
                if tokens:
                    # priorizar el token más largo, que suele ser el código de pieza
                    best_token = max(tokens, key=len)
                    if verbose:
                        print(f"[verbose] Fila {i+1}: Sin resultados. Intentando variante con código: '{best_token}'")

                    vres = counter.search(best_token, verbose=verbose)
                    if vres.count > 0:
                        result = vres
                        if verbose:
                            print(f"[verbose] Variante exitosa con token '{best_token}' -> links={vres.count}")

                # Si todavía 0, guardar HTML de la página para debug
                if result.count == 0 and debug_folder:
                    try:
                        html = counter.get_search_page_html(search_query)
                        safe_q = re.sub(r"[^A-Za-z0-9]+", "_", search_text)[:80]
                        fname = f"debug_row{i+1}_{safe_q}.html"
                        fpath = os.path.join(debug_folder, fname)
                        with open(fpath, "w", encoding="utf-8") as fh:
                            fh.write(html)
                        if verbose:
                            print(f"[verbose] HTML debug guardado en: {fpath}")
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

    # Preparar columna ID (si no existe, crear secuencial)
    if "ID" in df_out.columns:
        id_col = df_out["ID"]
    else:
        id_col = pd.Series(range(1, len(df_out) + 1), index=df_out.index)

    # Asegurar columna OEM consistente (usar la columna detectada inicialmente)
    df_out["OEM"] = oem_search

    # Priorizar valores existentes por fila si son válidos (>0); si no, usar calculados.
    def _merge_price_column(existing: pd.Series, computed: pd.Series) -> pd.Series:
        out = computed.copy()
        for idx, val in existing.astype(str).fillna("").items():
            v = val.strip()
            if not v:
                continue
            price = extract_price_from_text(v)
            if price is None:
                # permitir enteros simples sin decimales
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

    # Construir DataFrame final con el orden requerido: ID, OEM, Units, Max Price, Min Price
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
    parser.add_argument("--delay", type=float, default=0.5, help="Delay entre filas en segundos")
    parser.add_argument("--timeout", type=int, default=30000, help="Timeout Playwright en ms")
    parser.add_argument("--proxy", type=str, default=None, help="Proxy HTTP(S)")
    parser.add_argument("--max-pages", type=int, default=90, help="Máximo de páginas por búsqueda")
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

    input_file = locate_input_excel(folders["input"])
    print(f"\nArchivo de entrada: {os.path.basename(input_file)}")

    df = read_workbook(input_file)
    print(f"Filas totales: {len(df)}")

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
    )

    print("\n" + "=" * 70)
    print("INICIANDO EXTRACCIÓN EXACTA (SIN IVA CONFIRMADO EN FICHA)")
    print("=" * 70 + "\n")

    # Cache persistente para evitar reconsultas
    cache_path = os.path.join(folders["output"], "cache_oem.json")
    initial_cache = _load_cache_file(cache_path)

    # Si se solicita captura XHR, hacerlo para cada query y salir
    if args.capture_xhr:
        queries = [q.strip() for q in args.capture_xhr.split(",") if q.strip()]
        with EcoopartsCounter(cfg) as counter:
            for q in queries:
                print(f"Capturando XHR para: '{q}'")
                captures = counter.capture_xhr_for_query(q, folders["output"], verbose=args.verbose)
                print(f"  Capturas: {len(captures)} (ver {folders['output']})")
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

    input_filename = os.path.basename(input_file)
    output_filename = generate_output_filename(input_filename)
    output_path = os.path.join(folders["output"], output_filename)

    try:
        df_out.to_excel(output_path, index=False)
        saved_path = output_path
    except PermissionError:
        base, ext = os.path.splitext(output_path)
        alt_path = f"{base}_alt_{int(time.time())}{ext}"
        try:
            df_out.to_excel(alt_path, index=False)
            saved_path = alt_path
            print(f"✓ Archivo original bloqueado. Guardado como: {alt_path}")
        except Exception:
            raise

    print("\n" + "=" * 70)
    print(f"✓ Resultados guardados en: {saved_path}")
    print("=" * 70)

    if args.move_to_done:
        done_path = os.path.join(folders["done"], input_filename)
        shutil.move(input_file, done_path)
        print(f"✓ Archivo de entrada movido a: {done_path}")

    # Guardar cache persistente
    try:
        _save_cache_file(cache_path, merged_cache)
        print(f"✓ Caché guardada en: {cache_path}")
    except Exception:
        pass


if __name__ == "__main__":
    main()
