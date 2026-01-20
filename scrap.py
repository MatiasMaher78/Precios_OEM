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
    folders = {
        'input': base / 'Input',
        'output': base / 'Output',
        'done': base / 'Done'
    }
    for _, path in folders.items():
        path.mkdir(parents=True, exist_ok=True)
    return {k: str(v) for k, v in folders.items()}


def locate_input_excel(input_folder: str) -> str:
    input_path = Path(input_folder)
    candidates = [f for f in input_path.glob("*.xlsx") if not f.name.startswith("~$") and not f.name.startswith(".")]
    candidates.extend([f for f in input_path.glob("*.xls") if not f.name.startswith("~$") and not f.name.startswith(".")])

    if not candidates:
        raise FileNotFoundError(f"No se encontró ningún archivo Excel en {input_folder}")

    if len(candidates) > 1:
        raise FileExistsError(
            f"Se encontraron varios archivos Excel en {input_folder}: {[f.name for f in candidates]}. "
            "Debe haber exactamente 1 archivo."
        )

    return str(candidates[0])


def extract_date_from_filename(filename: str) -> Optional[str]:
    match = re.search(r'(\d{8})', filename)
    if match:
        return match.group(1)

    match = re.search(r'(\d{4})[-_](\d{2})[-_](\d{2})', filename)
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
        "pag", "busval", "filval", "panu", "tebu", "ord", "valo", "ubic", "toen", "veid",
        "qregx", "tmin", "ttseu", "txbu", "ivevh", "ivevhmat", "ivevhsel", "ivevhcsver",
        "ivevhse", "oem", "vin"
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
        r'(\d{1,3}(?:\.\d{3})+,\d{2})',   # 1.234,56
        r'(\d{1,3}(?:,\d{3})+\.\d{2})',   # 1,234.56
        r'(\d+,\d{2})',                   # 47,74
        r'(\d+\.\d{2})',                  # 47.74
        r'(\d+,\d{1})',                   # 47,7
        r'(\d+\.\d{1})',                  # 47.7
    ]

    for pattern in patterns:
        m = re.search(pattern, text)
        if not m:
            continue

        price_str = m.group(1)

        if ',' in price_str and '.' in price_str:
            last_comma = price_str.rfind(',')
            last_dot = price_str.rfind('.')
            if last_comma > last_dot:
                price_str = price_str.replace('.', '').replace(',', '.')
            else:
                price_str = price_str.replace(',', '')
        elif ',' in price_str:
            price_str = price_str.replace(',', '.')

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

# Selector exacto visto en tu ficha:
_SINIVA_SELECTOR = ".product__price--siniva"


@dataclass
class CounterConfig:
    headless: bool = True
    timeout_ms: int = 30000
    proxy: Optional[str] = None
    max_pages: int = 20
    per_page: int = 30
    scroll_rounds: int = 10
    scroll_wait_ms: int = 800

    # NUEVO: ficha exacta
    detail_delay_s: float = 0.25
    detail_retries: int = 2

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
        try:
            blocked_resource_types = {"image", "stylesheet", "font", "media"}
            blocked_domains = [
                "googlesyndication.com", "doubleclick.net", "google-analytics.com",
                "analytics", "ads", "adservice", "facebook.net", "facebook.com",
                "twitter.com", "scorecardresearch.com"
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
                            self._page.evaluate("(s,v)=>{const el=document.querySelector(s); if(el) el.value=v}", sel, str(query_text))
                        except Exception:
                            pass
                    filled = True
                    break
            except Exception:
                continue

        # Intentar clicar el botón de búsqueda
        btn_selectors = ["button.search__button--end", "button.mobile-search__button", "button.search__button.search__button--end"]
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
                    captured.append({
                        "id": len(captured),
                        "url": request.url,
                        "method": request.method,
                        "post_data": request.post_data or None,
                        "headers": dict(request.headers or {}),
                        "ts": time.time(),
                        "response": None,
                    })
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

                self._detail_page.wait_for_selector(_SINIVA_SELECTOR, timeout=min(self.cfg.timeout_ms, 15000))
                txt = (self._detail_page.locator(_SINIVA_SELECTOR).first.text_content() or "").strip()

                if verbose:
                    print(f"[verbose] ficha sinIVA raw='{txt}' url={url}")

                price = extract_price_from_text(txt)
                if price and price > 0:
                    return price
                return None

            except Exception as e:
                last_err = e
                if verbose:
                    print(f"[verbose] intento {attempt} falló en ficha: {e} url={url}")
                time.sleep(0.5)

        if verbose and last_err:
            print(f"[verbose] ficha FAIL definitivo: {last_err} url={url}")
        return None

    def search(self, query_text: str, *, verbose: bool = False) -> SearchResult:
        """
        1) Recolecta links desde listado (paginado).
        2) Abre cada ficha y extrae .product__price--siniva (exacto).
        """
        q = str(query_text or "").strip()
        if q == "":
            return SearchResult(count=0, prices=[])

        if q in self.cache:
            if verbose:
                print(f"[verbose] Cache hit para: '{q}'")
            return self.cache[q]

        self._ensure_page()
        assert self._page is not None

        total_links: Set[str] = set()

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

                try:
                    self._page.wait_for_selector(_PRODUCT_LINK_SELECTORS, timeout=min(self.cfg.timeout_ms, 20000))
                except PlaywrightTimeoutError:
                    if verbose:
                        print(f"[verbose] Página {page_num}: sin resultados (timeout selector). Stop.")
                    break

                page_links = self._scroll_to_load_more_links(verbose=verbose)

                if verbose:
                    print(f"[verbose] Página {page_num}: links encontrados={len(page_links)}")

                if not page_links:
                    # si no hay links con la navegación por URL, intentar búsqueda interactiva (fallback)
                    try:
                        if verbose:
                            print(f"[verbose] Página {page_num}: sin links con URL. Intentando búsqueda interactiva...")
                        inter_links = self._interactive_search_links(q, verbose=verbose)
                        if inter_links:
                            page_links = inter_links
                        else:
                            if verbose:
                                print(f"[verbose] Búsqueda interactiva no arrojó links para '{q}'")
                            break
                    except Exception:
                        break

                before_total = len(total_links)
                total_links |= page_links
                added = len(total_links) - before_total

                # Si no agrega links nuevos, cortar
                if page_num > 1 and added == 0:
                    if verbose:
                        print(f"[verbose] Página {page_num}: 0 links nuevos. Stop.")
                    break

                # Heurística de fin (si lista trae menos que per_page)
                if len(page_links) < self.cfg.per_page:
                    if verbose:
                        print(f"[verbose] Página {page_num}: < per_page ({self.cfg.per_page}). Fin.")
                    break

            except Exception as ex:
                if verbose:
                    print(f"[verbose] Error listado página {page_num}: {ex}")
                break

        # Extraer sin IVA exacto desde ficha
        links_list = list(total_links)

        # Opción de test: limitar fichas por búsqueda
        if self.cfg.max_details_per_query and self.cfg.max_details_per_query > 0:
            links_list = links_list[: self.cfg.max_details_per_query]
            if verbose:
                print(f"[verbose] LIMITANDO fichas a {len(links_list)} por --max-details-per-query")

        prices_exact: List[float] = []
        if verbose:
            print(f"\n[verbose] ===== FICHAS (sin IVA) =====")
            print(f"[verbose] fichas a procesar: {len(links_list)}")

        for idx, u in enumerate(links_list, start=1):
            p = self._extract_siniva_from_detail(u, verbose=verbose)
            if p and p > 0:
                prices_exact.append(p)

            if verbose and idx % 25 == 0:
                print(f"[verbose] fichas: {idx}/{len(links_list)} | precios_ok={len(prices_exact)}")

            time.sleep(self.cfg.detail_delay_s)

        result = SearchResult(count=len(total_links), prices=prices_exact)
        self.cache[q] = result

        if verbose:
            print(f"\n[verbose] ===== RESUMEN QUERY =====")
            print(f"[verbose] links únicos: {result.count}")
            print(f"[verbose] precios sin IVA OK: {len(result.prices)}")
            if result.prices:
                print(f"[verbose] min: €{result.min_price:.2f}")
                print(f"[verbose] max: €{result.max_price:.2f}")
            print(f"[verbose] ==========================\n")

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

            # Si no hay resultados, probar variantes: tokens alfanuméricos y quitar primera palabra
            if result.count == 0:
                tried = []
                # extraer tokens alfanuméricos largos (ej. 3A0949101A)
                tokens = re.findall(r"\b[A-Za-z0-9]{4,}\b", search_text)
                # probar el último token (normalmente el código)
                for t in (tokens[::-1] if tokens else []):
                    if t in tried:
                        continue
                    tried.append(t)
                    if verbose:
                        print(f"[verbose] Intentando variante token: '{t}' para fila {i+1}")
                    vres = counter.search(t, verbose=verbose)
                    if vres.count > 0:
                        result = vres
                        if verbose:
                            print(f"[verbose] Variante exitosa con token '{t}' -> links={vres.count}")
                        break

                # si aún 0, intentar quitar la primera palabra (ej. 'INTERMITENTE 3A0..' -> '3A0..')
                if result.count == 0:
                    parts = search_text.split()
                    if len(parts) > 1:
                        tail = " ".join(parts[1:])
                        if tail not in tried:
                            if verbose:
                                print(f"[verbose] Intentando variante sin primera palabra: '{tail}'")
                            vres = counter.search(tail, verbose=verbose)
                            if vres.count > 0:
                                result = vres
                                if verbose:
                                    print(f"[verbose] Variante exitosa con tail '{tail}' -> links={vres.count}")

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

    # Priorizar valores existentes en columnas en español, si están presentes y no vacías.
    def _has_nonempty(col: str) -> bool:
        return col in df_out.columns and df_out[col].astype(str).str.strip().replace("", pd.NA).notna().any()

    if _has_nonempty("PRECIO MAXIMO"):
        max_col = df_out["PRECIO MAXIMO"].astype(str).fillna("")
    else:
        max_col = out_max

    if _has_nonempty("PRECIO MINIMO"):
        min_col = df_out["PRECIO MINIMO"].astype(str).fillna("")
    else:
        min_col = out_min

    if _has_nonempty("CANTIDAD"):
        units_col = df_out["CANTIDAD"].astype(int)
    else:
        units_col = out_count.astype(int)

    # Construir DataFrame final con el orden requerido: ID, OEM, Units, Max Price, Min Price
    df_final = pd.DataFrame({
        "ID": id_col,
        "OEM": df_out["OEM"],
        "Units": units_col,
        "Max Price": max_col,
        "Min Price": min_col,
    }, index=df_out.index)

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

    # NUEVOS FLAGS para exactitud en ficha
    parser.add_argument("--detail-delay", type=float, default=0.25, help="Delay entre fichas (seg)")
    parser.add_argument("--detail-retries", type=int, default=2, help="Reintentos por ficha")
    parser.add_argument("--max-details-per-query", type=int, default=0, help="Limitar fichas por búsqueda (0 = sin límite)")
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

    input_file = locate_input_excel(folders['input'])
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
    )

    print("\n" + "=" * 70)
    print("INICIANDO EXTRACCIÓN EXACTA (SIN IVA CONFIRMADO EN FICHA)")
    print("=" * 70 + "\n")

    # Cache persistente para evitar reconsultas
    cache_path = os.path.join(folders['output'], "cache_oem.json")
    initial_cache = _load_cache_file(cache_path)

    # Si se solicita captura XHR, hacerlo para cada query y salir
    if args.capture_xhr:
        queries = [q.strip() for q in args.capture_xhr.split(",") if q.strip()]
        with EcoopartsCounter(cfg) as counter:
            for q in queries:
                print(f"Capturando XHR para: '{q}'")
                captures = counter.capture_xhr_for_query(q, folders['output'], verbose=args.verbose)
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
        debug_folder=folders['output'],
    )

    input_filename = os.path.basename(input_file)
    output_filename = generate_output_filename(input_filename)
    output_path = os.path.join(folders['output'], output_filename)

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
        except Exception as e:
            raise

    print(f"\n" + "=" * 70)
    print(f"✓ Resultados guardados en: {saved_path}")
    print("=" * 70)

    if args.move_to_done:
        done_path = os.path.join(folders['done'], input_filename)
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
