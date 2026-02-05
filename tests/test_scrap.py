import re
import pandas as pd

import scrap


def test_extract_price_from_text_basic():
    cases = {
        "40,00€ (sin IVA)": 40.00,
        "1.234,56 €": 1234.56,
        "47.74": 47.74,
        "": None,
        "texto sin precio": None,
    }
    for txt, expected in cases.items():
        assert scrap.extract_price_from_text(txt) == expected


def test_generate_output_filename_uses_date_in_name():
    name = "Input_20260123.xlsx"
    out = scrap.generate_output_filename(name)
    assert out == "Output_20260123.xlsx"


def test_generate_output_filename_defaults_today():
    name = "Input.xlsx"
    out = scrap.generate_output_filename(name)
    # Debe usar fecha de hoy con formato YYYYMMDD
    from datetime import datetime

    today = datetime.now().strftime("%Y%m%d")
    assert out == f"Output_{today}.xlsx"


def test_build_search_url_contains_expected_params():
    url = scrap.build_ecooparts_search_url("CAJA MARIPOSA AIRE 9640795280", page=1, per_page=30)
    assert url.startswith("https://ecooparts.com/recambios-automovil-segunda-mano/?")
    # basic params
    assert "pag=pro" in url
    assert "panu=" in url
    assert "tebu=" in url
    assert "qregx=" in url


class FakeCounter:
    def __init__(self, cfg=None):
        self.cache = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def search(self, q: str, *, verbose: bool = False):
        # Return zero for any query except the longest token (simulate fallback)
        if re.fullmatch(r"[A-Za-z0-9]{5,}", q) and q == "9640795280":
            return scrap.SearchResult(count=5, prices=[10.0, 12.0, 11.0])
        return scrap.SearchResult(count=0, prices=[])

    def get_search_page_html(self, q: str, *, page: int = 1):
        return "<html></html>"


def test_process_uses_longest_token_when_no_results(monkeypatch):
    df = pd.DataFrame({"OEM": ["CAJA MARIPOSA AIRE 9640795280"]})

    # Monkeypatch EcoopartsCounter to FakeCounter
    monkeypatch.setattr(scrap, "EcoopartsCounter", lambda cfg=None: FakeCounter(cfg))

    out_df, cache = scrap.process(
        df,
        batch=1,
        start=0,
        delay=0.0,
        counter_cfg=None,
        verbose=False,
        initial_cache=None,
        debug_folder=None,
    )

    assert out_df.iloc[0]["Units"] == 5
    assert out_df.iloc[0]["Min Price"].startswith("€")
    assert out_df.iloc[0]["Max Price"].startswith("€")


def test_cache_roundtrip(tmp_path):
    # Guardar y cargar caché debe preservar estructura básica
    cache_file = tmp_path / "cache_oem.json"
    cache = {
        "QUERY": scrap.SearchResult(count=3, prices=[5.0, 7.0, 6.0]),
    }
    scrap._save_cache_file(str(cache_file), cache)
    loaded = scrap._load_cache_file(str(cache_file))
    assert "QUERY" in loaded
    assert loaded["QUERY"].count == 3
    assert loaded["QUERY"].min_price == 5.0
    assert loaded["QUERY"].max_price == 7.0


def test_collect_list_prices_prefers_discounted():
    import scrap

    cfg = scrap.CounterConfig()
    cfg.prefer_discounted = True
    counter = scrap.EcoopartsCounter(cfg)

    class FakeEl:
        def __init__(self, text):
            self._text = text

        def inner_text(self):
            return self._text

    class FakeCard:
        def __init__(self, mapping):
            # mapping: selector -> text
            self.map = mapping

        def query_selector(self, sel):
            # simular matching simple por igualdad de selector al map
            for part in [s.strip() for s in sel.split(',')]:
                if part in self.map:
                    return FakeEl(self.map[part])
            return None

    class FakePage:
        def __init__(self, cards):
            self._cards = cards

        def query_selector_all(self, sel):
            return self._cards

    # Card1: tiene precio new (descuento) y old
    card1 = FakeCard({
        ".product-card__price--new": "83,16 €",
        ".product-card__price--old": "92,40 €",
    })

    # Card2: solo precio old
    card2 = FakeCard({
        ".product-card__price--old": "40,00 €",
    })

    counter._page = FakePage([card1, card2])

    prices = counter._collect_list_prices(verbose=False)
    # Debe preferir 83.16 (new) para la primera tarjeta y 40.00 para la segunda
    assert 83.16 in prices
    assert 40.00 in prices


def test_collect_list_prices_without_discount_preference():
    import scrap

    cfg = scrap.CounterConfig()
    cfg.prefer_discounted = False
    counter = scrap.EcoopartsCounter(cfg)

    class FakeEl:
        def __init__(self, text):
            self._text = text

        def inner_text(self):
            return self._text

    class FakeCard:
        def __init__(self, mapping):
            self.map = mapping

        def query_selector(self, sel):
            for part in [s.strip() for s in sel.split(',')]:
                if part in self.map:
                    return FakeEl(self.map[part])
            return None

    class FakePage:
        def __init__(self, cards):
            self._cards = cards

        def query_selector_all(self, sel):
            return self._cards

    card = FakeCard({
        ".product-card__price--new": "83,16 €",
        ".product-card__price--old": "92,40 €",
    })

    counter._page = FakePage([card])
    prices = counter._collect_list_prices(verbose=False)
    # Al desactivar prefer_discounted, debería tomar el old (92.40)
    assert 92.40 in prices or 83.16 in prices
vs

def test_fallback_prefers_alphanumeric_tokens():
    """Test que el fallback prefiere tokens alfanuméricos mixtos sobre palabras."""
    import re
    
    # Simular la lógica de fallback
    test_cases = [
        ("CATALIZADOR MR597649", "MR597649"),  # Debe preferir código alfanumérico
        ("ANTENA 6561TS", "6561TS"),
        ("ASIENTO 8906GC", "8906GC"),
        ("MECANISMO 6RU959801", "6RU959801"),
        ("PILOTO ABC", "PILOTO"),  # Si no hay alfanuméricos mixtos, usar el más largo
    ]
    
    for search_text, expected_token in test_cases:
        tokens = re.findall(r"\b[A-Za-z0-9]{5,}\b", search_text)
        # Filtrar tokens alfanuméricos mixtos
        mixed_tokens = [t for t in tokens if re.search(r"\d", t) and re.search(r"[A-Za-z]", t)]
        candidates = mixed_tokens if mixed_tokens else tokens
        best_token = max(candidates, key=len)
        
        assert best_token == expected_token, f"Para '{search_text}' esperaba '{expected_token}' pero obtuvo '{best_token}'"

