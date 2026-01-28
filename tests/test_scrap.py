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
