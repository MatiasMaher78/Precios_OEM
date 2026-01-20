import http.cookiejar
import urllib.request
import urllib.parse
import json
import base64
import random
import string
import re
import os
import sys

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Output")
if not os.path.isdir(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0 Safari/537.36"
)


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


def safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", s)[:80]


def manual_sequence(query: str):
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    opener.addheaders = [
        ("User-Agent", USER_AGENT),
        ("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
        ("Accept-Language", "es-ES"),
        ("Referer", "https://ecooparts.com/"),
    ]

    # 1) setgeoecoip.php POST
    setgeo_url = "https://ecooparts.com/functions/setgeoecoip.php"
    geo_payload = {"clientip": "45.179.75.234", "navv": "Chrome 120", "vert": "NAV:Chrome 120"}
    try:
        data = json.dumps(geo_payload).encode("utf-8")
        req = urllib.request.Request(setgeo_url, data=data, headers={"Content-Type": "application/json", "User-Agent": USER_AGENT, "Referer": "https://ecooparts.com/"})
        with opener.open(req, timeout=15) as resp:
            _ = resp.read()
    except Exception as e:
        print("setgeo failed:", e)

    # 2) session_data.php POST (gettradu)
    sess_url = "https://ecooparts.com/session_data.php"
    sess_data = urllib.parse.urlencode({"action": "gettradu", "sessionValue": ""}).encode("utf-8")
    try:
        req = urllib.request.Request(sess_url, data=sess_data, headers={"Content-Type": "application/x-www-form-urlencoded", "User-Agent": USER_AGENT, "Referer": "https://ecooparts.com/"})
        with opener.open(req, timeout=15) as resp:
            _ = resp.read()
    except Exception as e:
        print("session_data failed:", e)

    # 3) GET search URL
    url = build_ecooparts_search_url(query)
    print("Requesting:", url)
    try:
        with opener.open(url, timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print("GET search failed:", e)
        return False

    out_file = os.path.join(OUTPUT_DIR, f"manual_seq_{safe_name(query)}.html")
    with open(out_file, "w", encoding="utf-8") as fh:
        fh.write(html)
    print("Saved:", out_file)

    # Heurística: buscar enlaces de producto o selector de precio sin IVA
    found = False
    if re.search(r"/recambio-automovil-segunda-mano/", html, re.I):
        print("Found product href pattern in HTML")
        found = True
    if re.search(r"product__price--siniva", html, re.I):
        print("Found detail price selector in HTML")
        found = True

    print("Result for query '{}' -> product found: {}".format(query, found))
    return found


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python manual_seq.py 'QUERY1' ['QUERY2' ...]")
        sys.exit(1)
    for q in sys.argv[1:]:
        manual_sequence(q)
