import urllib.request as urlreq
from urllib.error import URLError, HTTPError
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import scrap

queries = [
    "MANDOS 96588239XT",
    "INTERMITENTE 3A0949101A",
]

out_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Output")
ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"

for q in queries:
    url = scrap.build_ecooparts_search_url(q, page=1, per_page=30)
    print(f"Query: {q}")
    print(f"URL: {url}")
    try:
        req = urlreq.Request(url, headers={"User-Agent": ua, "Referer": "https://ecooparts.com/"})
        with urlreq.urlopen(req, timeout=20) as resp:
            raw = resp.read()
            try:
                text = raw.decode("utf-8")
            except Exception:
                text = raw.decode("latin-1", errors="ignore")
            status = resp.getcode()

        print(f"Status: {status} | len={len(text)}")
        found = re.findall(r'recambio-automovil-segunda-mano/|product__price--siniva|class="product', text)
        print(f"Matches product-like patterns: {len(found)}")
        safe_q = re.sub(r"[^A-Za-z0-9]+", "_", q)[:80]
        fname = os.path.join(out_folder, f"fetch_{safe_q}.html")
        with open(fname, "w", encoding="utf-8") as fh:
            fh.write(text)
        print(f"Saved HTML to: {fname}\n")
    except HTTPError as e:
        print(f"HTTP error: {e.code} {e.reason}\n")
    except URLError as e:
        print(f"URL error: {e.reason}\n")
    except Exception as e:
        print(f"Error fetching: {e}\n")
