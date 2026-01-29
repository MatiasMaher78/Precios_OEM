"""Script simplificado para verificar fallback en queries problemáticas"""
import sys
import os
import re
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import scrap

queries = [
    "CATALIZADOR MR597649",
    "ANTENA 6561TS",
    "ASIENTO 8906GC",
    "MECANISMO 6RU959801",
]

cfg = scrap.CounterConfig(headless=True, timeout_ms=15000, max_pages=2, block_resources=True)

print("="*70)
print("ANALISIS DE FALLBACK EN QUERIES PROBLEMATICAS")
print("="*70)

with scrap.EcoopartsCounter(cfg) as counter:
    for query in queries:
        print(f"\nQuery: {query}")
        
        # Buscar query completa
        result = counter.search(query, verbose=False)
        print(f"  Query completa: {result.count} links")
        
        # Extraer tokens y buscar el más largo
        tokens = re.findall(r"\b[A-Za-z0-9]{5,}\b", query)
        if tokens and result.count == 0:
            best_token = max(tokens, key=len)
            fallback = counter.search(best_token, verbose=False)
            print(f"  Fallback token '{best_token}': {fallback.count} links")
            if fallback.count > 0:
                print(f"    -> FALSO POSITIVO (min={fallback.min_price}, max={fallback.max_price})")

print("\n" + "="*70)
