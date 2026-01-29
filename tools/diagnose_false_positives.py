"""
Script para diagnosticar queries que reportan resultados falsos positivos.
Ejecuta queries específicas con verbose y guarda HTML para inspección manual.
"""
import sys
import os
import re

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import scrap

def diagnose_query(query: str, output_dir: str):
    """Ejecuta una query y muestra diagnóstico detallado."""
    print(f"\n{'='*70}")
    print(f"DIAGNOSTICANDO: {query}")
    print(f"{'='*70}\n")
    
    cfg = scrap.CounterConfig(
        headless=True,
        timeout_ms=15000,
        max_pages=5,
        per_page=30,
        block_resources=False,  # No bloquear para ver si es problema de carga
        prefer_discounted=True,
    )
    
    with scrap.EcoopartsCounter(cfg) as counter:
        # Ejecutar búsqueda con verbose
        result = counter.search(query, verbose=True)
        
        print(f"\n--- RESULTADO QUERY ORIGINAL ---")
        print(f"  Units (links): {result.count}")
        print(f"  Precios encontrados: {len(result.prices)}")
        if result.min_price is not None:
            print(f"  Min: €{result.min_price:.2f}")
        else:
            print(f"  Min: None")
        if result.max_price is not None:
            print(f"  Max: €{result.max_price:.2f}")
        else:
            print(f"  Max: None")
        
        # Simular la lógica de fallback (extraer token más largo)
        if result.count == 0:
            tokens = re.findall(r"\b[A-Za-z0-9]{5,}\b", query)
            if tokens:
                best_token = max(tokens, key=len)
                print(f"\n--- FALLBACK: PROBANDO TOKEN '{best_token}' ---")
                fallback_result = counter.search(best_token, verbose=True)
                
                print(f"\n--- RESULTADO FALLBACK TOKEN ---")
                print(f"  Units (links): {fallback_result.count}")
                print(f"  Precios encontrados: {len(fallback_result.prices)}")
                if fallback_result.min_price is not None:
                    print(f"  Min: €{fallback_result.min_price:.2f}")
                else:
                    print(f"  Min: None")
                if fallback_result.max_price is not None:
                    print(f"  Max: €{fallback_result.max_price:.2f}")
                else:
                    print(f"  Max: None")
                
                if fallback_result.count > 0:
                    print(f"\n[!] FALSO POSITIVO: El token '{best_token}' encontro {fallback_result.count} resultados")
                    print(f"    pero la query completa '{query}' no deberia tener resultados.")
                    result = fallback_result  # Usar fallback para guardar HTML
        
        print(f"\n{'='*70}")
        print(f"RESULTADO FINAL USADO:")
        print(f"  Units (links): {result.count}")
        print(f"  Precios encontrados: {len(result.prices)}")
        if result.min_price is not None:
            print(f"  Min: €{result.min_price:.2f}")
        else:
            print(f"  Min: None")
        if result.max_price is not None:
            print(f"  Max: €{result.max_price:.2f}")
        else:
            print(f"  Max: None")
        print(f"{'='*70}\n")
        
        # Obtener HTML de la primera página para inspección
        try:
            html = counter.get_search_page_html(query)
            safe_name = query.replace(" ", "_").replace("/", "_")[:50]
            html_path = os.path.join(output_dir, f"diag_{safe_name}.html")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"✓ HTML guardado en: {html_path}")
        except Exception as e:
            print(f"✗ Error guardando HTML: {e}")
    
    return result


if __name__ == "__main__":
    # Queries reportadas como problemáticas
    problematic_queries = [
        "CATALIZADOR MR597649",
        "ANTENA 6561TS",
        "ASIENTO 8906GC",
        "MECANISMO 6RU959801",
    ]
    
    output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Output")
    
    print("="*70)
    print("DIAGNÓSTICO DE QUERIES CON FALSOS POSITIVOS")
    print("="*70)
    
    for query in problematic_queries:
        diagnose_query(query, output_dir)
        print("\n" + "="*70 + "\n")
