# Test Checklist (Manual)

This checklist covers main flows without hitting live services in CI.

- Listing extraction:
  - Search a part that you know exists; verify Units (links count) matches visible cards in listing.
  - Confirm Min/Max prices match listing values (not VAT price when present).

- Fallback logic:
  - With a query like `CAJA MARIPOSA AIRE 9640795280` that returns 0 initially, ensure the longest alphanumeric token `9640795280` is used as the single fallback and produces results.
  - Verify no single-word fallbacks ("caja", "mariposa", "aire") are attempted.

- Cache behavior:
  - Run twice with the same input, verify cache files present in `Output/` and subsequent run is faster.

- CLI flags:
  - `--units-from-links`: Units equals count of collected links even if more prices parsed.
  - `--no-price-dedupe`: Min/Max computed from all prices; when duplicates exist, enabling this keeps original values.

- Error handling:
  - Missing `Input` file: program reports clear error and exits.
  - Permission error on saving Excel: alternate file is created with timestamp.
