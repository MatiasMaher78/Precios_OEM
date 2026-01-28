import os
import sys
import argparse
from pathlib import Path


CACHE_DIR_NAMES = {"__pycache__", ".pytest_cache"}
DEBUG_FILE_PATTERNS = [
    "fetch_*.html",
    "debug_row*.html",
    "manual_seq_*.html",
]


def remove_cache_dirs(root: Path, dry_run: bool = False) -> int:
    count = 0
    for p in root.rglob("*"):
        if p.is_dir() and p.name in CACHE_DIR_NAMES:
            if dry_run:
                print(f"[dry-run] would remove dir: {p}")
            else:
                try:
                    for sub in p.rglob("*"):
                        if sub.is_file():
                            sub.unlink(missing_ok=True)
                    # remove empty dirs bottom-up
                    for sub in sorted(p.rglob("*"), reverse=True):
                        if sub.is_dir():
                            sub.rmdir()
                    p.rmdir()
                    print(f"removed dir: {p}")
                    count += 1
                except Exception as e:
                    print(f"warn: failed removing {p}: {e}")
    return count


def remove_debug_files(output_dir: Path, dry_run: bool = False) -> int:
    count = 0
    for pattern in DEBUG_FILE_PATTERNS:
        for f in output_dir.glob(pattern):
            if f.is_file():
                if dry_run:
                    print(f"[dry-run] would remove file: {f}")
                else:
                    try:
                        f.unlink(missing_ok=True)
                        print(f"removed file: {f}")
                        count += 1
                    except Exception as e:
                        print(f"warn: failed removing {f}: {e}")
    return count


def main():
    parser = argparse.ArgumentParser(description="Clean cache folders and debug files from the workspace")
    parser.add_argument("--root", default=None, help="Workspace root (defaults to current working directory)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be removed without deleting")
    args = parser.parse_args()

    root = Path(args.root or os.getcwd())
    output_dir = root / "Output"

    print(f"root: {root}")
    cache_removed = remove_cache_dirs(root, dry_run=args.dry_run)
    print(f"cache directories removed: {cache_removed}")

    if output_dir.is_dir():
        debug_removed = remove_debug_files(output_dir, dry_run=args.dry_run)
        print(f"debug files removed: {debug_removed}")
    else:
        print("Output/ directory not found; skipping debug files")


if __name__ == "__main__":
    sys.exit(main())
