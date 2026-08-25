"""
Shared building blocks for scripts/check_*.py — each check prints a
single OK/FAIL line so `make check-*` has an unambiguous pass/fail signal.
"""
import sys


def report(name: str, ok: bool, detail: str = "") -> None:
    status = "OK" if ok else "FAIL"
    line = f"{status}: {name}" + (f" — {detail}" if detail else "")
    print(line)
    if not ok:
        sys.exit(1)


def check_endpoint_reachable(url: str, timeout: float = 5.0) -> bool:
    import urllib.request
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.status == 200
    except Exception:
        return False
