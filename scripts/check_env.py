#!/usr/bin/env python3
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from utils.checks import check_endpoint_reachable, report  # noqa: E402

HEALTH_URL = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566").rstrip("/") + "/health"


def main() -> None:
    report("MiniStack reachable", check_endpoint_reachable(HEALTH_URL), HEALTH_URL)


if __name__ == "__main__":
    main()
