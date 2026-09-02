"""Unit tests for the shared helpers in src/utils/{synth,checks}.py.

These two modules are small but load-bearing:

- `synth.seeded_rng` is what makes every "deterministic by seed" claim in this
  portfolio's READMEs true. If it stopped being reproducible, every benchmark
  number would drift between runs and nobody would notice until the numbers
  disagreed with the docs.
- `synth.skewed_choice` produces the hot-key skew the Spark salting work exists
  to mitigate — if it generated a uniform distribution, the skew benchmark
  would be measuring nothing.
- `checks.report` is the OK/FAIL contract `make check-env` depends on: a failed
  check has to exit non-zero, or CI would go green on a broken environment.

Pure logic: no AWS, no Spark, no emulator needed.
"""

import http.server
import socket
import sys
import threading
import uuid
from collections import Counter
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from utils.checks import check_endpoint_reachable, report  # noqa: E402
from utils.synth import new_id, seeded_rng, skewed_choice  # noqa: E402

# --- synth ---------------------------------------------------------------


def test_same_seed_reproduces_the_same_sequence():
    # The invariant behind every `--seed 42` in this portfolio.
    first = [seeded_rng(42).random() for _ in range(3)]
    second = [seeded_rng(42).random() for _ in range(3)]
    assert first == second


def test_different_seeds_diverge():
    assert seeded_rng(42).random() != seeded_rng(43).random()


def test_new_id_returns_distinct_parseable_uuids():
    ids = [new_id() for _ in range(100)]
    assert len(set(ids)) == 100, "collisions here would silently merge distinct records"
    uuid.UUID(ids[0])  # raises if it isn't a well-formed UUID


def test_skewed_choice_concentrates_picks_on_the_hot_fraction():
    # The documented pattern: ~5% of items should absorb ~60% of picks. This is
    # what makes the partition-imbalance benchmark meaningful.
    rng = seeded_rng(42)
    items = [f"item-{i}" for i in range(100)]
    hot = set(items[:5])  # hot_fraction=0.05 of 100 items

    picks = Counter(
        skewed_choice(rng, items, hot_fraction=0.05, hot_weight=0.6) for _ in range(5000)
    )
    hot_share = sum(count for item, count in picks.items() if item in hot) / 5000

    assert hot_share == pytest.approx(0.6, abs=0.05), (
        f"expected ~60% of picks on the hot 5%, measured {hot_share:.1%}"
    )


def test_skewed_choice_is_deterministic_for_a_given_seed():
    a = [skewed_choice(seeded_rng(7), list(range(50))) for _ in range(1)]
    b = [skewed_choice(seeded_rng(7), list(range(50))) for _ in range(1)]
    assert a == b


def test_skewed_choice_always_has_at_least_one_hot_item():
    # With a tiny list, `hot_fraction` rounds to 0 items — the implementation
    # floors at 1 so the hot bucket is never empty and the call can't fail.
    rng = seeded_rng(42)
    assert skewed_choice(rng, ["only"], hot_fraction=0.05) == "only"


# --- checks --------------------------------------------------------------


def test_report_prints_ok_and_does_not_exit(capsys):
    report("env reachable", True)
    assert capsys.readouterr().out.strip() == "OK: env reachable"


def test_report_includes_the_detail_when_given(capsys):
    report("env reachable", True, "endpoint responded in 12ms")
    assert "OK: env reachable — endpoint responded in 12ms" in capsys.readouterr().out


def test_report_exits_nonzero_on_failure(capsys):
    # Without this, `make check-env` would pass on a broken environment and the
    # real failure would surface much later, as a confusing test timeout.
    with pytest.raises(SystemExit) as exc:
        report("ministack up", False, "connection refused")

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "FAIL: ministack up — connection refused" in out


def test_endpoint_reachable_is_false_for_a_closed_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        closed_port = s.getsockname()[1]  # bound then released: nothing listens here
    assert check_endpoint_reachable(f"http://127.0.0.1:{closed_port}", timeout=0.5) is False


def test_endpoint_reachable_is_true_for_a_live_200():
    server = http.server.HTTPServer(("127.0.0.1", 0), http.server.SimpleHTTPRequestHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        url = f"http://127.0.0.1:{server.server_address[1]}/"
        assert check_endpoint_reachable(url, timeout=2.0) is True
    finally:
        server.shutdown()
