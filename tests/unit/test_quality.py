"""Unit tests for the shared quality-report framework (src/utils/quality.py).

This module is what turns an E2E run into the `benchmarks/quality-report.json`
and `docs/quality-report.md` that this repo's README cites as evidence. If its
threshold comparison or scoring is wrong, every published quality number is
wrong with it — so the direction of the comparison, the per-dimension scoring,
and the "does a failure actually fail" path are all asserted directly here
rather than being trusted implicitly through the E2E suite.

Pure logic: no AWS, no Spark, no emulator needed.
"""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from utils.quality import Dimension, QualityReport  # noqa: E402


def test_higher_is_better_passes_at_or_above_threshold():
    report = QualityReport(pipeline="t")
    above = report.check(Dimension.COMPLETENESS, "above", measured=0.95, threshold=0.90)
    exactly = report.check(Dimension.COMPLETENESS, "exactly", measured=0.90, threshold=0.90)
    below = report.check(Dimension.COMPLETENESS, "below", measured=0.89, threshold=0.90)

    assert above.passed
    assert exactly.passed, "threshold is inclusive — measured == threshold must pass"
    assert not below.passed


def test_lower_is_better_inverts_the_comparison():
    # The direction matters: an SLA in seconds or a dedup-delta is "good when
    # small". Getting this backwards would silently mark every breach as a pass.
    report = QualityReport(pipeline="t")
    under = report.check(
        Dimension.TIMELINESS, "under", measured=110.0, threshold=120.0, higher_is_better=False
    )
    exactly = report.check(
        Dimension.TIMELINESS, "exactly", measured=120.0, threshold=120.0, higher_is_better=False
    )
    over = report.check(
        Dimension.TIMELINESS, "over", measured=121.0, threshold=120.0, higher_is_better=False
    )

    assert under.passed
    assert exactly.passed
    assert not over.passed, "a measurement above a lower-is-better threshold is a breach"


def test_score_is_per_dimension_plus_overall():
    report = QualityReport(pipeline="t")
    report.check(Dimension.COMPLETENESS, "a", measured=1.0, threshold=1.0)  # pass
    report.check(Dimension.COMPLETENESS, "b", measured=0.0, threshold=1.0)  # fail
    report.check(Dimension.VALIDITY, "c", measured=1.0, threshold=1.0)  # pass

    score = report.score()

    assert score[Dimension.COMPLETENESS.value] == 0.5, "1 of 2 completeness checks passed"
    assert score[Dimension.VALIDITY.value] == 1.0
    assert score["overall"] == pytest.approx(2 / 3, abs=1e-4), "2 of 3 checks overall"
    assert Dimension.TIMELINESS.value not in score, "dimensions with no checks are not reported"


def test_score_of_an_empty_report_is_zero_not_a_crash():
    # A pipeline that recorded no checks scores 0, rather than dividing by zero
    # — otherwise a run that silently skipped every check would look perfect.
    assert QualityReport(pipeline="t").score() == {"overall": 0.0}


def test_assert_all_passed_raises_and_names_every_failure():
    report = QualityReport(pipeline="my-pipeline")
    report.check(Dimension.CORRECTNESS, "ok_check", measured=1.0, threshold=1.0)
    report.check(
        Dimension.CORRECTNESS,
        "broken_check",
        measured=0.2,
        threshold=0.9,
        detail="rows lost between stages",
    )

    with pytest.raises(AssertionError) as exc:
        report.assert_all_passed()

    message = str(exc.value)
    assert "1/2 quality checks failed" in message
    assert "my-pipeline" in message
    assert "broken_check" in message
    assert "rows lost between stages" in message, "the detail is the actionable part"
    assert "ok_check" not in message, "passing checks are not noise in a failure report"


def test_assert_all_passed_is_silent_when_everything_passes():
    report = QualityReport(pipeline="t")
    report.check(Dimension.CONSISTENCY, "reproducible", measured=1.0, threshold=1.0)
    report.assert_all_passed()  # must not raise
    assert report.failed_checks() == []


def test_report_serializes_measured_values_not_just_verdicts(tmp_path):
    # The whole point of this framework over a plain pass/fail runner: the
    # artifact records what was measured against what was expected, so a
    # reader can judge the number instead of trusting a boolean.
    report = QualityReport(pipeline="t")
    report.check(
        Dimension.CORRECTNESS,
        "dedup_rate",
        measured=0.081,
        threshold=0.005,
        higher_is_better=False,
        detail="measured vs injected delta",
    )

    out = tmp_path / "nested" / "quality-report.json"
    report.to_json(str(out))
    payload = json.loads(out.read_text())

    assert payload["pipeline"] == "t"
    assert payload["generated_at"]
    assert payload["score"]["overall"] == 0.0, "the single check failed, so overall is 0"
    (check,) = payload["checks"]
    assert check["measured"] == 0.081
    assert check["threshold"] == 0.005
    assert check["detail"] == "measured vs injected delta"
    assert check["passed"] is False


def test_markdown_report_renders_every_check_and_marks_failures(tmp_path):
    report = QualityReport(pipeline="t")
    report.check(Dimension.VALIDITY, "passing_one", measured=1.0, threshold=1.0)
    report.check(Dimension.VALIDITY, "failing_one", measured=0.0, threshold=1.0)

    out = tmp_path / "quality-report.md"
    report.to_markdown(str(out))
    text = out.read_text()

    assert "passing_one" in text
    assert "failing_one" in text
    assert "**FAIL**" in text, "a failed check has to be visually findable in the doc"
    assert "50%" in text, "overall score is rendered as a percentage"
