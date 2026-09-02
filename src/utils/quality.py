"""
A data-quality report, not a pass/fail test runner. Each check records
what was actually measured against an explicit threshold, across the 5
standard data-quality dimensions — so an E2E test produces evidence
("dedup rate measured at 0.081, injected rate was 0.080"), not just a
boolean.

Usage:
    report = QualityReport(pipeline="fintech-txn-integrity-pipeline")
    report.check(Dimension.CORRECTNESS, "dedup_rate_matches_injected",
                  measured=0.081, threshold=0.005, higher_is_better=False,
                  detail="measured vs injected rate delta")
    report.to_json("benchmarks/quality-report.json")
    report.to_markdown("docs/quality-report.md")
    report.assert_all_passed()  # for use inside pytest
"""

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path


class Dimension(StrEnum):
    COMPLETENESS = "completeness"  # did rows get lost between stages?
    CORRECTNESS = "correctness"  # does output match ground truth?
    CONSISTENCY = "consistency"  # does re-running produce the same result?
    VALIDITY = "validity"  # does everything conform to schema/domain?
    TIMELINESS = "timeliness"  # did it finish within SLA?


@dataclass
class QualityCheck:
    dimension: Dimension
    name: str
    measured: float
    threshold: float
    passed: bool
    detail: str
    higher_is_better: bool


@dataclass
class QualityReport:
    pipeline: str
    checks: list[QualityCheck] = field(default_factory=list)
    generated_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def check(
        self,
        dimension: Dimension,
        name: str,
        measured: float,
        threshold: float,
        higher_is_better: bool = True,
        detail: str = "",
    ) -> QualityCheck:
        passed = (measured >= threshold) if higher_is_better else (measured <= threshold)
        c = QualityCheck(dimension, name, measured, threshold, passed, detail, higher_is_better)
        self.checks.append(c)
        return c

    def score(self) -> dict:
        by_dim: dict[str, list[bool]] = {}
        for c in self.checks:
            by_dim.setdefault(c.dimension.value, []).append(c.passed)
        result = {dim: round(sum(passes) / len(passes), 4) for dim, passes in by_dim.items()}
        all_passes = [c.passed for c in self.checks]
        result["overall"] = round(sum(all_passes) / len(all_passes), 4) if all_passes else 0.0
        return result

    def failed_checks(self) -> list[QualityCheck]:
        return [c for c in self.checks if not c.passed]

    def assert_all_passed(self) -> None:
        failed = self.failed_checks()
        if failed:
            lines = [
                f"  [{c.dimension.value}] {c.name}: measured={c.measured} "
                f"{'<' if c.higher_is_better else '>'} threshold={c.threshold} — {c.detail}"
                for c in failed
            ]
            raise AssertionError(
                f"{len(failed)}/{len(self.checks)} quality checks failed for {self.pipeline}:\n"
                + "\n".join(lines)
            )

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "generated_at": self.generated_at,
            "score": self.score(),
            "checks": [
                {
                    "dimension": c.dimension.value,
                    "name": c.name,
                    "measured": c.measured,
                    "threshold": c.threshold,
                    "passed": c.passed,
                    "detail": c.detail,
                }
                for c in self.checks
            ],
        }

    def to_json(self, path: str) -> None:
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(self.to_dict(), indent=2))

    def to_markdown(self, path: str) -> None:
        score = self.score()
        lines = [
            f"# Quality report — {self.pipeline}",
            "",
            f"Generated: {self.generated_at}",
            "",
            f"**Overall score: {score['overall']:.0%}** "
            f"({sum(c.passed for c in self.checks)}/{len(self.checks)} checks passed)",
            "",
            "| Dimension | Score |",
            "|---|---|",
        ]
        for dim in Dimension:
            if dim.value in score:
                lines.append(f"| {dim.value} | {score[dim.value]:.0%} |")
        lines += [
            "",
            "## Checks",
            "",
            "| Dimension | Check | Measured | Threshold | Status | Detail |",
            "|---|---|---|---|---|---|",
        ]
        for c in self.checks:
            status = "PASS" if c.passed else "**FAIL**"
            lines.append(
                f"| {c.dimension.value} | {c.name} | {c.measured} | {c.threshold} "
                f"| {status} | {c.detail} |"
            )

        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text("\n".join(lines) + "\n")
