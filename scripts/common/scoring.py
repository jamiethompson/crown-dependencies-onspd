"""Config-driven scoring utilities."""

from __future__ import annotations


def clamp(value: int, *, minimum: int, maximum: int) -> int:
    return max(minimum, min(maximum, value))


def _evaluate_condition(condition: str, *, source_classes: set[str], coordinate_source: str | None) -> bool:
    condition = condition.strip()
    if condition.startswith("has_source(") and condition.endswith(")"):
        source = condition[len("has_source(") : -1]
        return source in source_classes
    if condition.startswith("coord_source(") and condition.endswith(")"):
        source = condition[len("coord_source(") : -1]
        return source == coordinate_source
    return False


def apply_scoring_profile(
    profile: dict,
    *,
    source_classes: set[str],
    coordinate_source: str | None,
) -> tuple[int, dict]:
    raw_score = 0
    applied_rules: list[str] = []

    for rule in profile.get("rules", []):
        condition = rule.get("when", "")
        if _evaluate_condition(condition, source_classes=source_classes, coordinate_source=coordinate_source):
            raw_score += int(rule.get("add", 0))
            applied_rules.append(rule.get("id", "unnamed_rule"))

    clamp_cfg = profile.get("clamp", {"min": 0, "max": 100})
    clamped_score = clamp(raw_score, minimum=int(clamp_cfg["min"]), maximum=int(clamp_cfg["max"]))

    explanation = {
        "applied_rules": applied_rules,
        "raw_score": raw_score,
        "clamped_score": clamped_score,
    }
    return clamped_score, explanation
