# services/shariah_rules.py

from dataclasses import dataclass
from typing import Callable, Dict, Any


@dataclass
class ShariahRule:
    rule_id: str
    description: str
    threshold_key: str
    severity: str
    check_fn: Callable[[Dict[str, Any], Dict[str, float]], bool]
def check_debt_ratio(company, thresholds):
    return company.get("debt_ratio", 0) <= thresholds["max_debt_ratio"]


def check_interest_income(company, thresholds):
    return company.get("interest_income_ratio", 0) <= thresholds["max_interest_income"]
SHARIAH_RULES = [
    ShariahRule(
        rule_id="SR-001",
        description="Debt ratio must not exceed threshold",
        threshold_key="max_debt_ratio",
        severity="HIGH",
        check_fn=check_debt_ratio,
    ),
    ShariahRule(
        rule_id="SR-002",
        description="Interest income must remain below threshold",
        threshold_key="max_interest_income",
        severity="HIGH",
        check_fn=check_interest_income,
    ),
]