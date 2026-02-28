"""
Explainability Engine
Provides structured reasoning for Shariah compliance decisions
Thesis-grade minimal implementation
"""

from typing import Dict, List, Any


def generate_explanation(
    company: Dict[str, Any],
    status: str,
    violations: List[Dict],
    thresholds: Dict
) -> Dict[str, Any]:
    """
    Generate structured explainability output
    """

    explanation = {
        "company_id": company.get("company_id"),
        "overall_status": status,
        "summary": _build_summary(status, violations),
        "rule_analysis": [],
        "confidence": _compute_confidence(violations),
        "methodology": "AAOIFI quantitative screening (rule-based)"
    }

    # Build per-rule explanations
    for v in violations:
        explanation["rule_analysis"].append({
            "rule": v.get("rule"),
            "observed_value": v.get("value"),
            "threshold": v.get("threshold"),
            "status": "FAIL",
            "reason": _humanize_reason(v)
        })

    return explanation


# ========================
# Helper functions
# ========================

def _build_summary(status: str, violations: List[Dict]) -> str:
    if status == "COMPLIANT":
        return "Company passed all Shariah screening thresholds."
    return f"Company failed {len(violations)} Shariah screening rule(s)."


def _compute_confidence(violations: List[Dict]) -> float:
    """
    Thesis-friendly confidence heuristic
    """
    if not violations:
        return 0.95
    return max(0.5, 0.95 - (0.1 * len(violations)))


def _humanize_reason(v: Dict) -> str:
    rule = v.get("rule")
    value = v.get("value")
    threshold = v.get("threshold")

    return f"{rule} value {value} exceeds allowed threshold {threshold}."
# services/explainability_engine.py

# ================================
# ML Explainability Layer
# ================================

def generate_ml_explanation(features: dict) -> dict:
    """
    Placeholder for future SHAP/LIME integration.
    Currently rule-based surrogate.
    """

    return {
        "ml_explainability": {
            "method": "rule-based surrogate",
            "feature_importance": [],
            "note": "ML model not yet active"
        }
    }