import numpy as np

def calculate_risk_score(company: dict) -> dict:
    """
    Simple rule-based risk scoring (can later be replaced with ML model)
    """

    debt_ratio = company["total_debt"] / company["total_assets"]
    liquidity_ratio = company["cash_and_interest_securities"] / company["total_assets"]
    non_halal_ratio = company["non_halal_income"] / company["total_income"]

    # Weighted scoring logic
    risk_score = (
        debt_ratio * 40 +
        liquidity_ratio * 30 +
        non_halal_ratio * 30
    ) * 100

    if risk_score < 30:
        risk_level = "LOW"
    elif risk_score < 60:
        risk_level = "MEDIUM"
    else:
        risk_level = "HIGH"

    return {
        "risk_score": round(risk_score, 2),
        "risk_level": risk_level
    }
