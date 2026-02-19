def calculate_risk_score(company: dict) -> dict:

    total_assets = company.get("total_assets", 1)
    total_income = company.get("total_income", 1)

    if total_assets == 0:
        total_assets = 1

    if total_income == 0:
        total_income = 1

    debt_ratio = company["total_debt"] / total_assets
    liquidity_ratio = company["cash_and_interest_securities"] / total_assets
    non_halal_ratio = company["non_halal_income"] / total_income

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
