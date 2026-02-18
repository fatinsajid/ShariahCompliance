import json
import os
from dal.db_connector import save_company, save_result, initialize_tables


def load_thresholds():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(BASE_DIR, "config", "shariah_thresholds.json")
    with open(path, "r") as f:
        return json.load(f)

def evaluate_single_company(company, thresholds):
    try:
        violations = []

        # Qualitative Screening
        if company["sector"] in thresholds["prohibited_sectors"]:
            violations.append("Prohibited business activity")

        # Quantitative Screening
        debt_ratio = company["total_debt"] / company["total_assets"]
        non_halal_ratio = company["non_halal_income"] / company["total_income"]
        liquidity_ratio = company["cash_and_interest_securities"] / company["total_assets"]

        if debt_ratio > thresholds["debt_ratio_max"]:
            violations.append("Debt ratio exceeds 30%")

        if non_halal_ratio > thresholds["non_halal_income_ratio_max"]:
            violations.append("Non-halal income exceeds 5%")

        if liquidity_ratio > thresholds["liquidity_ratio_max"]:
            violations.append("Liquidity ratio exceeds 30%")

        status = "COMPLIANT" if not violations else "NON-COMPLIANT"

        save_company(company)
        save_result(company["company_id"], status, violations)

        return {
            "status": "SUCCESS",
            "result": {
                "company_id": company["company_id"],
                "compliance_status": status,
                "violations": violations
            }
        }

    except Exception as e:
        return {"status": "ERROR", "message": str(e)}

if __name__ == "__main__":
    initialize_tables()
    print("Shariah Compliance Engine Ready.")
