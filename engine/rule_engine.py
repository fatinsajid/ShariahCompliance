def evaluate_compliance(companies):
    results = []

    for company in companies:
        company_id = company["company_id"]

        violations = []

        # --- AAOIFI Financial Screening Rules ---

        # Rule 1: Debt ratio ≤ 30%
        debt_ratio = company["total_debt"] / company["total_assets"]
        if debt_ratio > 0.30:
            violations.append("Debt ratio exceeds AAOIFI threshold (30%)")

        # Rule 2: Non-halal income ≤ 5%
        non_halal_ratio = company["non_halal_income"] / company["total_income"]
        if non_halal_ratio > 0.05:
            violations.append("Non-halal income exceeds AAOIFI threshold (5%)")

        # Rule 3: Prohibited business sector
        prohibited_sectors = [
            "Conventional Banking",
            "Alcohol",
            "Gambling",
            "Tobacco",
            "Adult Entertainment"
        ]

        if company["sector"] in prohibited_sectors:
            violations.append("Company operates in prohibited sector")

        # --- Final Compliance Decision ---
        status = "Compliant" if not violations else "Non-Compliant"

        results.append({
            "company_id": company_id,
            "status": status,
            "violations": violations
        })

    return results
