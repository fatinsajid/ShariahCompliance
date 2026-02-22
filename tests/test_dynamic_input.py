import os
import json
from bootstrap import evaluate_single_company, load_thresholds

thresholds = load_thresholds()

def get_company_input():
    print("\nEnter Company Details")

    return {
        "company_id": input("Company ID: "),
        "total_assets": float(input("Total Assets: ")),
        "total_debt": float(input("Total Interest-Bearing Debt: ")),
        "total_income": float(input("Total Income: ")),
        "non_halal_income": float(input("Non-Halal Income: ")),
        "cash_and_interest_securities": float(input("Cash + Interest Securities: ")),
        "sector": input("Sector: ")
    }

while True:
    company = get_company_input()
    result = evaluate_single_company(company, thresholds)
    print("\nCompliance Result:")
    print(result)

    cont = input("\nEvaluate another company? (y/n): ")
    if cont.lower() != "y":
        break
