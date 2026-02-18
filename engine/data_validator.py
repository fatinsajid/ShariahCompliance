def validate_company_data(company):
    errors = []

    if not company.get("company_id"):
        errors.append("Missing company ID")

    if company["total_assets"] <= 0:
        errors.append("Total assets must be greater than zero")

    if company["total_income"] <= 0:
        errors.append("Total income must be greater than zero")

    if company["total_debt"] < 0:
        errors.append("Total debt cannot be negative")

    if company["non_halal_income"] < 0:
        errors.append("Non-halal income cannot be negative")

    if company["total_debt"] > company["total_assets"]:
        errors.append("Total debt exceeds total assets")

    if company["non_halal_income"] > company["total_income"]:
        errors.append("Non-halal income exceeds total income")

    if not company.get("sector"):
        errors.append("Sector not specified")

    return errors
