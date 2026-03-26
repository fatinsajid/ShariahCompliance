from fastapi import APIRouter, Request

router = APIRouter()

@router.get("/overview")
def dashboard_overview(request: Request):
    tenant_id = getattr(request.state, "tenant_id", "demo-tenant")

    if tenant_id == "demo-tenant":
        return {
            "total_companies": 12,
            "compliance_pct": 75,
            "non_compliance_pct": 25,
            "avg_violations": 1.8,
            "risk_distribution": [0.1,0.3,0.5,0.2,0.7,0.9,0.15,0.4,0.8,0.6,0.25,0.35]
        }

    # Fetch from DB
    companies = fetch_companies(tenant_id)
    results = fetch_results(tenant_id)

    total = len(companies)
    compliant = sum(1 for r in results if r["status"] == "COMPLIANT")
    non_compliant = total - compliant
    total_violations = sum(len(r.get("violations", [])) for r in results)
    risk_scores = [r.get("risk_score", 0) for r in results]

    avg_violations = total_violations / total if total else 0

    return {
        "total_companies": total,
        "compliance_pct": (compliant / total * 100) if total else 0,
        "non_compliance_pct": (non_compliant / total * 100) if total else 0,
        "avg_violations": avg_violations,
        "risk_distribution": risk_scores
    }