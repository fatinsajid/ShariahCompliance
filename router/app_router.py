# src/routers/app_router.py
from fastapi import APIRouter, Depends, HTTPException, Request
from typing import Dict
from sqlalchemy.orm import Session
from dal.db_connector import get_db_session  # Your SQLAlchemy session dependency
from datetime import datetime

app_router = APIRouter()


# ------------------------------
# Supabase auth placeholder
# ------------------------------
async def get_current_user(request: Request) -> Dict:
    token = request.headers.get("Authorization")
    if not token:
        raise HTTPException(status_code=401, detail="Unauthorized")
    # TODO: Verify token with Supabase
    return {"user_id": "user123", "email": "user@example.com"}


# ------------------------------
# Dashboard Overview with DB
# ------------------------------
@app_router.get("/dashboard/overview")
async def dashboard_overview(
    request: Request,
    db: Session = Depends(get_db_session),
):
    tenant_id = getattr(request.state, "tenant_id", "demo-tenant")
    # Total companies
    total_companies = db.execute("SELECT COUNT(*) FROM companies").scalar() or 0

    # Compliance percentage
    compliant_count = db.execute(
        "SELECT COUNT(*) FROM companies WHERE compliance = TRUE"
    ).scalar() or 0
    compliance_percent = round((compliant_count / total_companies) * 100, 2) if total_companies else 0
    non_compliance_percent = 100 - compliance_percent

    # Average violations
    avg_violations = db.execute(
        "SELECT AVG(violations) FROM audit_logs"
    ).scalar() or 0

    # Risk distribution example (replace with real DB query if you have risk levels)
    risk_distribution = [0.1, 0.3, 0.4, 0.2]

    # Recent audit logs (limit 5)
    recent_audit_logs = db.execute(
        "SELECT company, status, violations, date FROM audit_logs ORDER BY date DESC LIMIT 5"
    ).fetchall()

    recent_audit_logs_formatted = [
        {
            "company": row.company,
            "status": row.status,
            "violations": row.violations,
            "date": row.date.strftime("%Y-%m-%d") if isinstance(row.date, datetime) else str(row.date)
        }
        for row in recent_audit_logs
    ]

    return {
        "totalCompanies": total_companies,
        "compliancePercent": compliance_percent,
        "nonCompliancePercent": non_compliance_percent,
        "avgViolations": avg_violations,
        "riskDistribution": risk_distribution,
        "recentAuditLogs": recent_audit_logs_formatted,
        "user": user,
    }


# ------------------------------
# Other routes
# ------------------------------

@app_router.get("/data-analysis")
async def data_analysis_page(user: Dict = Depends(get_current_user)):
    return {
        "charts": {
            "riskDistribution": [0, 2, 4, 6],
            "complianceTrends": [80, 82, 78, 85]
        },
        "user": user
    }


@app_router.get("/companies")
async def companies_page(user: Dict = Depends(get_current_user)):
    return {
        "companies": [
            {"id": 1, "name": "ABC Ltd", "compliance": True},
            {"id": 2, "name": "XYZ Inc", "compliance": False},
        ],
        "user": user
    }


@app_router.get("/audit-logs")
async def audit_logs_page(user: Dict = Depends(get_current_user)):
    return {
        "logs": [
            {"action": "create", "entity": "fatwa", "by": "Dr. Ahmed"},
            {"action": "update", "entity": "company", "by": "Admin"}
        ],
        "user": user
    }


@app_router.get("/scholar-reviews")
async def scholar_reviews_page(user: Dict = Depends(get_current_user)):
    return {
        "reviews": [
            {"id": 101, "scholar": "Dr. Ahmed", "status": "approved"},
            {"id": 102, "scholar": "Sheikh Yusuf", "status": "pending"}
        ],
        "user": user
    }


@app_router.post("/login")
async def login_page(email: str, password: str):
    # TODO: Implement Supabase login
    if email == "admin@example.com" and password == "password":
        return {"token": "fake-jwt-token"}
    raise HTTPException(status_code=401, detail="Invalid credentials")