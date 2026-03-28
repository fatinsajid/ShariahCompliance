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
async def dashboard_overview():
    try:
        print("Dashboard route hit")  # <-- log when the route is called

        # TODO: Replace this with real DB fetch
        # Simulate fetching from database
        result = {
            "totalCompanies": 12,
            "compliancePercent": 75,
            "nonCompliancePercent": 25,
            "avgViolations": 1.8,
            "riskDistribution": [0.1, 0.2, 0.3, 0.4],
            "recentAuditLogs": [
                {"company": "ABC Ltd", "status": "Compliant", "violations": 0, "date": "2026-03-28"},
                {"company": "XYZ Inc", "status": "Non-Compliant", "violations": 3, "date": "2026-03-27"},
            ]
        }
        print("Dashboard data prepared:", result)
        return result

    except Exception as e:
        print("Dashboard error:", e)  # <-- log the actual exception
        raise HTTPException(status_code=500, detail="Internal Server Error")

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