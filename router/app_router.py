# src/routers/app_router.py
from fastapi import APIRouter, Depends, HTTPException, Request
from typing import Dict

app_router = APIRouter()

# Supabase auth placeholder
async def get_current_user(request: Request) -> Dict:
    token = request.headers.get("Authorization")
    if not token:
        raise HTTPException(status_code=401, detail="Unauthorized")
    # TODO: Verify token with Supabase
    return {"user_id": "user123", "email": "user@example.com"}


# Dashboard page
@app_router.get("/dashboard")
async def dashboard_page(user: Dict = Depends(get_current_user)):
    return {
        "totalCompanies": 12,
        "compliancePercent": 75,
        "nonCompliancePercent": 25,
        "avgViolations": 1.8,
        "user": user
    }


# Data Analysis page
@app_router.get("/data-analysis")
async def data_analysis_page(user: Dict = Depends(get_current_user)):
    return {
        "charts": {
            "riskDistribution": [0, 2, 4, 6],
            "complianceTrends": [80, 82, 78, 85]
        },
        "user": user
    }


# Companies page
@app_router.get("/companies")
async def companies_page(user: Dict = Depends(get_current_user)):
    return {
        "companies": [
            {"id": 1, "name": "ABC Ltd", "compliance": True},
            {"id": 2, "name": "XYZ Inc", "compliance": False},
        ],
        "user": user
    }


# Audit Logs page
@app_router.get("/audit-logs")
async def audit_logs_page(user: Dict = Depends(get_current_user)):
    return {
        "logs": [
            {"action": "create", "entity": "fatwa", "by": "Dr. Ahmed"},
            {"action": "update", "entity": "company", "by": "Admin"}
        ],
        "user": user
    }


# Scholar Reviews page
@app_router.get("/scholar-reviews")
async def scholar_reviews_page(user: Dict = Depends(get_current_user)):
    return {
        "reviews": [
            {"id": 101, "scholar": "Dr. Ahmed", "status": "approved"},
            {"id": 102, "scholar": "Sheikh Yusuf", "status": "pending"}
        ],
        "user": user
    }


# Login page (no auth required)
@app_router.post("/login")
async def login_page(email: str, password: str):
        # TODO: Implement Supabase login
    if email == "admin@example.com" and password == "password":
        return {"token": "fake-jwt-token"}
    raise HTTPException(status_code=401, detail="Invalid credentials")