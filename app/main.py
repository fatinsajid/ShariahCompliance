import os
import json
import joblib
import pandas as pd
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from jose import jwt, JWTError
from dal.db_connector import (
    get_user_tenant,
    fetch_companies,
    save_company,
    save_result,
    populate_features
)
from services.final_decision_engine import FinalDecisionEngine
from pydantic import BaseModel
from services.event_publisher import publish_compliance_events
from services.shariah_governance import check_shariah_compliance, fetch_scholar_approvals, get_active_fatwa, fatwa_is_approved, run_shariah_governance
from services.explainability_engine import generate_explanation
from services.audit_logger import log_compliance_decision

# ----------------------------
# 1️⃣ FastAPI instance
# ----------------------------
app = FastAPI(
    title="Shariah Compliance API v2",
    description="API for risk prediction & compliance check",
    version="1.0"
)

# ----------------------------
# 2️⃣ Environment & Model
# ----------------------------
SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET")
ALGORITHM = "HS256"

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_PATH = os.path.join(PROJECT_ROOT, "models", "risk_model_v1.pkl")
ANOMALY_MODEL_PATH = os.path.join(PROJECT_ROOT, "models", "anomaly_model_v1.pkl")

def require_role(request: Request, allowed_roles: list):
    user_role = getattr(request.state, "role", None)
    if user_role not in allowed_roles:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
# Load model safely
try:
    model = joblib.load(MODEL_PATH)
    print("✅ ML model loaded successfully")
except Exception as e:
    model = None
    print(f"❌ ML model failed to load: {e}")

# ----------------------------
# 3️⃣ Middleware: Supabase JWT Auth
# ----------------------------
@app.middleware("http")
async def supabase_auth_middleware(request: Request, call_next):
    # Public endpoints
    if request.url.path in ["/health", "/", "/docs", "/openapi.json"]:
        return await call_next(request)

    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return JSONResponse(
            status_code=401,
            content={"detail": "Missing or invalid Authorization header"}
        )

    token = auth_header.split(" ")[1]

    try:
        payload = jwt.decode(
            token,
            SUPABASE_JWT_SECRET,
            algorithms=[ALGORITHM],
            audience="authenticated"  # required for Supabase
        )

        tenant_info = get_user_tenant(payload["sub"])

        request.state.tenant_id = tenant_info["tenant_id"]
        request.state.role = tenant_info["role"]

    except JWTError:
        return JSONResponse(
            status_code=401,
            content={"detail": "Invalid or expired token"}
        )

    return await call_next(request)

# ----------------------------
# 4️⃣ Health Check
# ----------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

# ----------------------------
# 5️⃣ Root
# ----------------------------
@app.get("/")
def root():
    return {
        "message": "Shariah Compliance API is running",
        "endpoints": ["/health", "/predict/{company_id}", "/compliance/{company_id}"]
    }

# ----------------------------
# 6️⃣ Predict Risk
# ----------------------------


anomaly_model = joblib.load(ANOMALY_MODEL_PATH)

@app.get("/predict/{company_id}")
def predict(company_id: str, request: Request):
    tenant_id = request.state.tenant_id
    companies = fetch_companies(tenant_id)
    company = next((c for c in companies if c["company_id"] == company_id), None)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

    # Prepare features
    X = pd.DataFrame([{
        "total_assets": company["total_assets"],
        "total_debt": company["total_debt"],
        "total_income": company["total_income"],
        "non_halal_income": company["non_halal_income"],
        "cash_and_interest_securities": company["cash_and_interest_securities"]
    }])

    # Risk prediction
    risk_score = model.predict_proba(X)[0][1] if hasattr(model, "predict_proba") else model.predict(X)[0]

    # Anomaly detection
    anomaly_flag = anomaly_model.predict(X)[0]  # -1 = anomaly, 1 = normal

    return {
        "company_id": company_id,
        "risk_score": float(risk_score),
        "anomaly": True if anomaly_flag == -1 else False
    }
# ----------------------------
# 7️⃣ Compliance Check
# ----------------------------
@app.get("/compliance/{company_id}")
def compliance(company_id: str, request: Request):
    tenant_id = getattr(request.state, "tenant_id", None)
    if not tenant_id:
        raise HTTPException(status_code=401, detail="Tenant not found in request state")

    # Fetch companies for this tenant
    companies = fetch_companies(tenant_id)
    company = next((c for c in companies if c["company_id"] == company_id), None)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

    # ----------------------------
    # 1️⃣ Use Final Decision Engine
    # ----------------------------
    engine = FinalDecisionEngine(tenant_id)
    result = engine.evaluate_company(company)

    # ----------------------------
    # 2️⃣ Optionally return scholar reviews
    # ----------------------------
    from services.shariah_governance import fetch_scholar_reviews
    reviews = fetch_scholar_reviews(company_id, tenant_id)

    result["scholar_reviews"] = reviews

    return result
# ----------------------------
# Scholar Review: Approve/Reject
# ----------------------------
@app.post("/scholar/review/{review_id}")
def review_compliance(review_id: str, request: Request, decision: str, comments: str = ""):
    user = request.state.user
    tenant_id = request.state.tenant_id

    # Check role
    if user.get("role") != "scholar":
        raise HTTPException(403, "Insufficient permissions")

    # Update review
    from dal.db_connector import update_scholar_review
    update_scholar_review(review_id, decision, comments)

    return {"review_id": review_id, "status": decision, "comments": comments}


# ----------------------------
# Fetch Audit Log
# ----------------------------
@app.get("/audit/compliance/{company_id}")
def audit_log(company_id: str, request: Request):
    tenant_id = request.state.tenant_id
    from dal.db_connector import fetch_scholar_reviews, fetch_result_by_company

    compliance_result = fetch_result_by_company(company_id, tenant_id)
    reviews = fetch_scholar_reviews(company_id, tenant_id)

    return {
        "company_id": company_id,
        "compliance_result": compliance_result,
        "scholar_reviews": reviews
    }

@app.post("/assign_review/{company_id}")
def assign_review(company_id: str, request: Request):
    tenant_id = request.state.tenant_id
    compliance_result_id = 123  # get from your DB logic
    scholar_id = "scholar-1"    # pick a scholar dynamically or from request

    review_id = assign_scholar_review(tenant_id, company_id, compliance_result_id, scholar_id)
    return {"review_id": review_id}
# ----------------------------
# Publish Events (API Gateway trigger)
# ----------------------------
@app.post("/events/publish")
def publish_events(request: Request):
    tenant_id = request.state.tenant_id

    try:
        count = publish_compliance_events(tenant_id)

        return {
            "message": "Events published successfully",
            "tenant_id": tenant_id,
            "events_published": count
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
# ----------------------------
# Request Schema
# ----------------------------
class CompanyInput(BaseModel):
    company_id: str
    total_assets: float
    total_debt: float
    total_income: float
    non_halal_income: float
    cash_and_interest_securities: float
@app.post("/screen")
def screen_company(payload: CompanyInput):

    result = run_shariah_governance(
        company_data=payload.dict(),
        scholar_reviews=[]
    )

    return result

# Define input schema for API
class CompanyInput(BaseModel):
    company_id: str
    total_assets: float
    total_debt: float
    total_income: float
    non_halal_income: float
    cash_and_interest_securities: float
    sector: str

@app.post("/screen")
def screen_company(payload: dict, request: Request):
    """
    Integrated company screening:
    - ML risk score
    - Shariah compliance check
    - Anomaly detection
    - Explainability
    - Scholar/fatwa governance
    - Audit logging
    """
    tenant_id = request.state.tenant_id
    company_id = payload.get("company_id")

    # 1️⃣ Fetch company data
    companies = fetch_companies(tenant_id)
    company = next((c for c in companies if c["company_id"] == company_id), None)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

    # 2️⃣ ML Risk Prediction
    X = pd.DataFrame([{
        "total_assets": company["total_assets"],
        "total_debt": company["total_debt"],
        "total_income": company["total_income"],
        "non_halal_income": company["non_halal_income"],
        "cash_and_interest_securities": company["cash_and_interest_securities"]
    }])

    try:
        if hasattr(model, "predict_proba"):
            probs = model.predict_proba(X)
            risk_score = probs[0][1] if probs.shape[1] > 1 else probs[0][0]
        else:
            risk_score = model.predict(X)[0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ML prediction failed: {e}")

    # 3️⃣ Shariah Compliance
    from config.shariah_thresholds import THRESHOLDS
    status, violations = check_shariah_compliance(company, THRESHOLDS)

    # 4️⃣ Explainability
    explanation = generate_explanation(company, status, violations, THRESHOLDS)

    # 5️⃣ Governance & Fatwa check
    rule_code = "SHARIAH_SCREENING"
    fatwa = get_active_fatwa(rule_code, tenant_id)
    governance_flag = None

    if fatwa:
        fatwa_id, fatwa_version, ruling = fatwa
        if not fatwa_is_approved(fatwa_id):
            governance_flag = "Pending scholar approval"
        log_compliance_decision(
            tenant_id=tenant_id,
            company_id=company_id,
            rule_code=rule_code,
            fatwa_version=fatwa_version,
            status=status
        )

    # 6️⃣ Save results & update ML features
    save_company(company, tenant_id)
    save_result(company_id, tenant_id, status, violations)
    populate_features(tenant_id)

    # 7️⃣ Optional: publish event
    try:
        publish_compliance_events(tenant_id)
    except Exception as e:
        print(f"⚠️ Event publishing failed: {e}")

    # 8️⃣ Fetch scholar reviews
    reviews = fetch_scholar_approvals(company_id, tenant_id)

    # 9️⃣ Assemble response
    response = {
        "company_id": company_id,
        "risk_score": float(risk_score),
        "compliance_status": status,
        "violations": violations,
        "explanation": explanation,
        "governance_flag": governance_flag,
        "scholar_reviews": reviews
    }

    return response

