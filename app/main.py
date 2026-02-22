import os
import joblib
import pandas as pd
from fastapi.responses import JSONResponse
from fastapi import FastAPI, Request, HTTPException
from jose import jwt, JWTError
from dal.db_connector import (
    get_user_tenant,
    fetch_companies,
    save_company,
    save_result,
    populate_features
)

app = FastAPI(title="Shariah Compliance API v2")

# ----------------------------
# Environment
# ----------------------------
SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET")
ALGORITHM = "HS256"  # default Supabase
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_PATH = os.path.join(PROJECT_ROOT, "models", "risk_model_v1.pkl")

# ----------------------------
# Load ML Model
# ----------------------------
try:
    model = joblib.load(MODEL_PATH)
    print("✅ Model loaded successfully.")
except Exception as e:
    model = None
    print(f"❌ Model failed to load: {e}")

# ----------------------------
# Middleware: Supabase JWT Auth
# ----------------------------
from fastapi.responses import JSONResponse  # ADD THIS IMPORT AT TOP


@app.middleware("http")
async def supabase_auth_middleware(request: Request, call_next):
    # allow public endpoints
    if request.url.path in ["/health", "/", "/docs", "/openapi.json"]:
        return await call_next(request)

    auth_header = request.headers.get("Authorization")

    if not auth_header or not auth_header.startswith("Bearer "):
        return JSONResponse(
            status_code=401,
            content={"detail": "Missing or invalid Authorization header"},
        )

    token = auth_header.split(" ")[1]

    try:
        payload = jwt.decode(
            token,
            SUPABASE_JWT_SECRET,
            algorithms=[ALGORITHM],
            audience="authenticated",  # ✅ important for Supabase
        )

        request.state.user = payload
        request.state.tenant_id = get_user_tenant(payload["sub"])

    except JWTError:
        return JSONResponse(
            status_code=401,
            content={"detail": "Invalid or expired token"},
        )

    return await call_next(request)
# ----------------------------
# Health Check
# ----------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

# ----------------------------
# Predict Risk
# ----------------------------
@app.get("/predict/{company_id}")
def predict(company_id: str, request: Request):
    tenant_id = request.state.tenant_id
    companies = fetch_companies(tenant_id)
    company = next((c for c in companies if c["company_id"] == company_id), None)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")
    tenant_id = request.state.tenant_id
    # Prepare features
    X = pd.DataFrame([{
        "total_assets": company["total_assets"],
        "total_debt": company["total_debt"],
        "total_income": company["total_income"],
        "non_halal_income": company["non_halal_income"],
        "cash_and_interest_securities": company["cash_and_interest_securities"]
    }])

    if model is None:
        raise HTTPException(status_code=500, detail="ML model not loaded")

    try:
        if hasattr(model, "predict_proba"):
            probs = model.predict_proba(X)
            risk_score = probs[0][1] if probs.shape[1] > 1 else probs[0][0]
        else:
            risk_score = model.predict(X)[0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {"company_id": company_id, "risk_score": float(risk_score)}

# ----------------------------
# Compliance Check
# ----------------------------
@app.get("/compliance/{company_id}")
def compliance(company_id: str, request: Request):
    tenant_id = request.state.tenant_id
    companies = fetch_companies(tenant_id)
    company = next((c for c in companies if c["company_id"] == company_id), None)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

    # Run compliance check (using your services)
    from services.compliance_engine import check_shariah_compliance
    from config.shariah_thresholds import THRESHOLDS
    status, violations = check_shariah_compliance(company, THRESHOLDS)

    # Save results
    save_company(company, tenant_id)
    save_result(company_id, tenant_id, status, violations)

    # Update ML features
    populate_features(tenant_id)

    return {"company_id": company_id, "status": status, "violations": violations}