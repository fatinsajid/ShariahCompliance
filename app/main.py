# app/main.py
import os
import subprocess
import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException, Request
from dal import db_connector
from jose import jwt, JWTError
from app.gateway import api_gateway_middleware

# ----------------------------
# 1️⃣ FastAPI instance (ONLY ONCE)
# ----------------------------
app = FastAPI(
    title="Shariah Compliance API",
    description="API for risk prediction & compliance check",
    version="1.0"
)
app.middleware("http")(api_gateway_middleware)
# ----------------------------
# 2️⃣ Supabase Auth Config
# ----------------------------
SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET")
ALGORITHM = "HS256"

# ----------------------------
# 3️⃣ Auth Middleware
# ----------------------------
PUBLIC_PATHS = {"/", "/health", "/docs", "/openapi.json", "/redoc"}

async def supabase_auth_middleware(request: Request, call_next):
    if request.url.path in PUBLIC_PATHS:
        return await call_next(request)

    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    token = auth_header.split(" ")[1]

    try:
        payload = jwt.decode(token, SUPABASE_JWT_SECRET, algorithms=[ALGORITHM])
        request.state.user = payload
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    return await call_next(request)

app.middleware("http")(supabase_auth_middleware)

# ----------------------------
# 4️⃣ Paths
# ----------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_PATH = os.path.join(PROJECT_ROOT, "models", "risk_model_v1.pkl")
TRAIN_SCRIPT = os.path.join(PROJECT_ROOT, "models", "train_model.py")

# ----------------------------
# 5️⃣ Ensure model exists
# ----------------------------
if not os.path.exists(MODEL_PATH):
    print("⚠️ Model not found. Training now...")
    subprocess.run(["python", TRAIN_SCRIPT], check=True)
    print("✅ Model trained.")

# ----------------------------
# 6️⃣ Load model
# ----------------------------
try:
    model = joblib.load(MODEL_PATH)
    print("✅ Model loaded successfully.")
except Exception as e:
    print(f"❌ Failed to load model: {e}")
    model = None

# ----------------------------
# 7️⃣ Root
# ----------------------------
@app.get("/")
def root():
    return {
        "message": "Shariah Compliance API running",
        "endpoints": [
            "/health",
            "/predict/{company_id}",
            "/compliance/{company_id}"
        ]
    }

# ----------------------------
# 8️⃣ Health
# ----------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

# ----------------------------
# 9️⃣ Predict Risk (AUTH REQUIRED)
# ----------------------------
@app.get("/predict/{company_id}")
def predict(company_id: str, request: Request):
    if model is None:
        raise HTTPException(status_code=500, detail="Model not loaded")

    user = getattr(request.state, "user", {})
    # Optional role check
    # if user.get("role") != "analyst":
    #     raise HTTPException(status_code=403, detail="Insufficient permissions")

    try:
        company = db_connector.fetch_company_data(company_id)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

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
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "company_id": company_id,
        "risk_score": float(risk_score)
    }

# ----------------------------
# 🔟 Compliance Check (AUTH REQUIRED)
# ----------------------------
@app.get("/compliance/{company_id}")
def compliance_check(company_id: str, request: Request):
    try:
        company = db_connector.fetch_company_data(company_id)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

    violations = []

    if company["non_halal_income"] / max(company["total_income"], 1) > 0.05:
        violations.append("Non-halal income > 5%")

    if company["cash_and_interest_securities"] / max(company["total_assets"], 1) > 0.1:
        violations.append("High interest-bearing assets")

    status = "Non-Compliant" if violations else "Compliant"

    db_connector.save_result(company_id, status, violations)

    return {
        "company_id": company_id,
        "status": status,
        "violations": violations
    }
