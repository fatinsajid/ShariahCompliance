# app/main.py
import os
import subprocess
import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi import Request, HTTPException
from dal import db_connector
from jose import jwt, JWTError


# 2️⃣ FastAPI instance
app = FastAPI(title="Shariah Compliance API")

SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET")  # From Render
ALGORITHM = "HS256"  # Supabase default
SUPABASE_URL = os.getenv("SUPABASE_URL")

async def supabase_auth_middleware(request: Request, call_next):
    # Allow health check without auth
    if request.url.path == "/health":
        return await call_next(request)

    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")

    token = auth_header.split(" ")[1]

    try:
        payload = jwt.decode(token, SUPABASE_JWT_SECRET, algorithms=[ALGORITHM])
        request.state.user = payload  # Attach user info to request
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    return await call_next(request)

app.middleware("http")(supabase_auth_middleware)

app = FastAPI(title="Shariah Compliance API", description="API for risk prediction & compliance check", version="1.0")

# ----------------------------
# 1️⃣ Paths
# ----------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_PATH = os.path.join(PROJECT_ROOT, "models", "risk_model_v1.pkl")
TRAIN_SCRIPT = os.path.join(PROJECT_ROOT, "models", "train_model.py")

# ----------------------------
# 2️⃣ Ensure model exists
# ----------------------------
if not os.path.exists(MODEL_PATH):
    print("⚠️ Model not found. Training now...")
    subprocess.run(["python", TRAIN_SCRIPT], check=True)
    print("✅ Model trained.")

# ----------------------------
# 3️⃣ Load the trained model
# ----------------------------
try:
    model = joblib.load(MODEL_PATH)
    print("✅ Model loaded successfully.")
except Exception as e:
    print(f"❌ Failed to load model: {e}")
    model = None

# ----------------------------
# 4️⃣ Root Endpoint
# ----------------------------
@app.get("/")
def root():
    return {
        "message": "Shariah Compliance API is running.",
        "endpoints": ["/health", "/predict/{company_id}", "/compliance/{company_id}"]
    }

# ----------------------------
# 5️⃣ Health Check
# ----------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

# ----------------------------
# 6️⃣ Predict Risk Endpoint
# ----------------------------
@app.get("/predict/{company_id}")
def predict(company_id: str):
    if model is None:
        raise HTTPException(status_code=500, detail="Model not loaded")

    try:
        company = db_connector.fetch_company_data(company_id)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

    # Prepare features (match model training)
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

    return {"company_id": company_id, "risk_score": float(risk_score)}

# ----------------------------
# 7️⃣ Compliance Check Endpoint
# ----------------------------
@app.get("/compliance/{company_id}")
def compliance_check(company_id: str):
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

    # Save result to DB
    db_connector.save_result(company_id, status, violations)

    return {"company_id": company_id, "status": status, "violations": violations}

@app.get("/predict/{company_id}")
def predict(company_id: str, request: Request):
    user = request.state.user
    # Optional: check role
    if user.get("role") != "analyst":
        return {"error": "Insufficient permissions"}

    # Fetch company & predict risk as before
    ...

