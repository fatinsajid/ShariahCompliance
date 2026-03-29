# ============================
# FULL REFACTORED ARCHITECTURE (SUPABASE ONLY)
# ============================

# ----------------------------
# 0️⃣ Imports
# ----------------------------
import os
import io
import json
from uuid import uuid4
from datetime import datetime
from typing import Optional, List, Dict

import numpy as np
import pandas as pd
import joblib

from fastapi import FastAPI, Request, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from jose import jwt, JWTError
from dotenv import load_dotenv

from supabase import create_client, Client

# ----------------------------
# 1️⃣ Environment Setup
# ----------------------------
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ----------------------------
# 2️⃣ FastAPI App
# ----------------------------
app = FastAPI(title="Shariah Compliance API - Supabase Refactored")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "https://shariahcompliance.onrender.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------
# 3️⃣ ML Models
# ----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

MODEL_PATH = os.path.join(BASE_DIR, "..", "models", "risk_model_v1.pkl")
ANOMALY_MODEL_PATH = os.path.join(BASE_DIR, "..", "models", "anomaly_model_v1.pkl")

MODEL_PATH = os.path.abspath(MODEL_PATH)
ANOMALY_MODEL_PATH = os.path.abspath(ANOMALY_MODEL_PATH)

model = None
anomaly_model = None

try:
    print("Loading model from:", MODEL_PATH)
    model = joblib.load(MODEL_PATH)
    print("✅ Model loaded")
except Exception as e:
    print("❌ Model load failed:", e)

try:
    print("Loading anomaly model from:", ANOMALY_MODEL_PATH)
    anomaly_model = joblib.load(ANOMALY_MODEL_PATH)
    print("✅ Anomaly model loaded")
except Exception as e:
    print("❌ Anomaly model load failed:", e)
if model is None or anomaly_model is None:
    raise HTTPException(
        status_code=500,
        detail="ML models not loaded. Check deployment."
    )

# ----------------------------
# 4️⃣ Utility Functions
# ----------------------------

def clean_for_json(obj):
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(v) for v in obj]
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, (np.integer,)):
        return int(obj)
    elif isinstance(obj, (np.floating,)):
        return float(obj)
    return obj

# ----------------------------
# 5️⃣ Middleware (Auth)
# ----------------------------
@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    auth_header = request.headers.get("Authorization")
    tenant_id = "demo-tenant"

    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        try:
            payload = jwt.decode(token, SUPABASE_JWT_SECRET, algorithms=["HS256"], audience="authenticated")
            tenant_id = payload.get("sub") or tenant_id
        except JWTError:
            return JSONResponse(status_code=401, content={"detail": "Invalid token"})

    request.state.tenant_id = tenant_id
    return await call_next(request)

# ----------------------------
# 6️⃣ DAL (Supabase Only)
# ----------------------------

def insert_audit_log(data: Dict):
    res = supabase.table("compliance_audit_log").insert(data).execute()
    return res.data


def fetch_audit_logs(tenant_id: str):
    res = supabase.table("compliance_audit_log").select("*").eq("tenant_id", tenant_id).execute()
    return res.data


def fetch_companies_from_logs(tenant_id: str):
    res = (
        supabase.table("compliance_audit_log")
        .select("company_id, company_name, company_industry, total_assets, total_debt, total_income, non_halal_income, cash_and_interest_securities")
        .eq("tenant_id", tenant_id)
        .execute()
    )
    return res.data

# ----------------------------
# 7️⃣ Service Layer
# ----------------------------

class FinalDecisionEngine:
    def __init__(self, tenant_id: str, model, anomaly_model):
        self.tenant_id = tenant_id
        self.model = model
        self.anomaly_model = anomaly_model

    def prepare_features(self, company_data):
        # Compute features your model was trained on
        debt_ratio = company_data['total_debt'] / company_data['total_assets'] if company_data[
                                                                                      'total_assets'] != 0 else 0
        liquidity_ratio = company_data['cash_and_interest_securities'] / company_data['total_assets'] if company_data[
                                                                                                             'total_assets'] != 0 else 0
        non_halal_income_ratio = company_data['non_halal_income'] / company_data['total_income'] if company_data[
                                                                                                        'total_income'] != 0 else 0

        return pd.DataFrame([{
            'debt_ratio': debt_ratio,
            'liquidity_ratio': liquidity_ratio,
            'non_halal_income_ratio': non_halal_income_ratio,
            'other_financial_metric1': 0,  # placeholder
            'other_financial_metric2': 0  # placeholder
        }])

    def evaluate_company(self, company: Dict):
        X = self._prepare_features([{
            "total_assets": company["total_assets"],
            "total_debt": company["total_debt"],
            "total_income": company["total_income"],
            "non_halal_income": company["non_halal_income"],
            "cash_and_interest_securities": company["cash_and_interest_securities"],
        }])

        risk_score = float(self.model.predict_proba(X)[0][1])
        anomaly_flag = self.anomaly_model.predict(X)[0]

        return {
            "risk_score": risk_score,
            "status": "compliant" if risk_score < 0.5 else "non-compliant",
            "violations": [],
            "explanation": ["Auto-generated explanation"],
            "anomalies": {"anomaly_flag": anomaly_flag}
        }
# ----------------------------
# 8️⃣ Pydantic Models
# ----------------------------

class CompanyInput(BaseModel):
    company_name: str
    company_industry: Optional[str] = None
    total_assets: float
    total_debt: float
    total_income: float
    non_halal_income: float
    cash_and_interest_securities: float

# ----------------------------
# 9️⃣ Core Pipeline
# ----------------------------

def run_pipeline(tenant_id: str, payload: CompanyInput):
    company_id = str(uuid4())

    company_data = payload.dict()
    company_data["company_id"] = company_id

    engine = FinalDecisionEngine(tenant_id, model, anomaly_model)
    result = engine.evaluate_company(company_data)
    result = clean_for_json(result)

    audit_record = {
        "audit_id": str(uuid4()),
        "tenant_id": tenant_id,
        "company_id": company_id,
        "company_name": payload.company_name,
        "company_industry": payload.company_industry,
        "created_at": datetime.utcnow().isoformat(),
        "audit_details": result,
        "violations_count": len(result.get("violations", [])),
        "risk_score": result.get("risk_score"),
        "explanation": json.dumps(result.get("explanation")),
        "anomaly_flag": str(result.get("anomalies", {}).get("anomaly_flag")),
        "total_assets": payload.total_assets,
        "total_debt": payload.total_debt,
        "total_income": payload.total_income,
        "non_halal_income": payload.non_halal_income,
        "cash_and_interest_securities": payload.cash_and_interest_securities,
        "compliance_status": result.get("status"),
        "rule_code": "SHARIAH_SCREENING",
        "fatwa_version": 1,
        "triggered_by": "api"
    }

    insert_audit_log(audit_record)

    return {"company_id": company_id, "result": result}

# ----------------------------
# 🔟 API Endpoints
# ----------------------------

@app.post("/api/analyze/single")
def analyze_single(payload: CompanyInput, request: Request):
    tenant_id = request.state.tenant_id
    return run_pipeline(tenant_id, payload)


@app.post("/api/analyze/bulk")
async def analyze_bulk(file: UploadFile = File(...), request: Request = None):
    tenant_id = request.state.tenant_id

    content = await file.read()
    df = pd.read_csv(io.BytesIO(content))

    results = []
    for _, row in df.iterrows():
        payload = CompanyInput(**row.to_dict())
        result = run_pipeline(tenant_id, payload)
        results.append(result)

    return {"processed": len(results), "results": results}


@app.get("/audit/logs")
def get_logs(request: Request):
    tenant_id = request.state.tenant_id
    return fetch_audit_logs(tenant_id)


@app.get("/companies")
def get_companies(request: Request):
    tenant_id = request.state.tenant_id
    return fetch_companies_from_logs(tenant_id)

# ----------------------------
# 1️⃣1️⃣ Health
# ----------------------------

@app.get("/health")
def health():
    return {"status": "ok"}
