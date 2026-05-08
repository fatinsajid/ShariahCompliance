# ============================
# FULL REFACTORED ARCHITECTURE (SUPABASE ONLY)
# ============================

# ----------------------------
# 0️⃣ Imports
# ----------------------------
import os
import io
import json
from uuid import uuid4, NAMESPACE_URL, UUID
from datetime import datetime
from typing import Optional, List, Dict

import numpy as np
import pandas as pd

from fastapi.responses import JSONResponse
from fastapi import FastAPI, Request, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
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
    allow_origins=[
        "http://localhost:5173",
        "https://shariahcompliance.onrender.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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

    tenant_id = None  # default safe

    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        try:
            payload = jwt.decode(
                token,
                SUPABASE_JWT_SECRET,
                algorithms=["HS256"],
                audience="authenticated"
            )
            tenant_id = payload.get("sub")
        except JWTError:
            return JSONResponse(status_code=401, content={"detail": "Invalid token"})

    # fallback (VERY IMPORTANT for UUID column)
    if not tenant_id:
        raise HTTPException(status_code=401, detail="Missing tenant context")

    # Ensure valid UUID
    tenant_id = str(UUID(tenant_id))

    request.state.tenant_id = tenant_id
    return await call_next(request)
# ----------------------------
# 6️⃣ DAL (Supabase Only)
# ----------------------------

def insert_audit_log(data: Dict):
    res = supabase.table("compliance_audit_log").insert(data).execute()
    return res.data
def insert_audit_logs_bulk(data_list: list):
    if not data_list:
        return []
    res = supabase.table("compliance_audit_log").insert(data_list).execute()
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
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id

    def compute_metrics(self, company: Dict):
        total_assets = company.get("total_assets", 1)
        total_debt = company.get("total_debt", 0)
        total_income = company.get("total_income", 1)
        non_halal_income = company.get("non_halal_income", 0)
        cash = company.get("cash_and_interest_securities", 0)

        debt_ratio = total_debt / (total_assets + 1)
        liquidity_ratio = cash / (total_assets + 1)
        non_halal_ratio = non_halal_income / (total_income + 1)

        risk_score = (
            0.5 * debt_ratio +
            0.3 * non_halal_ratio +
            0.2 * liquidity_ratio
        )

        return {
            "risk_score": round(risk_score, 4),
            "debt_ratio": debt_ratio,
            "liquidity_ratio": liquidity_ratio,
            "non_halal_income_ratio": non_halal_ratio
        }
    def evaluate_company(self, company: Dict):

        metrics = self.compute_metrics(company)

        # Simple rule-based compliance
        violations = []

        if metrics["debt_ratio"] > 0.33:
            violations.append("Debt ratio exceeds 33%")

        if metrics["non_halal_income_ratio"] > 0.05:
            violations.append("Non-halal income exceeds 5%")

        status = "non-compliant" if violations else "compliant"

        # Simple anomaly logic
        anomaly_flag = "yes" if metrics["debt_ratio"] > 0.6 else "no"

        return {
            "risk_score": metrics["risk_score"],
            "status": status,
            "violations": violations,
            "explanation": [
                f"Debt ratio: {metrics['debt_ratio']:.2f}",
                f"Non-halal income ratio: {metrics['non_halal_income_ratio']:.2f}",
                f"Liquidity ratio: {metrics['liquidity_ratio']:.2f}"
            ],
            "anomalies": {"anomaly_flag": anomaly_flag},
            "features": metrics
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
    company_id = str(uuid4(NAMESPACE_URL, f"{tenant_id}-{payload.company_name}"))

    company_data = payload.dict()
    company_data["company_id"] = company_id

    engine = FinalDecisionEngine(tenant_id)
    result = engine.evaluate_company(company_data)
    result = clean_for_json(result)
    features = result.get("features", {})

    audit_record = {
        "audit_id": str(uuid4()),
        "tenant_id": tenant_id,
        "company_id": company_id,
        "company_name": payload.company_name,
        "company_industry": payload.company_industry,
        "created_at": datetime.utcnow().isoformat(),
        "audit_details": result,
        "violations_count": len(result.get("violations", [])) or 0,
        "risk_score": result.get("risk_score") or 0,
        "explanation": json.dumps(result.get("explanation")),
        "anomaly_flag": str(result.get("anomalies", {}).get("anomaly_flag")),
        "total_assets": payload.total_assets,
        "total_debt": payload.total_debt,
        "total_income": payload.total_income,
        "non_halal_income": payload.non_halal_income,
        "cash_and_interest_securities": payload.cash_and_interest_securities,
        "compliance_status": result.get("status") or "unknown",
        "rule_code": "SHARIAH_SCREENING",
        "fatwa_version": "1",
        "triggered_by": "api",
        "debt_ratio": result.get("features", {}).get("debt_ratio"),
        "liquidity_ratio": result.get("features", {}).get("liquidity_ratio"),
        "non_halal_income_ratio": result.get("features", {}).get("non_halal_income_ratio"),
        }

    res = supabase.table("compliance_audit_log").insert(
        audit_record
    ).execute()

    return {"company_id": company_id, "result": result}

# ----------------------------
# 🔟 API Endpoints
# ----------------------------

@app.post("/api/analyze/single")
def analyze_single(payload: CompanyInput, request: Request):
    tenant_id = request.state.tenant_id
    return run_pipeline(tenant_id, payload)


# ----------------------------
# 🔹 Improved Bulk Endpoint
# ----------------------------
@app.post("/api/analyze/bulk")
async def analyze_bulk(request: Request, file: UploadFile = File(...)):
    tenant_id = request.state.tenant_id

    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files allowed")

    content = await file.read()
    df = pd.read_csv(io.BytesIO(content))

    # Normalize columns
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    audit_records = []
    results = []
    errors = []

    for idx, row in df.iterrows():
        try:
            clean_row = {
                k: (None if pd.isna(v) else v)
                for k, v in row.to_dict().items()
            }

            payload = CompanyInput(**clean_row)

            result = run_pipeline(tenant_id, payload)
            results.append(result)

        except Exception as e:
            errors.append({
                "row": int(idx),
                "error": str(e),
                "data": row.to_dict()
            })
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
