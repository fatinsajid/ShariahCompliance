# ----------------------------
# 0️⃣ Imports
# ----------------------------
import os
import io
from uuid import uuid4
import joblib
import pandas as pd
from datetime import datetime
from fastapi import FastAPI, Request, HTTPException, UploadFile, File, Depends, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, RedirectResponse
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from jose import jwt, JWTError
from pydantic import BaseModel
from psycopg2 import connect, OperationalError
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from database import SessionLocal
from typing import Optional



# DAL
from dal.db_connector import (
    get_user_tenant,
    fetch_companies,
    fetch_result_by_company,
    fetch_audit_logs,
    save_company,
    save_result,
    populate_features,
    fetch_scholar_approvals
)

# Services
from services.final_decision_engine import FinalDecisionEngine
from services.event_publisher import publish_compliance_events
from services.shariah_governance import (
    check_shariah_compliance,
    run_shariah_governance
)
from services.explainability_engine import generate_explanation
from services.audit_logger import log_compliance_decision

# Routers & Auth
from app.routes.dashboard import router as dashboard_router
from app.auth import get_current_user
from router.app_router import app_router
from contextlib import asynccontextmanager
from models.compliance_audit_log import ComplianceAuditLog  # SQLAlchemy model

router = APIRouter()

load_dotenv()  # loads .env file


# ----------------------------
# Lifespan FIRST
# ----------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        app.state.db_conn = connect(DATABASE_URL, cursor_factory=RealDictCursor)
        print("✅ Database connected")
    except OperationalError as e:
        app.state.db_conn = None
        print(f"❌ DB connection error: {e}")

    yield

    db_conn = getattr(app.state, "db_conn", None)
    if db_conn:
        db_conn.close()
        print("Database connection closed")


# ----------------------------
# THEN FastAPI app
# ----------------------------
app = FastAPI(
    title="Shariah Compliance API v2",
    description="API for risk prediction & compliance check",
    version="1.0",
    lifespan=lifespan
)
origins = ["http://localhost:5173"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------
# Helper function: Run analysis + insert into audit log
# ----------------------------
def analyze_and_log_company(tenant_id: str, company: dict, triggered_by: str) -> dict:
    """
    Run governance analysis on a single company and save result in compliance_audit_log
    """
    engine = FinalDecisionEngine(tenant_id)
    result = engine.evaluate_company(company)

    audit_record = {
        "audit_id": str(uuid.uuid4()),
        "tenant_id": tenant_id,
        "company_id": company.get("company_id"),
        "rule_code": "SHARIAH_SCREENING",
        "fatwa_version": 1,
        "compliance_status": result.get("status"),
        "triggered_by": triggered_by,
        "created_at": datetime.utcnow().isoformat(),
        "company_name": company.get("company_name"),
        "company_industry": company.get("sector"),
        "audit_details": result,
        "violations_count": len(result.get("violations", [])),
        "risk_score": result.get("risk_score"),
        "explanation": str(result.get("explanation")),
        "scholar_reviews": result.get("scholar_reviews"),
        "anomaly_flag": str(result.get("anomalies", {}).get("anomaly_flag")),
        "total_assets": company.get("total_assets"),
        "total_debt": company.get("total_debt"),
        "total_income": company.get("total_income"),
        "non_halal_income": company.get("non_halal_income"),
        "cash_and_interest_securities": company.get("cash_and_interest_securities"),
    }

    insert_audit_log(audit_record)
    return result
# ----------------------------
# 2️⃣ Environment & Models
# ----------------------------
SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET")
ALGORITHM = "HS256"

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_PATH = os.path.join(PROJECT_ROOT, "models", "risk_model_v1.pkl")
ANOMALY_MODEL_PATH = os.path.join(PROJECT_ROOT, "models", "anomaly_model_v1.pkl")

# Load main ML model
try:
    model = joblib.load(MODEL_PATH)
    print("✅ ML model loaded successfully")
except Exception as e:
    model = None
    print(f"❌ ML model failed to load: {e}")

# Load anomaly detection model
try:
    anomaly_model = joblib.load(ANOMALY_MODEL_PATH)
    print("✅ Anomaly model loaded successfully")
except Exception as e:
    anomaly_model = None
    print(f"❌ Anomaly model failed to load: {e}")

DATABASE_URL = os.getenv("DATABASE_URL")
def get_db_conn():
    return connect(DATABASE_URL, cursor_factory=RealDictCursor)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: connect to DB
    try:
        app.state.db_conn = connect(DATABASE_URL, cursor_factory=RealDictCursor)
        print("✅ Database connected")
    except OperationalError as e:
        app.state.db_conn = None
        print(f"❌ DB connection error: {e}")

    yield  # Control returns to FastAPI here

    # Shutdown: close DB
    db_conn = getattr(app.state, "db_conn", None)
    if db_conn:
        db_conn.close()
        print("Database connection closed")


# ----------------------------
# 3️⃣ Utility: Role Check
# ----------------------------
def require_role(request: Request, allowed_roles: list):
    user_role = getattr(request.state, "role", None)
    if user_role not in allowed_roles:
        raise HTTPException(status_code=403, detail="Insufficient permissions")

# ----------------------------
# 4️⃣ Middleware: Supabase JWT Auth
# ----------------------------
@app.middleware("http")
async def supabase_auth_middleware(request: Request, call_next):
    """
    Handles authentication via Supabase JWT.
    Sets request.state.tenant_id for all routes.
    Dashboard is public if JWT is missing.
    """
    auth_header = request.headers.get("Authorization")
    tenant_id = getattr(request.state, "tenant_id", None)
    if not tenant_id:
        tenant_id = "550e8400-e29b-41d4-a716-446655440000"

    # 🔒 Decode JWT if provided
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        try:
            payload = jwt.decode(
                token,
                SUPABASE_JWT_SECRET,
                algorithms=[ALGORITHM],
                audience="authenticated"
            )
            tenant_info = get_user_tenant(payload.get("sub"))
            if tenant_info:
                tenant_id = tenant_info.get("tenant_id")
                request.state.role = tenant_info.get("role", "user")
        except JWTError:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or expired token"}
            )

    # ⚡ Public dashboard fallback
    if request.url.path.startswith("/dashboard") and tenant_id is None:
        tenant_id = "demo-tenant"

    request.state.tenant_id = tenant_id
    return await call_next(request)

# ----------------------------
# 5️⃣ Middleware: CORS
# ----------------------------

def get_db_connection():
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        return conn
    except Exception as e:
        print("DB connection error:", e)
        raise HTTPException(status_code=500, detail="Database connection failed")


# ----------------------------
# 6️⃣ Include Routers
# ----------------------------
app.include_router(dashboard_router)
app.include_router(app_router)

# ----------------------------
# 7️⃣ Health & Root
# ----------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/")
def root():
    return {
        "message": "Shariah Compliance API is running",
        "endpoints": ["/health", "/predict/{company_id}", "/compliance/{company_id}"]
    }

# ----------------------------
# 8️⃣ Risk Prediction
# ----------------------------
@app.get("/predict/{company_id}")
def predict(company_id: str, user=Depends(get_current_user)):
    tenant_id = user["tenant_id"]
    companies = fetch_companies(tenant_id)
    company = next((c for c in companies if c["company_id"] == company_id), None)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

    X = pd.DataFrame([{
        "total_assets": company["total_assets"],
        "total_debt": company["total_debt"],
        "total_income": company["total_income"],
        "non_halal_income": company["non_halal_income"],
        "cash_and_interest_securities": company["cash_and_interest_securities"]
    }])

    risk_score = model.predict_proba(X)[0][1] if hasattr(model, "predict_proba") else model.predict(X)[0]
    anomaly_flag = anomaly_model.predict(X)[0]

    return {
        "company_id": company_id,
        "risk_score": float(risk_score),
        "anomaly": True if anomaly_flag == -1 else False
    }

# ----------------------------
# 9️⃣ Compliance Check
# ----------------------------
@app.get("/compliance/{company_id}")
def compliance(company_id: str, request: Request):
    tenant_id = getattr(request.state, "tenant_id", None)
    if not tenant_id:
        raise HTTPException(status_code=401, detail="Tenant not found")

    companies = fetch_companies(tenant_id)
    company = next((c for c in companies if c["company_id"] == company_id), None)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

    engine = FinalDecisionEngine(tenant_id)
    result = engine.evaluate_company(company)

    reviews = fetch_scholar_approvals(company_id, tenant_id)
    result["scholar_reviews"] = reviews
    return result
import numpy as np

def clean_for_json(obj):
    """
    Convert NumPy types to native Python types for JSON serialization
    """
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(v) for v in obj]
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    return obj
# ----------------------------
# 10️⃣ Scholar Review Endpoints
# ----------------------------
@app.post("/scholar/review/{review_id}")
def review_compliance(review_id: str, request: Request, decision: str, comments: str = ""):
    user = request.state.user
    tenant_id = request.state.tenant_id

    if user.get("role") != "scholar":
        raise HTTPException(403, "Insufficient permissions")

    update_scholar_review(review_id, decision, comments)
    return {"review_id": review_id, "status": decision, "comments": comments}

@app.post("/assign_review/{company_id}")
def assign_review(company_id: str, request: Request):
    tenant_id = request.state.tenant_id
    compliance_result_id = 123
    scholar_id = "scholar-1"
    review_id = assign_scholar_review(tenant_id, company_id, compliance_result_id, scholar_id)
    return {"review_id": review_id}

# ----------------------------
# 11️⃣ Audit Logs
# ----------------------------
@app.get("/audit/compliance/{company_id}")
def audit_log(company_id: str, request: Request):
    tenant_id = request.state.tenant_id
    compliance_result = fetch_result_by_company(company_id, tenant_id)
    reviews = fetch_scholar_reviews(company_id, tenant_id)
    return {
        "company_id": company_id,
        "compliance_result": compliance_result,
        "scholar_reviews": reviews
    }

# ----------------------------
# 12️⃣ Screening: Single
# ----------------------------
class CompanyInput(BaseModel):
    company_name: str
    company_industry: Optional[str] = None
    total_assets: float
    total_debt: float
    total_income: float
    non_halal_income: float
    cash_and_interest_securities: float
    sector: str

@app.post("/screen")
def screen_company(payload: dict, request: Request):
    tenant_id = request.state.tenant_id
    company_id = payload.get("company_id")

    companies = fetch_companies(tenant_id)
    company = next((c for c in companies if c["company_id"] == company_id), None)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

    # ML Risk
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

    # Compliance
    status, violations = check_shariah_compliance(company, THRESHOLDS)
    anomalies = detect_anomaly(company)
    explanation = generate_explanation(company, status, violations, THRESHOLDS)

    # Governance
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
    result_payload = {
        "risk_score": float(risk_score),
        "status": status,
        "violations": violations,
        "explanation": explanation,
        "governance_flag": governance_flag,
        "scholar_reviews": reviews
    }
    save_full_pipeline(company, tenant_id, result_payload)
    # Save & update features
    save_full_pipeline(company, tenant_id, result_payload)

    # Optional event publish
    try:
        publish_compliance_events(tenant_id)
    except Exception as e:
        print(f"⚠️ Event publishing failed: {e}")

    # Scholar reviews
    reviews = fetch_scholar_approvals(company_id, tenant_id)

    return {
        "company_id": company_id,
        "risk_score": float(risk_score),
        "compliance_status": status,
        "violations": violations,
        "explanation": explanation,
        "governance_flag": governance_flag,
        "scholar_reviews": reviews
    }

# ----------------------------
# 13️⃣ Bulk Screening
# ----------------------------
@app.post("/screen/bulk")
async def bulk_screen(request: Request, file: UploadFile = File(...)):
    tenant_id = request.state.tenant_id
    if file.filename.endswith(".csv"):
        df = pd.read_csv(file.file)
        results = []
        for _, row in df.iterrows():
            company_data = row.to_dict()
            result = run_shariah_governance(company_data=company_data, scholar_reviews=[])
            results.append({"company_id": company_data.get("company_id"), "result": result})
        return {"processed": len(results), "results": results}
    elif file.filename.endswith(".pdf"):
        return {"message": "PDF processing not fully implemented yet"}
    else:
        raise HTTPException(status_code=400, detail="Unsupported file format")

# ----------------------------
# 14️⃣ Download CSV/PDF
# ----------------------------
@app.post("/download/csv")
def download_csv(results: list):
    try:
        flattened = []
        for item in results:
            company_id = item.get("company_id")
            result = item.get("result", {})
            flattened.append({
                "company_id": company_id,
                "status": result.get("status"),
                "risk_score": result.get("risk_score"),
                "anomaly_flag": result.get("anomaly", {}).get("anomaly_flag"),
                "violations": ", ".join(result.get("violations", []))
            })
        df = pd.DataFrame(flattened)
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        return StreamingResponse(buffer, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=results.csv"})
    except Exception as e:
        return {"error": str(e)}

@app.post("/download/pdf")
def download_pdf(results: list):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer)
    styles = getSampleStyleSheet()
    elements = []
    for item in results:
        company_id = item.get("company_id")
        result = item.get("result", {})
        text = f"""
        Company: {company_id}<br/>
        Status: {result.get("status")}<br/>
        Risk Score: {result.get("risk_score")}<br/>
        Violations: {', '.join(result.get("violations", []))}
        """
        elements.append(Paragraph(text, styles["Normal"]))
        elements.append(Spacer(1, 12))
    doc.build(elements)
    buffer.seek(0)
    return StreamingResponse(buffer, media_type="application/pdf", headers={"Content-Disposition": "attachment; filename=report.pdf"})

# ----------------------------
# 15️⃣ Dashboard Endpoints
# ----------------------------
@app.get("/dashboard/")
def dashboard_root():
    return RedirectResponse(url="/dashboard/overview")

@app.get("/dashboard/overview")
def dashboard_overview(request: Request):
    db_conn = getattr(request.app.state, "db_conn", None)

    if not db_conn:
        return {"error": "Database not connected"}

    with db_conn.cursor() as cur:
        # Total companies
        cur.execute("SELECT COUNT(*) FROM companies;")
        total = cur.fetchone()[0]

        # Compliance breakdown
        cur.execute("""
            SELECT status, COUNT(*)
            FROM companies
            GROUP BY status;
        """)
        rows = cur.fetchall()
        status_counts = {r[0]: r[1] for r in rows}

        compliant = status_counts.get("Compliant", 0)
        non_compliant = status_counts.get("Non-Compliant", 0)

        compliance_percent = (compliant / total * 100) if total else 0
        non_compliance_percent = (non_compliant / total * 100) if total else 0

        # Average violations
        cur.execute("SELECT AVG(violations) FROM audit_logs;")
        avg_violations = cur.fetchone()[0] or 0

        # Risk distribution
        cur.execute("""
            SELECT
                CASE
                    WHEN risk_score < 0.3 THEN 'low'
                    WHEN risk_score < 0.7 THEN 'medium'
                    ELSE 'high'
                END,
                COUNT(*)
            FROM companies
            GROUP BY 1;
        """)
        dist_rows = cur.fetchall()

        risk_map = {"low": 0, "medium": 0, "high": 0}
        for r in dist_rows:
            risk_map[r[0]] = r[1]

        # Recent logs
        cur.execute("""
            SELECT c.name, a.status, a.violations, a.created_at
            FROM audit_logs a
            JOIN companies c ON a.company_id = c.id
            ORDER BY a.created_at DESC
            LIMIT 5;
        """)
        logs = [
            {
                "company": r[0],
                "status": r[1],
                "violations": r[2],
                "date": r[3].strftime("%Y-%m-%d")
            }
            for r in cur.fetchall()
        ]

    return {
        "totalCompanies": total,
        "compliancePercent": compliance_percent,
        "nonCompliancePercent": non_compliance_percent,
        "avgViolations": float(avg_violations),
        "riskDistribution": [
            risk_map["low"],
            risk_map["medium"],
            risk_map["high"]
        ],
        "recentAuditLogs": logs
    }
@app.get("/dashboard/audit-logs")
def dashboard_audit_logs(request: Request):
    tenant_id = getattr(request.state, "tenant_id", "demo-tenant")
    if tenant_id == "demo-tenant":
        return [
            {
                "id": f"log-{i + 1}",
                "company_id": f"C{i + 1}",
                "rule_code": "SHARIAH_SCREENING",
                "status": "compliant" if i % 4 != 0 else "non-compliant",
                "fatwa_version": "v1.0",
                "created_at": "2026-03-24T12:00:00"
            } for i in range(12)
        ]
    logs = fetch_audit_logs(tenant_id)
    return logs

# ----------------------------
# 16️⃣ KPIs
# ----------------------------
@app.get("/dashboard/kpis")
def get_kpis(request: Request):
    tenant_id = request.state.tenant_id
    companies = fetch_companies(tenant_id)
    total = len(companies)
    if total == 0:
        return {"message": "No data"}

    compliant = 0
    total_violations = 0
    for c in companies:
        status, violations = check_shariah_compliance(c, THRESHOLDS)
        if status == "compliant":
            compliant += 1
        total_violations += len(violations)

    return {
        "total_companies": total,
        "compliant_percentage": round(compliant / total * 100, 2),
        "non_compliant_percentage": round((total - compliant) / total * 100, 2),
        "avg_violations": round(total_violations / total, 2)
    }

# ----------------------------
# 17️⃣ Companies
# ----------------------------
# -----------------------------
# Companies endpoint
# -----------------------------
@app.get("/companies")
def get_companies(request: Request):
    tenant_id = getattr(request.state, "tenant_id", "demo-tenant")  # optional tenant support

    try:
        response = supabase.table("companies").select("*").execute()
        companies = response.data
        if not companies:
            return {"companies": []}
        return {"companies": companies}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------
# Compliance Results endpoint
# -----------------------------
@app.get("/compliance_results/{company_id}")
def get_compliance_by_company(company_id: int, request: Request):
    """
    Fetch compliance details for a single company by ID
    Includes company name from companies table
    """
    try:
        # Join-like fetch: get the compliance result for company_id
        response = (
            supabase.table("compliance_results")
            .select("company_id, risk_score, compliance_status, violations, explanation")
            .eq("company_id", company_id)
            .execute()
        )
        results = response.data

        if not results:
            return {"compliance": None}  # fallback for frontend

        compliance_data = results[0]

        # Fetch company name
        company_resp = (
            supabase.table("companies")
            .select("name")
            .eq("company_id", company_id)
            .maybe_single()
            .execute()
        )

        company_name = company_resp.data["name"] if company_resp.data else "Unknown Company"
        compliance_data["company_name"] = company_name

        return {"compliance": compliance_data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
# ----------------------------
# 18️⃣ Events
# ----------------------------
@app.get("/events")
def get_events(request: Request):
    tenant_id = getattr(request.state, "tenant_id", "demo-tenant")
    # Placeholder events
    return [
        {"id": 1, "event": "Company C1 compliance check", "status": "ok"},
        {"id": 2, "event": "Company C2 screening", "status": "warning"},
    ]
# ----------------------------
# Single company audit endpoint
# ----------------------------
@app.post("/audit/single/{company_id}")
def audit_single_company(company_id: str, request: Request):
    tenant_id = getattr(request.state, "tenant_id", None)
    if not tenant_id:
        return JSONResponse(status_code=401, content={"detail": "Tenant not found"})

    # fetch company data from DB or service
    company = fetch_company_by_id(tenant_id, company_id)
    if not company:
        return JSONResponse(status_code=404, content={"detail": f"Company {company_id} not found"})

    result = analyze_and_log_company(tenant_id, company, triggered_by="single_screen")
    return result

# ----------------------------
# Bulk audit endpoint
# ----------------------------
@app.post("/audit/bulk")
def audit_bulk(request: Request, file: UploadFile = File(...)):
    tenant_id = getattr(request.state, "tenant_id", None)
    if not tenant_id:
        return JSONResponse(status_code=401, content={"detail": "Tenant not found"})

    if not file.filename.endswith(".csv"):
        return JSONResponse(status_code=400, content={"detail": "Only CSV files supported"})

    df = pd.read_csv(file.file)
    results = []

    for _, row in df.iterrows():
        company = row.to_dict()
        result = analyze_and_log_company(tenant_id, company, triggered_by="bulk_screen")
        results.append({"company_id": company.get("company_id"), "result": result})

    return {"processed": len(results), "results": results}
class SingleCompanyRequest(BaseModel):
    company_name: str
    company_industry: str
    total_assets: float
    total_debt: float
    total_income: float
    non_halal_income: float
    cash_and_interest_securities: float
@app.post("/api/analyze/bulk")
async def analyze_bulk(file: UploadFile = File(...)):
    try:
        content = await file.read()
        # FinalDecisionEngine should have a bulk CSV method
        results = FinalDecisionEngine().analyze_bulk_csv(content)

        # Insert all entries into compliance_audit_log
        db = get_db_session()
        for r in results:
            audit_entry = ComplianceAuditLog(
                audit_id=str(uuid4()),
                company_id = str(uuid.uuid4()),
                company_name=r.get("company_name"),
                company_industry=r.get("company_industry"),
                created_at=datetime.utcnow().isoformat(),
                audit_details=r.get("audit_details"),
                violations_count=r.get("violations_count"),
                risk_score=r.get("risk_score"),
                explanation=r.get("explanation"),
                scholar_reviews=r.get("scholar_reviews"),
                anomaly_flag=r.get("anomaly_flag"),
                total_assets=r.get("total_assets"),
                total_debt=r.get("total_debt"),
                total_income=r.get("total_income"),
                non_halal_income=r.get("non_halal_income"),
                cash_and_interest_securities=r.get("cash_and_interest_securities"),
                compliance_status=r.get("compliance_status"),
                rule_code=r.get("rule_code"),
                fatwa_version=r.get("fatwa_version"),
                triggered_by="bulk_analysis"
            )
            db.add(audit_entry)
        db.commit()

        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/analyze/single")
async def analyze_single_company(payload: CompanyInput, request: Request):

    try:
        db = SessionLocal()
        tenant_id = getattr(request.state, "tenant_id", "demo-tenant")
        company_id = str(uuid4())

        payload = {
            "company_id": company_id,
            "company_name": data.company_name,
            "sector": data.company_industry,
            "total_assets": data.total_assets,
            "total_debt": data.total_debt,
            "total_income": data.total_income,
            "non_halal_income": data.non_halal_income,
            "cash_and_interest_securities": data.cash_and_interest_securities,
        }

        result = FinalDecisionEngine(tenant_id).evaluate_company(payload.dict())

        audit_entry = ComplianceAuditLog(
            audit_id=str(uuid4()),
            company_id=company_id,
            company_name=data.company_name,
            company_industry=data.company_industry,
            created_at=datetime.utcnow().isoformat(),
            audit_details=result,
            violations_count=result.get("violations_count", 0),
            risk_score=result.get("risk_score", 0),
            explanation=result.get("explanation", ""),
            scholar_reviews=result.get("scholar_reviews", []),
            anomaly_flag=result.get("anomaly_flag", False),
            total_assets=data.total_assets,
            total_debt=data.total_debt,
            total_income=data.total_income,
            non_halal_income=data.non_halal_income,
            cash_and_interest_securities=data.cash_and_interest_securities,
            compliance_status=result.get("compliance_status", "unknown"),
            rule_code=result.get("rule_code", "SHARIAH_SCREENING"),
            fatwa_version=result.get("fatwa_version", 1),
            triggered_by="single_analysis"
        )


        db.close()

        return {"company_id": company_id, "result": result}


    except Exception as e:

        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
# ----------------------------
# 19️⃣ Run
# ----------------------------
# Uncomment for local debugging
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)# ----------------------------
# 0️⃣ Imports
# ----------------------------
