import os
import json
import io
import joblib
import pandas as pd
from fastapi import FastAPI, Request, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
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
    if request.url.path.startswith("/dashboard"):
        return await call_next(request)
    # TEMPORARY: bypass auth for development
    if (
            request.url.path.startswith("/dashboard") or
            request.url.path.startswith("/audit") or
            request.url.path.startswith("/predict") or
            request.url.path.startswith("/compliance") or
            request.url.path.startswith("/screen") or
            request.url.path.startswith("/download")
    ):
        return await call_next(request)
    # if request.url.path in ["/health", "/", "/docs", "/openapi.json"]:
    #     return await call_next(request)

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

@app.post("/screen/bulk")
async def bulk_screen(request: Request, file: UploadFile = File(...)):

    tenant_id = request.state.tenant_id

    if file.filename.endswith(".csv"):
        df = pd.read_csv(file.file)

        results = []

        for _, row in df.iterrows():
            company_data = row.to_dict()

            result = run_shariah_governance(
                company_data=company_data,
                scholar_reviews=[]
            )

            results.append({
                "company_id": company_data.get("company_id"),
                "result": result
            })

        return {
            "processed": len(results),
            "results": results
        }

    elif file.filename.endswith(".pdf"):
        return {"message": "PDF processing not fully implemented yet"}

    else:
        raise HTTPException(status_code=400, detail="Unsupported file format")

@app.post("/download/csv")
def download_csv(results: list):
    """
    Accepts list of screening results and returns CSV file
    """

    try:
        # Flatten results
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

        return StreamingResponse(
            buffer,
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=results.csv"}
        )

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

    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=report.pdf"}
    )
@app.get("/dashboard/")
def dashboard_root():
    return RedirectResponse(url="/dashboard/overview")

@app.get("/dashboard/overview")
def dashboard_overview(request: Request):

    tenant_id = getattr(request.state, "tenant_id", "demo-tenant")
    if tenant_id == "demo-tenant":
        return {
            "total_companies": 12,
            "compliance_pct": 75,
            "non_compliance_pct": 25,
            "avg_violations": 1.8,
            "risk_distribution": [0.1, 0.3, 0.5, 0.2, 0.7, 0.9, 0.15, 0.4, 0.8, 0.6, 0.25, 0.35]
        }
    companies = fetch_companies(tenant_id)
    results = fetch_results(tenant_id)

    total = len(companies)
    compliant = sum(1 for r in results if r["status"] == "compliant")
    non_compliant = total - compliant
    avg_violations = sum(len(r["violations"]) for r in results) / max(total, 1)
    risk_distribution = [r.get("risk_score", 0) for r in results]

    for c in companies:
        result = fetch_result_by_company(c["company_id"], tenant_id)

        if not result:
            continue

        if result["status"] == "COMPLIANT":
            compliant += 1
        else:
            non_compliant += 1

        total_violations += len(result.get("violations", []))
        risk_scores.append(result.get("risk_score", 0))

    avg_violations = total_violations / total if total else 0

    return {
        "total_companies": total,
        "compliance_pct": (compliant / total * 100) if total else 0,
        "non_compliance_pct": (non_compliant / total * 100) if total else 0,
        "avg_violations": avg_violations,
        "risk_distribution": risk_scores
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
            }
            for i in range(12)
        ]

    logs = fetch_audit_logs(tenant_id)
    return logs

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # your Vite dev URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)