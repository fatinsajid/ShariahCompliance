# ----------------------------
# 0️⃣ Imports
# ----------------------------
import os
import io
import json
import joblib
import pandas as pd
from fastapi import FastAPI, Request, HTTPException, UploadFile, File, Depends, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, RedirectResponse
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from jose import jwt, JWTError
from pydantic import BaseModel

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
    get_active_fatwa,
    fatwa_is_approved,
    run_shariah_governance
)
from services.explainability_engine import generate_explanation
from services.audit_logger import log_compliance_decision

# Routers & Auth
from app.routes.dashboard import router as dashboard_router
from app.auth import get_current_user
from router.app_router import app_router


# ----------------------------
# 1️⃣ FastAPI instance
# ----------------------------
app = FastAPI(
    title="Shariah Compliance API v2",
    description="API for risk prediction & compliance check",
    version="1.0"
)

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
    tenant_id = None

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
origins = ["http://localhost:5173","https://shariahcompliance.onrender.com"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

    reviews = fetch_scholar_reviews(company_id, tenant_id)
    result["scholar_reviews"] = reviews
    return result

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
    company_id: str
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

    # Save & update features
    save_company(company, tenant_id)
    save_result(company_id, tenant_id, status, violations)
    populate_features(tenant_id)

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
    tenant_id = getattr(request.state, "tenant_id", None)
    if not tenant_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    companies = fetch_companies(tenant_id)
    total = len(companies)
    compliant = 0
    non_compliant = 0
    total_violations = 0
    risk_scores = []
    recent_audit_logs = []

    if total == 0:
        return {
            "totalCompanies": 0,
            "compliancePercent": 0,
            "nonCompliancePercent": 0,
            "avgViolations": 0,
            "riskDistribution": [],
            "recentAuditLogs": []
        }

    for c in companies:
        result = fetch_result_by_company(c["company_id"], tenant_id)
        if not result:
            continue
        status_lower = result["status"].lower()
        if status_lower == "compliant":
            compliant += 1
        else:
            non_compliant += 1
        violations_count = len(result.get("violations", []))
        total_violations += violations_count
        risk_scores.append(result.get("risk_score", 0))
        recent_audit_logs.append({
            "company": c.get("name", "Unknown"),
            "status": result["status"].capitalize(),
            "violations": violations_count,
            "date": result.get("date", "N/A")
        })

    avg_violations = total_violations / total if total > 0 else 0
    return {
        "totalCompanies": total,
        "compliancePercent": round(compliant / total * 100, 1),
        "nonCompliancePercent": round(non_compliant / total * 100, 1),
        "avgViolations": round(avg_violations, 1),
        "riskDistribution": risk_scores,
        "recentAuditLogs": recent_audit_logs
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
@app.get("/companies")
def get_companies(request: Request):
    tenant_id = getattr(request.state, "tenant_id", "demo-tenant")
    # Placeholder: Supabase fetch
    return {
        "companies": [
            {"company_id": 1, "name": "ABC Corp", "status": "Compliant", "risk_score": 0.3},
            {"company_id": 2, "name": "XYZ Ltd", "status": "Non-Compliant", "risk_score": 0.85},
        ]
    }

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
# 19️⃣ Run
# ----------------------------
# Uncomment for local debugging
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)