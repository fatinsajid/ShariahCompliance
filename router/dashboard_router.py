# main.py or dashboard_routes.py
from fastapi import APIRouter, Depends
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

router = APIRouter()

DATABASE_URL = os.getenv("DATABASE_URL")

def get_db_conn():
    return psycopg2.connect(DATABASE_URL)

@router.get("/dashboard/overview")
def dashboard_overview():
    try:
        conn = get_db_conn()
        cur = conn.cursor()

        # 1️⃣ Total companies
        cur.execute("SELECT COUNT(*) FROM companies;")
        total_companies = cur.fetchone()[0] or 0

        # 2️⃣ Compliance %
        cur.execute("""
            SELECT status, COUNT(*) 
            FROM companies 
            GROUP BY status;
        """)
        status_counts = dict(cur.fetchall())
        compliant = status_counts.get("Compliant", 0)
        non_compliant = status_counts.get("Non-Compliant", 0)
        compliance_percent = round((compliant / total_companies) * 100, 1) if total_companies else 0
        non_compliance_percent = round((non_compliant / total_companies) * 100, 1) if total_companies else 0

        # 3️⃣ Average violations
        cur.execute("SELECT AVG(violations) FROM audit_logs;")
        avg_violations = float(cur.fetchone()[0] or 0)

        # 4️⃣ Risk distribution (example: 5 bins)
        cur.execute("SELECT risk_score FROM risk_distribution;")
        risk_scores = [r[0] for r in cur.fetchall()]
        risk_distribution = risk_scores if risk_scores else [0, 0, 0, 0, 0]

        # 5️⃣ Recent Audit Logs (last 5)
        cur.execute("""
            SELECT c.name, a.status, a.violations, a.date
            FROM audit_logs a
            JOIN companies c ON a.company_id = c.id
            ORDER BY a.date DESC
            LIMIT 5;
        """)
        recent_audit_logs = [
            {"company": r[0], "status": r[1], "violations": r[2], "date": r[3].strftime("%Y-%m-%d")}
            for r in cur.fetchall()
        ]

        cur.close()
        conn.close()

        return {
            "totalCompanies": total_companies,
            "compliancePercent": compliance_percent,
            "nonCompliancePercent": non_compliance_percent,
            "avgViolations": avg_violations,
            "riskDistribution": risk_distribution,
            "recentAuditLogs": recent_audit_logs
        }

    except Exception as e:
        return {"error": str(e)}