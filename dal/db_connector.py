import os
from contextlib import contextmanager
from typing import List, Dict, Iterable, Any
import uuid
from datetime import datetime
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from config.db_config import DB_CONFIG
from sqlalchemy import text
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from supabase import create_client, Client


# -----------------------------
# 🔴 LOAD ENV FIRST (CRITICAL FIX)
# -----------------------------
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------------
# SQLAlchemy Setup
# -----------------------------
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db_session():
    try:
        yield get_db_session()
    finally:
        get_db_session.close()
    return SessionLocal()


# -----------------------------
# Psycopg2 Pool
# -----------------------------
POOL = SimpleConnectionPool(minconn=1, maxconn=10, dsn=DATABASE_URL)

def get_connection():
    if "database_url" not in DB_CONFIG or not DB_CONFIG["database_url"]:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DB_CONFIG["database_url"])

# -----------------------------
# Context Manager
# -----------------------------
def get_db_session() -> Session:
    db = SessionLocal()  # create a session
    try:
        yield db
        db.commit()        # commit changes if any
    except Exception:
        db.rollback()      # rollback on exception
        raise
    finally:
        db.close()         # close session

# -----------------------------
# Compliance Insert
# -----------------------------
def insert_compliance_record(payload: Dict[str, Any]) -> None:
    session = get_db_session()
    try:
        query = text("""
            INSERT INTO compliance_results (
                tenant_id,
                company_id,
                status,
                risk_score,
                details,
                created_at
            )
            VALUES (
                :tenant_id,
                :company_id,
                :status,
                :risk_score,
                :details,
                NOW()
            )
        """)
        session.execute(query, payload)
        session.commit()

        logger.info(
            f"✅ Compliance inserted: tenant={payload.get('tenant_id')} "
            f"company={payload.get('company_id')}"
        )

    except Exception as e:
        session.rollback()
        logger.error(f"❌ Insert compliance failed: {e}")
        raise
    finally:
        session.close()

# -----------------------------
# Initialize Tables
# -----------------------------
def initialize_tables():
    with get_cursor() as cur:
        cur.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto";')

        cur.execute("""
            CREATE TABLE IF NOT EXISTS tenants (
                tenant_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name VARCHAR NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_tenants (
                user_id UUID,
                tenant_id UUID,
                role VARCHAR DEFAULT 'member',
                PRIMARY KEY (user_id, tenant_id)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                company_id VARCHAR,
                tenant_id UUID,
                total_assets DOUBLE PRECISION,
                total_debt DOUBLE PRECISION,
                total_income DOUBLE PRECISION,
                non_halal_income DOUBLE PRECISION,
                cash_and_interest_securities DOUBLE PRECISION,
                sector VARCHAR,
                PRIMARY KEY (company_id, tenant_id)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS compliance_results (
                id SERIAL PRIMARY KEY,
                company_id VARCHAR,
                tenant_id UUID,
                compliance_status VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                violations TEXT
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS companies_features (
                company_id VARCHAR,
                tenant_id UUID,
                debt_ratio DOUBLE PRECISION,
                liquidity_ratio DOUBLE PRECISION,
                non_halal_income_ratio DOUBLE PRECISION,
                other_financial_metric1 DOUBLE PRECISION,
                other_financial_metric2 DOUBLE PRECISION,
                PRIMARY KEY (company_id, tenant_id)
            );
        """)

    logger.info("✅ Tables initialized")

# -----------------------------
# Tenant Management
# -----------------------------
def create_tenant(tenant_id: str = None, name: str = "default") -> str:
    if not tenant_id:
        tenant_id = str(uuid.uuid4())

    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO tenants (tenant_id, name)
            VALUES (%s, %s)
            ON CONFLICT (tenant_id) DO NOTHING;
        """, (tenant_id, name))

    logger.info(f"✅ Tenant created or exists: {tenant_id} ({name})")
    return tenant_id

def _get_user_tenant_basic(user_id: str) -> str:
    with get_cursor() as cur:
        cur.execute("""
            SELECT tenant_id
            FROM user_tenants
            WHERE user_id = %s
            LIMIT 1
        """, (user_id,))
        row = cur.fetchone()

    if not row:
        raise ValueError("User not assigned to any tenant")

    return row[0]

# 🔑 Main function used in main.py
def get_user_tenant(user_id: str) -> dict:
    with get_cursor() as cur:
        cur.execute("""
            SELECT tenant_id, role
            FROM profiles
            WHERE id = %s
        """, (user_id,))
        row = cur.fetchone()

    if not row:
        raise Exception(f"No profile found for user {user_id}")

    return {
        "tenant_id": str(row[0]),
        "role": row[1] or "analyst"
    }

def list_tenants() -> List[str]:
    with get_cursor() as cur:
        cur.execute("SELECT tenant_id FROM tenants")
        rows = cur.fetchall()
    return [r[0] for r in rows]

# -----------------------------
# Companies
# -----------------------------
def save_company(company: Dict, tenant_id: str):
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO companies (
                company_id, tenant_id,
                total_assets, total_debt, total_income,
                non_halal_income, cash_and_interest_securities,
                sector
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (company_id, tenant_id)
            DO UPDATE SET
                total_assets = EXCLUDED.total_assets,
                total_debt = EXCLUDED.total_debt,
                total_income = EXCLUDED.total_income,
                non_halal_income = EXCLUDED.non_halal_income,
                cash_and_interest_securities = EXCLUDED.cash_and_interest_securities,
                sector = EXCLUDED.sector;
        """, (
            company["company_id"],
            tenant_id,
            company.get("total_assets", 0),
            company.get("total_debt", 0),
            company.get("total_income", 0),
            company.get("non_halal_income", 0),
            company.get("cash_and_interest_securities", 0),
            company.get("sector", "Unknown"),
        ))

def stream_companies(companies: Iterable[Dict], tenant_id: str, batch_size: int = 500):
    buffer = []
    for company in companies:
        buffer.append(company)
        if len(buffer) >= batch_size:
            _bulk_upsert(buffer, tenant_id)
            buffer.clear()
    if buffer:
        _bulk_upsert(buffer, tenant_id)
    logger.info("✅ Streaming ingestion complete")

def _bulk_upsert(companies: List[Dict], tenant_id: str):
    with get_cursor() as cur:
        args = [
            (
                c["company_id"], tenant_id,
                c.get("total_assets", 0), c.get("total_debt", 0),
                c.get("total_income", 0), c.get("non_halal_income", 0),
                c.get("cash_and_interest_securities", 0),
                c.get("sector", "Unknown")
            )
            for c in companies
        ]
        execute_batch(cur, """
            INSERT INTO companies (
                company_id, tenant_id, total_assets, total_debt,
                total_income, non_halal_income, cash_and_interest_securities, sector
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (company_id, tenant_id)
            DO UPDATE SET
                total_assets = EXCLUDED.total_assets,
                total_debt = EXCLUDED.total_debt,
                total_income = EXCLUDED.total_income,
                non_halal_income = EXCLUDED.non_halal_income,
                cash_and_interest_securities = EXCLUDED.cash_and_interest_securities,
                sector = EXCLUDED.sector;
        """, args, page_size=500)

# -----------------------------
# ML Features
# -----------------------------
def populate_features(tenant_id: str):
    companies = fetch_companies(tenant_id)
    if not companies:
        logger.warning("⚠️ No companies for ML feature generation")
        return

    with get_cursor() as cur:
        for c in companies:
            debt_ratio = c["total_debt"] / (c["total_assets"] + 1)
            liquidity_ratio = c["cash_and_interest_securities"] / (c["total_assets"] + 1)
            non_halal_income_ratio = c["non_halal_income"] / (c["total_income"] + 1)
            other_financial_metric1 = c["total_assets"] / (c["total_debt"] + 1)
            other_financial_metric2 = c["total_income"] / (c["total_assets"] + 1)

            cur.execute("""
                INSERT INTO companies_features (
                    company_id, tenant_id,
                    debt_ratio, liquidity_ratio, non_halal_income_ratio,
                    other_financial_metric1, other_financial_metric2
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (company_id, tenant_id)
                DO UPDATE SET
                    debt_ratio = EXCLUDED.debt_ratio,
                    liquidity_ratio = EXCLUDED.liquidity_ratio,
                    non_halal_income_ratio = EXCLUDED.non_halal_income_ratio,
                    other_financial_metric1 = EXCLUDED.other_financial_metric1,
                    other_financial_metric2 = EXCLUDED.other_financial_metric2;
            """, (
                c["company_id"], tenant_id,
                debt_ratio, liquidity_ratio, non_halal_income_ratio,
                other_financial_metric1, other_financial_metric2
            ))

    logger.info("✅ ML features populated")

def fetch_companies(tenant_id: str) -> List[Dict]:
    with get_cursor() as cur:
        cur.execute("""
            SELECT company_id, total_assets, total_debt,
                   total_income, non_halal_income,
                   cash_and_interest_securities, sector
            FROM companies
            WHERE tenant_id = %s
        """, (tenant_id,))
        rows = cur.fetchall()

    return [
        {
            "company_id": r[0],
            "total_assets": float(r[1] or 0),
            "total_debt": float(r[2] or 0),
            "total_income": float(r[3] or 0),
            "non_halal_income": float(r[4] or 0),
            "cash_and_interest_securities": float(r[5] or 0),
            "sector": r[6] or "Unknown"
        }
        for r in rows
    ]

# -----------------------------
# Compliance Results
# -----------------------------
def save_result(company_id: str, tenant_id: str, status: str, violations: List[str]):
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO compliance_results (
                company_id, tenant_id, compliance_status, violations
            )
            VALUES (%s,%s,%s,%s);
        """, (
            company_id,
            tenant_id,
            status,
            ", ".join(violations) if violations else "None"
        ))

def fetch_results(tenant_id: str):
    with get_cursor() as cur:
        cur.execute("""
            SELECT company_id, compliance_status, violations, created_at
            FROM compliance_results
            WHERE tenant_id = %s
        """, (tenant_id,))
        rows = cur.fetchall()

    return [
        {
            "company_id": r[0],
            "status": r[1],
            "violations": r[2].split(", ") if r[2] else [],
            "created_at": r[3]
        }
        for r in rows
    ]

def fetch_result_by_company(company_id: str, tenant_id: str):
    with get_cursor() as cur:
        cur.execute("""
            SELECT compliance_status, violations, created_at
            FROM compliance_results
            WHERE company_id = %s AND tenant_id = %s
            ORDER BY created_at DESC
            LIMIT 1
        """, (company_id, tenant_id))
        row = cur.fetchone()

    if not row:
        return None

    return {
        "status": row[0],
        "violations": row[1].split(", ") if row[1] else [],
        "date": row[2]
    }

def fetch_audit_logs(tenant_id: str):
    with get_cursor() as cur:
        cur.execute("""
            SELECT company_id, compliance_status, violations, created_at
            FROM compliance_results
            WHERE tenant_id = %s
            ORDER BY created_at DESC
            LIMIT 20
        """, (tenant_id,))
        rows = cur.fetchall()

    return [
        {
            "company": r[0],
            "status": r[1],
            "violations": len(r[2].split(", ")) if r[2] else 0,
            "date": r[3]
        }
        for r in rows
    ]

# -----------------------------
# 🔹 Fatwa Access
# -----------------------------
def fetch_fatwa_by_id(fatwa_id: str, tenant_id: str):
    """Fetch a single fatwa record for a tenant"""
    response = (
        supabase
        .from_("fatwas")
        .select("fatwa_id, title, description, version, rule_code, ruling")
        .eq("fatwa_id", fatwa_id)
        .eq("tenant_id", tenant_id)
        .single()
        .execute()
    )
    if response.data:
        return response.data
    return None
def fetch_fatwa_by_rule(rule_code: str, tenant_id: str):
    """Fetch all fatwas for a given rule and tenant"""
    response = (
        supabase
        .from_("fatwas")
        .select("fatwa_id, title, description, version, rule_code, ruling")
        .eq("rule_code", rule_code)
        .eq("tenant_id", tenant_id)
        .execute()
    )
    return response.data or []
# -----------------------------
# Scholar Approvals
# -----------------------------
def fetch_scholar_approvals(fatwa_id: str):
    with get_cursor() as cur:
        cur.execute(
            "SELECT scholar_id, decision FROM scholar_review WHERE fatwa_id=%s",
            (fatwa_id,)
        )
        return cur.fetchall()
# -----------------------------
# Audit Logs
# -----------------------------
def insert_audit_log(log_entry: dict):
    """ Thesis-safe audit log insertion. For now, this prints the log to console.
        In a production system, this would persist to an audit table.
    """
    print(f"📜 Audit Log: {log_entry}")
    # Optional: persist timestamp if not provided
    log_entry.setdefault("created_at", datetime.utcnow())
    # Optional: you could append to an in-memory list for testing
    # _audit_logs.append(log_entry)

# -----------------------------
# 🔹 Full ML → DB Pipeline
# -----------------------------
def save_full_pipeline(company: Dict, tenant_id: str, result: Dict):
    """
    Saves a company, ML features, compliance result, and audit log in one transaction.
    """
    with get_cursor() as cur:
        # 1️⃣ Insert/update company
        cur.execute("""
            INSERT INTO companies (
                company_id, tenant_id, total_assets, total_debt,
                total_income, non_halal_income, cash_and_interest_securities, sector
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (company_id, tenant_id)
            DO UPDATE SET
                total_assets = EXCLUDED.total_assets,
                total_debt = EXCLUDED.total_debt,
                total_income = EXCLUDED.total_income,
                non_halal_income = EXCLUDED.non_halal_income,
                cash_and_interest_securities = EXCLUDED.cash_and_interest_securities,
                sector = EXCLUDED.sector;
        """, (
            company["company_id"], tenant_id,
            company.get("total_assets", 0),
            company.get("total_debt", 0),
            company.get("total_income", 0),
            company.get("non_halal_income", 0),
            company.get("cash_and_interest_securities", 0),
            company.get("sector", "Unknown")
        ))

        # 2️⃣ ML Features
        debt_ratio = company.get("total_debt", 0) / (company.get("total_assets", 0) + 1)
        liquidity_ratio = company.get("cash_and_interest_securities", 0) / (company.get("total_assets", 0) + 1)
        non_halal_income_ratio = company.get("non_halal_income", 0) / (company.get("total_income", 0) + 1)
        other_financial_metric1 = company.get("total_assets", 0) / (company.get("total_debt", 0) + 1)
        other_financial_metric2 = company.get("total_income", 0) / (company.get("total_assets", 0) + 1)

        cur.execute("""
            INSERT INTO companies_features (
                company_id, tenant_id, debt_ratio, liquidity_ratio, non_halal_income_ratio,
                other_financial_metric1, other_financial_metric2
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (company_id, tenant_id)
            DO UPDATE SET
                debt_ratio = EXCLUDED.debt_ratio,
                liquidity_ratio = EXCLUDED.liquidity_ratio,
                non_halal_income_ratio = EXCLUDED.non_halal_income_ratio,
                other_financial_metric1 = EXCLUDED.other_financial_metric1,
                other_financial_metric2 = EXCLUDED.other_financial_metric2;
        """, (
            company["company_id"], tenant_id,
            debt_ratio, liquidity_ratio, non_halal_income_ratio,
            other_financial_metric1, other_financial_metric2
        ))

        # 3️⃣ Compliance Result
        cur.execute("""
            INSERT INTO compliance_results (
                company_id, tenant_id, compliance_status, violations
            )
            VALUES (%s,%s,%s,%s)
        """, (
            company["company_id"], tenant_id,
            result.get("compliance_status", "pending"),
            ", ".join(result.get("violations", [])) or "None"
        ))

        # 4️⃣ Audit Log
        log_entry = {
            "company_id": company["company_id"],
            "tenant_id": tenant_id,
            "status": result.get("compliance_status", "pending"),
            "violations": len(result.get("violations", [])),
            "risk_score": result.get("risk_score", 0),
            "created_at": datetime.utcnow()
        }
        insert_audit_log(log_entry)

    logger.info(f"✅ Full pipeline saved for company {company['company_id']}")