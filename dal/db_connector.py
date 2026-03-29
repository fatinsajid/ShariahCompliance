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
import numpy as np


@contextmanager
def get_cursor():
    """
    Context manager for a psycopg2 cursor using connection pool.
    Automatically commits or rolls back transactions.
    """
    conn = None
    try:
        conn = POOL.getconn()
        cur = conn.cursor()
        yield cur
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            cur.close()
            POOL.putconn(conn)

def serialize(obj):
    """Recursively convert all non-JSON-safe types"""

    if isinstance(obj, dict):
        return {k: serialize(v) for k, v in obj.items()}

    elif isinstance(obj, list):
        return [serialize(i) for i in obj]

    elif isinstance(obj, datetime):
        return obj.isoformat()

    elif isinstance(obj, uuid.UUID):
        return str(obj)

    # 🔥 CRITICAL FIX
    elif isinstance(obj, (np.bool_,)):
        return bool(obj)

    elif isinstance(obj, (np.integer,)):
        return int(obj)

    elif isinstance(obj, (np.floating,)):
        return float(obj)

    return obj
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
def fetch_companies(tenant_id: str) -> List[Dict]:
    """
    Fetch company financials and industry info from compliance_audit_log
    for a given tenant, selecting only necessary columns.
    """
    res = supabase.table("compliance_audit_log").select(
        "company_id, company_name, company_industry, "
        "total_assets, total_debt, total_income, "
        "non_halal_income, cash_and_interest_securities"
    ).eq("tenant_id", tenant_id).execute()

    companies = res.data if hasattr(res, "data") and res.data else []

    return [
        {
            "company_id": c["company_id"],
            "company_name": c.get("company_name") or "Unknown",
            "company_industry": c.get("company_industry") or "Unknown",
            "total_assets": float(c.get("total_assets") or 0),
            "total_debt": float(c.get("total_debt") or 0),
            "total_income": float(c.get("total_income") or 0),
            "non_halal_income": float(c.get("non_halal_income") or 0),
            "cash_and_interest_securities": float(c.get("cash_and_interest_securities") or 0),
        }
        for c in companies
    ]

def fetch_features(tenant_id: str):
    """
    Fetch numeric features for ML from compliance_audit_log
    """
    res = supabase.table("compliance_audit_log").select(
        "company_id, total_assets, total_debt, total_income, "
        "non_halal_income, cash_and_interest_securities, risk_score"
    ).eq("tenant_id", tenant_id).execute()

    companies = res.data if res.data else []

    features_list = []
    for c in companies:
        features_list.append({
            "company_id": c["company_id"],
            "debt_ratio": (c.get("total_debt") or 0) / ((c.get("total_assets") or 0) + 1),
            "liquidity_ratio": (c.get("cash_and_interest_securities") or 0) / ((c.get("total_assets") or 0) + 1),
            "non_halal_income_ratio": (c.get("non_halal_income") or 0) / ((c.get("total_income") or 0) + 1),
            "risk_score": c.get("risk_score") or 0,
        })

    return features_list

def populate_features(tenant_id: str):
    """
    Compute and store features for all companies of a tenant
    based on compliance_audit_log data.
    """
    companies = fetch_companies(tenant_id)

    for company in companies:
        # Example feature calculation (customize as needed)
        total_assets = company["total_assets"]
        total_debt = company["total_debt"]
        net_worth = total_assets - total_debt
        halal_income_ratio = (
            (company["total_income"] - company["non_halal_income"]) / company["total_income"]
            if company["total_income"] > 0 else 0
        )

        features = {
            "company_id": company["company_id"],
            "tenant_id": tenant_id,
            "net_worth": net_worth,
            "halal_income_ratio": halal_income_ratio,
            # Add more features here if needed
        }

        # Save or update features in a separate table if you have one,
        # or log them as needed.
        # Example: supabase.table("company_features").upsert(features).execute()
        print(f"Computed features for {company['company_name']}: {features}")

from typing import List, Dict
from dal.db_connector import supabase

def fetch_companies(tenant_id: str) -> List[Dict]:
    """
    Fetch companies' financial and industry info from compliance_audit_log
    for a given tenant. Only selects necessary columns for features.
    """
    res = supabase.table("compliance_audit_log").select(
        "company_id, company_name, company_industry, "
        "total_assets, total_debt, total_income, "
        "non_halal_income, cash_and_interest_securities"
    ).eq("tenant_id", tenant_id).execute()

    companies = res.data if hasattr(res, "data") and res.data else []

    return [
        {
            "company_id": c["company_id"],
            "company_name": c.get("company_name") or "Unknown",
            "company_industry": c.get("company_industry") or "Unknown",
            "total_assets": float(c.get("total_assets") or 0),
            "total_debt": float(c.get("total_debt") or 0),
            "total_income": float(c.get("total_income") or 0),
            "non_halal_income": float(c.get("non_halal_income") or 0),
            "cash_and_interest_securities": float(c.get("cash_and_interest_securities") or 0),
        }
        for c in companies
    ]
# -----------------------------
# Compliance Results
# -----------------------------

# dal/db_connector.py
import os

# Initialize Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)





def save_result(company_id: str, tenant_id: str, compliance_status: str, violations: list, extra_data: dict = None):
    """
    Save a compliance audit record to Supabase.
    """
    audit_data = {
        "company_id": company_id,
        "tenant_id": tenant_id,
        "compliance_status": compliance_status,
        "violations_count": len(violations),
        "audit_details": {"violations": violations},
        "created_at": datetime.utcnow().isoformat()
    }

    if extra_data:
        audit_data.update(extra_data)

    res = supabase.table("compliance_audit_log").insert(audit_data).execute()

    if hasattr(res, "status_code") and res.status_code >= 400:
        print("❌ Failed to save audit record:", res.data)
        return False
    if hasattr(res, "error") and res.error:
        print("❌ Failed to save audit record:", res.error)
        return False

    print(f"✅ Audit record saved for company {company_id}")
    return True
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
    log_entry.setdefault("created_at", datetime.utcnow().isoformat())
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
            "created_at": datetime.utcnow().isoformat()
        }
        insert_audit_log(log_entry)

    logger.info(f"✅ Full pipeline saved for company {company['company_id']}")