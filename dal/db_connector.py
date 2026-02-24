import os
import logging
from contextlib import contextmanager
from typing import List, Dict, Iterable, Any
import uuid

import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from config.db_config import DB_CONFIG
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

def insert_compliance_record(payload: Dict[str, Any]) -> None:
    """
    Insert compliance result (async worker safe)
    Expected payload:
    {
        tenant_id,
        company_id,
        status,
        risk_score,
        details
    }
    """
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


def get_db_session():
    pass



def get_connection():
    if "database_url" not in DB_CONFIG or not DB_CONFIG["database_url"]:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DB_CONFIG["database_url"])
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------------
# Environment
# -----------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

# -----------------------------
# Connection Pool
# -----------------------------
POOL = SimpleConnectionPool(minconn=1, maxconn=10, dsn=DATABASE_URL)

# -----------------------------
# Context Manager for DB Access
# -----------------------------
@contextmanager
def get_cursor():
    conn = POOL.getconn()
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.exception("DB transaction failed")
        raise
    finally:
        cur.close()
        POOL.putconn(conn)

# -----------------------------
# Initialize Tables
# -----------------------------
def initialize_tables():
    with get_cursor() as cur:
        # Enable UUID generation
        cur.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto";')

        # Tenants
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tenants (
                tenant_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name VARCHAR NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # User ↔ Tenant
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_tenants (
                user_id UUID,
                tenant_id UUID,
                role VARCHAR DEFAULT 'member',
                PRIMARY KEY (user_id, tenant_id)
            );
        """)

        # Companies
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

        # Compliance Results
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

        # ML Features
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

def get_user_tenant(user_id: str) -> str:
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
        execute_batch(
            cur,
            """
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
            """,
            args, page_size=500
        )

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

def fetch_features(tenant_id: str) -> List[Dict]:
    with get_cursor() as cur:
        cur.execute("""
            SELECT company_id, debt_ratio, liquidity_ratio,
                   non_halal_income_ratio, other_financial_metric1, other_financial_metric2
            FROM companies_features
            WHERE tenant_id = %s
        """, (tenant_id,))
        rows = cur.fetchall()
    return [
        {
            "company_id": r[0],
            "debt_ratio": float(r[1] or 0),
            "liquidity_ratio": float(r[2] or 0),
            "non_halal_income_ratio": float(r[3] or 0),
            "other_financial_metric1": float(r[4] or 0),
            "other_financial_metric2": float(r[5] or 0)
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

# -----------------------------
# Optional: Supabase Push
# -----------------------------
def push_features_to_supabase(features: List[Dict]):
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.warning("⚠️ Supabase not configured")
        return
    from supabase import create_client
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    supabase.table("companies_features").upsert(features).execute()
def ensure_user_tenant(user_id: str, tenant_id: str, role: str = "admin"):
    """
    Ensure that a user is assigned to a tenant.
    Creates the mapping if it does not exist.
    """
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO user_tenants (user_id, tenant_id, role)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, tenant_id) DO NOTHING;
        """, (user_id, tenant_id, role))
    logger.info(f"✅ User {user_id} assigned to tenant {tenant_id}")

def fetch_all_tenants():
    """
    Fetch all tenant IDs from the database.
    Assumes a 'tenants' table exists or tenant IDs are stored in companies.
    """
    conn = get_connection()
    cursor = conn.cursor()
    # Example: get distinct tenant_id from companies table
    cursor.execute("SELECT DISTINCT tenant_id FROM companies;")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [r[0] for r in rows]
# ---------------------------------------------------
# 🔐 Get User Tenant + Role (Supabase profiles)
# ---------------------------------------------------
def get_user_tenant(user_id: str) -> dict:
    """
    Fetch tenant_id and role for a Supabase user.

    Args:
        user_id (str): Supabase auth.users.id (UUID)

    Returns:
        dict: { tenant_id: str, role: str }
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT tenant_id, role
            FROM profiles
            WHERE id = %s
            """,
            (user_id,)
        )

        row = cur.fetchone()

        if not row:
            raise Exception(f"No profile found for user {user_id}")

        return {
            "tenant_id": str(row[0]),
            "role": row[1] or "analyst"
        }
# ---------------------------------------------------
# Scholar Reviews
# ---------------------------------------------------
def create_scholar_review(tenant_id, company_id, compliance_result_id, scholar_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO scholar_reviews (tenant_id, company_id, compliance_result_id, scholar_id)
        VALUES (%s,%s,%s,%s)
        RETURNING id
    """, (tenant_id, company_id, compliance_result_id, scholar_id))
    review_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return review_id

def update_scholar_review(review_id, status, comments):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE scholar_reviews
        SET status=%s, comments=%s, reviewed_at=NOW()
        WHERE id=%s
    """, (status, comments, review_id))
    conn.commit()
    cur.close()
    conn.close()

def fetch_scholar_reviews(company_id, tenant_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, company_id, compliance_result_id, scholar_id, status, comments, reviewed_at
        FROM scholar_reviews
        WHERE company_id=%s AND tenant_id=%s
    """, (company_id, tenant_id))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [
        {
            "id": r[0],
            "company_id": r[1],
            "compliance_result_id": r[2],
            "scholar_id": r[3],
            "status": r[4],
            "comments": r[5],
            "reviewed_at": r[6]
        } for r in rows
    ]
# ----------------------------
# Fatwa Table Access
# ----------------------------
def fetch_fatwa_by_id(fatwa_id: str, tenant_id: str):
    """
    Fetch a fatwa record for a tenant
    """
    with get_cursor() as cur:
        cur.execute("""
            SELECT fatwa_id, title, description
            FROM fatwas
            WHERE fatwa_id = %s AND tenant_id = %s
        """, (fatwa_id, tenant_id))
        row = cur.fetchone()
        if row:
            return {"fatwa_id": row[0], "title": row[1], "description": row[2]}
        return None

def create_scholar_review(tenant_id: str, company_id: str, compliance_result_id: int, scholar_id: str):
    """
    Create a new scholar review
    """
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO scholar_reviews (tenant_id, company_id, compliance_result_id, scholar_id, status)
            VALUES (%s, %s, %s, %s, 'pending') RETURNING review_id
        """, (tenant_id, company_id, compliance_result_id, scholar_id))
        row = cur.fetchone()
        return row[0] if row else None

def fetch_scholar_reviews(company_id: str, tenant_id: str):
    """
    Get all scholar reviews for a company
    """
    with get_cursor() as cur:
        cur.execute("""
            SELECT review_id, scholar_id, compliance_result_id, violation_code, status, created_at
            FROM scholar_reviews
            WHERE company_id = %s AND tenant_id = %s
        """, (company_id, tenant_id))
        rows = cur.fetchall()
        return [
            {
                "review_id": r[0],
                "scholar_id": r[1],
                "compliance_result_id": r[2],
                "violation_code": r[3],
                "status": r[4],
                "created_at": r[5]
            }
            for r in rows
        ]