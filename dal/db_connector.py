import os
import json
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager
from typing import List, Dict, Iterable, Optional
from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------
# 🔧 Environment
# ---------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

# ---------------------------------------------------
# 🚀 Connection Pool (CRITICAL for Render)
# ---------------------------------------------------
POOL = SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dsn=DATABASE_URL
)

# ---------------------------------------------------
# 🧠 Context Manager
# ---------------------------------------------------
@contextmanager
def get_cursor():
    conn = POOL.getconn()
    try:
        cursor = conn.cursor()
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        POOL.putconn(conn)

# ---------------------------------------------------
# 🏗️ Initialize Tables (Multi-Tenant)
# ---------------------------------------------------
def initialize_tables():
    with get_cursor() as cur:

        # Tenants
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tenants (
                tenant_id UUID PRIMARY KEY,
                name VARCHAR NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # User ↔ Tenant mapping
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

        # Compliance results
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

        # ML features
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

# ---------------------------------------------------
# 🔐 Tenant Lookup
# ---------------------------------------------------
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
        raise ValueError("User not assigned to tenant")

    return row[0]

# ---------------------------------------------------
# 💾 Save Company (Upsert, Multi-Tenant)
# ---------------------------------------------------
def save_company(company: Dict, tenant_id: str):
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO companies (
                company_id,
                tenant_id,
                total_assets,
                total_debt,
                total_income,
                non_halal_income,
                cash_and_interest_securities,
                sector
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
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

# ---------------------------------------------------
# 🚀 STREAMING INGESTION (HIGH VALUE)
# ---------------------------------------------------
def stream_companies(
    companies: Iterable[Dict],
    tenant_id: str,
    batch_size: int = 500
):
    """
    High-throughput ingestion for Kafka / files / streams.
    """

    buffer = []

    for company in companies:
        buffer.append(company)

        if len(buffer) >= batch_size:
            _bulk_upsert(buffer, tenant_id)
            buffer.clear()

    if buffer:
        _bulk_upsert(buffer, tenant_id)


def _bulk_upsert(companies: List[Dict], tenant_id: str):
    with get_cursor() as cur:
        args = [
            (
                c["company_id"],
                tenant_id,
                c.get("total_assets", 0),
                c.get("total_debt", 0),
                c.get("total_income", 0),
                c.get("non_halal_income", 0),
                c.get("cash_and_interest_securities", 0),
                c.get("sector", "Unknown"),
            )
            for c in companies
        ]

        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO companies (
                company_id,
                tenant_id,
                total_assets,
                total_debt,
                total_income,
                non_halal_income,
                cash_and_interest_securities,
                sector
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (company_id, tenant_id)
            DO UPDATE SET
                total_assets = EXCLUDED.total_assets,
                total_debt = EXCLUDED.total_debt,
                total_income = EXCLUDED.total_income,
                non_halal_income = EXCLUDED.non_halal_income,
                cash_and_interest_securities = EXCLUDED.cash_and_interest_securities,
                sector = EXCLUDED.sector;
            """,
            args,
            page_size=500
        )

# ---------------------------------------------------
# 🔍 Fetch Companies (Tenant-Scoped)
# ---------------------------------------------------
def fetch_companies(tenant_id: str) -> List[Dict]:
    with get_cursor() as cur:
        cur.execute("""
            SELECT company_id, total_assets, total_debt, total_income,
                   non_halal_income, cash_and_interest_securities, sector
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
            "sector": r[6] or "Unknown",
        }
        for r in rows
    ]

# ---------------------------------------------------
# 📤 Optional Supabase Sync
# ---------------------------------------------------
def push_features_to_supabase(features: List[Dict]):
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("⚠️ Supabase not configured")
        return

    from supabase import create_client

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    for f in features:
        supabase.table("companies_features").upsert(f).execute()
