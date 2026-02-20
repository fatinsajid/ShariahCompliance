import os
import logging
from dotenv import load_dotenv
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager
from supabase import create_client

load_dotenv()
# ---------------------------------------------------
# 🔧 Logging Setup
# ---------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------
# 🔐 Environment Variables (Render-friendly)
# ---------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not set")

# ---------------------------------------------------
# 🚀 Connection Pool (VERY IMPORTANT FOR CLOUD)
# ---------------------------------------------------
try:
    pool = SimpleConnectionPool(
        minconn=1,
        maxconn=10,
        dsn=DATABASE_URL
    )
    logger.info("✅ Database connection pool created")
except Exception as e:
    logger.exception("❌ Failed to create DB pool")
    raise

# ---------------------------------------------------
# 🧠 Context Manager for Safe Connections
# ---------------------------------------------------
@contextmanager
def get_cursor():
    conn = None
    try:
        conn = pool.getconn()
        cursor = conn.cursor()
        yield cursor
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logger.exception("❌ Database operation failed")
        raise
    finally:
        if conn:
            pool.putconn(conn)

# ---------------------------------------------------
# 🏗️ Initialize Tables
# ---------------------------------------------------
def initialize_tables():
    with get_cursor() as cursor:

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                company_id VARCHAR PRIMARY KEY,
                total_assets FLOAT,
                total_debt FLOAT,
                total_income FLOAT,
                non_halal_income FLOAT,
                cash_and_interest_securities FLOAT,
                sector VARCHAR
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS compliance_results (
                id SERIAL PRIMARY KEY,
                company_id VARCHAR,
                compliance_status VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                violations TEXT
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS companies_features (
                company_id VARCHAR PRIMARY KEY,
                debt_ratio FLOAT,
                liquidity_ratio FLOAT,
                non_halal_income_ratio FLOAT,
                other_financial_metric1 FLOAT,
                other_financial_metric2 FLOAT
            );
        """)

    logger.info("✅ Tables initialized")

# ---------------------------------------------------
# 💾 Save Company
# ---------------------------------------------------
def save_company(company: dict):
    with get_cursor() as cursor:
        cursor.execute("""
            INSERT INTO companies (
                company_id,
                total_assets,
                total_debt,
                total_income,
                non_halal_income,
                cash_and_interest_securities,
                sector
            ) VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (company_id)
            DO UPDATE SET
                total_assets = EXCLUDED.total_assets,
                total_debt = EXCLUDED.total_debt,
                total_income = EXCLUDED.total_income,
                non_halal_income = EXCLUDED.non_halal_income,
                cash_and_interest_securities = EXCLUDED.cash_and_interest_securities,
                sector = EXCLUDED.sector;
        """, (
            company["company_id"],
            company["total_assets"],
            company["total_debt"],
            company["total_income"],
            company["non_halal_income"],
            company["cash_and_interest_securities"],
            company["sector"]
        ))

# ---------------------------------------------------
# 📊 Save Compliance Result
# ---------------------------------------------------
def save_result(company_id: str, status: str, violations: list):
    with get_cursor() as cursor:
        cursor.execute("""
            INSERT INTO compliance_results (
                company_id,
                compliance_status,
                violations
            ) VALUES (%s, %s, %s);
        """, (
            company_id,
            status,
            ", ".join(violations) if violations else "None"
        ))

# ---------------------------------------------------
# 📥 Fetch Companies
# ---------------------------------------------------
def fetch_companies():
    with get_cursor() as cursor:
        cursor.execute("""
            SELECT company_id, total_assets, total_debt, total_income,
                   non_halal_income, cash_and_interest_securities, sector
            FROM companies
        """)
        rows = cursor.fetchall()

    return [
        {
            "company_id": r[0],
            "total_assets": float(r[1] or 0.0),
            "total_debt": float(r[2] or 0.0),
            "total_income": float(r[3] or 0.0),
            "non_halal_income": float(r[4] or 0.0),
            "cash_and_interest_securities": float(r[5] or 0.0),
            "sector": r[6] or "Unknown",
        }
        for r in rows
    ]

# ---------------------------------------------------
# 🤖 Populate ML Features
# ---------------------------------------------------
def populate_features():
    companies = fetch_companies()
    if not companies:
        logger.warning("⚠️ No companies found")
        return

    with get_cursor() as cursor:
        for c in companies:
            debt_ratio = c["total_debt"] / (c["total_assets"] + 1)
            liquidity_ratio = c["cash_and_interest_securities"] / (c["total_assets"] + 1)
            non_halal_income_ratio = c["non_halal_income"] / (c["total_income"] + 1)
            other1 = c["total_assets"] / (c["total_debt"] + 1)
            other2 = c["total_income"] / (c["total_assets"] + 1)

            cursor.execute("""
                INSERT INTO companies_features (
                    company_id, debt_ratio, liquidity_ratio,
                    non_halal_income_ratio, other_financial_metric1, other_financial_metric2
                ) VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (company_id) DO UPDATE SET
                    debt_ratio = EXCLUDED.debt_ratio,
                    liquidity_ratio = EXCLUDED.liquidity_ratio,
                    non_halal_income_ratio = EXCLUDED.non_halal_income_ratio,
                    other_financial_metric1 = EXCLUDED.other_financial_metric1,
                    other_financial_metric2 = EXCLUDED.other_financial_metric2;
            """, (
                c["company_id"],
                debt_ratio,
                liquidity_ratio,
                non_halal_income_ratio,
                other1,
                other2
            ))

    logger.info("✅ ML features populated")

# ---------------------------------------------------
# ☁️ Push Features to Supabase
# ---------------------------------------------------
def push_features_to_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.warning("⚠️ Supabase credentials missing")
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    features = fetch_features()

    if not features:
        logger.warning("⚠️ No features to push")
        return

    for f in features:
        try:
            supabase.table("companies_features").upsert(f).execute()
        except Exception:
            logger.exception(f"❌ Failed pushing {f['company_id']}")

    logger.info("✅ Features pushed to Supabase")

# ---------------------------------------------------
# 📥 Fetch ML Features
# ---------------------------------------------------
def fetch_features():
    with get_cursor() as cursor:
        cursor.execute("""
            SELECT company_id, debt_ratio, liquidity_ratio,
                   non_halal_income_ratio, other_financial_metric1, other_financial_metric2
            FROM companies_features
        """)
        rows = cursor.fetchall()

    return [
        {
            "company_id": r[0],
            "debt_ratio": float(r[1] or 0.0),
            "liquidity_ratio": float(r[2] or 0.0),
            "non_halal_income_ratio": float(r[3] or 0.0),
            "other_financial_metric1": float(r[4] or 0.0),
            "other_financial_metric2": float(r[5] or 0.0),
        }
        for r in rows
    ]
