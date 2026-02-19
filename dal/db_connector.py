import psycopg2
from config.db_config import DB_CONFIG

# ---------------------------------------------------
# 1️⃣ Get Connection
# ---------------------------------------------------
def get_connection():
    return psycopg2.connect(DB_CONFIG["database_url"])

# ---------------------------------------------------
# 2️⃣ Initialize Tables
# ---------------------------------------------------
def initialize_tables():
    conn = get_connection()
    cursor = conn.cursor()

    # Raw company data
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

    # Compliance results
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS compliance_results (
            id SERIAL PRIMARY KEY,
            company_id VARCHAR,
            compliance_status VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            violations TEXT
        );
    """)

    # ML-ready features
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

    conn.commit()
    cursor.close()
    conn.close()

# ---------------------------------------------------
# 3️⃣ Save Company
# ---------------------------------------------------
def save_company(company):
    conn = get_connection()
    cursor = conn.cursor()

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

    conn.commit()
    cursor.close()
    conn.close()

# ---------------------------------------------------
# 4️⃣ Save Compliance Result
# ---------------------------------------------------
def save_result(company_id, status, violations):
    conn = get_connection()
    cursor = conn.cursor()

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

    conn.commit()
    cursor.close()
    conn.close()

# ---------------------------------------------------
# 5️⃣ Fetch All Companies
# ---------------------------------------------------
def fetch_companies():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT company_id, total_assets, total_debt, total_income,
               non_halal_income, cash_and_interest_securities, sector
        FROM companies
    """)

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    companies = []
    for r in rows:
        companies.append({
            "company_id": r[0],
            "total_assets": float(r[1] or 0.0),
            "total_debt": float(r[2] or 0.0),
            "total_income": float(r[3] or 0.0),
            "non_halal_income": float(r[4] or 0.0),
            "cash_and_interest_securities": float(r[5] or 0.0),
            "sector": r[6] or "Unknown"
        })
    return companies

# ---------------------------------------------------
# 6️⃣ Populate ML-ready features
# ---------------------------------------------------
def populate_features():
    companies = fetch_companies()
    if not companies:
        print("⚠️ No companies found to generate features.")
        return

    conn = get_connection()
    cursor = conn.cursor()

    for c in companies:
        debt_ratio = c["total_debt"] / (c["total_assets"] + 1)
        liquidity_ratio = c["cash_and_interest_securities"] / (c["total_assets"] + 1)
        non_halal_income_ratio = c["non_halal_income"] / (c["total_income"] + 1)
        other_financial_metric1 = c["total_assets"] / (c["total_debt"] + 1)
        other_financial_metric2 = c["total_income"] / (c["total_assets"] + 1)

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
            other_financial_metric1,
            other_financial_metric2
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ ML-ready features table populated.")

# ---------------------------------------------------
# 7️⃣ Fetch ML Features
# ---------------------------------------------------
def fetch_features():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT company_id, debt_ratio, liquidity_ratio,
               non_halal_income_ratio, other_financial_metric1, other_financial_metric2
        FROM companies_features
    """)

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    features = []
    for r in rows:
        features.append({
            "company_id": r[0],
            "debt_ratio": float(r[1] or 0.0),
            "liquidity_ratio": float(r[2] or 0.0),
            "non_halal_income_ratio": float(r[3] or 0.0),
            "other_financial_metric1": float(r[4] or 0.0),
            "other_financial_metric2": float(r[5] or 0.0)
        })
    return features


# ---------------------------------------------------
# 8️⃣ Push ML Features to Supabase
# ---------------------------------------------------
from supabase import create_client, Client
from config.db_config import DB_CONFIG


def push_features_to_supabase():
    from supabase import create_client
    SUPABASE_URL = DB_CONFIG["url"]
    SUPABASE_KEY = DB_CONFIG["key"]

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("⚠️ Supabase credentials not set. Skipping push.")
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    features = fetch_features()
    if not features:
        print("⚠️ No ML features to push to Supabase.")
        return

    for f in features:
        res = supabase.table("companies_features").upsert(f).execute()
        if res.error:
            print(f"⚠️ Failed to push {f['company_id']}: {res.error}")

    print("✅ ML features pushed to Supabase.")

