# import psycopg2
# from config.db_config import DB_CONFIG
#
#
# def get_connection():
#     return psycopg2.connect(**DB_CONFIG)
# def fetch_company_data(company_id):
#     conn = get_connection()
#     cur = conn.cursor()
#
#     cur.execute("""
#         SELECT company_id, total_assets, total_debt, total_income,
#                non_halal_income, sector
#         FROM companies
#         WHERE company_id = %s
#     """, (company_id,))
#
#     row = cur.fetchone()
#     cur.close()
#     conn.close()
#
#     if not row:
#         raise Exception(f"Company {company_id} not found in database")
#
#     return {
#         "company_id": row[0],
#         "total_assets": float(row[1]),
#         "total_debt": float(row[2]),
#         "total_income": float(row[3]),
#         "non_halal_income": float(row[4]),
#         "sector": row[5]
#     }
# import psycopg2
# from config.db_config import DB_CONFIG
#
#
# def get_connection():
#     return psycopg2.connect(
#         host=DB_CONFIG["host"],
#         database=DB_CONFIG["database"],
#         user=DB_CONFIG["user"],
#         password=DB_CONFIG["password"],
#         port=DB_CONFIG["port"]
#     )
#
#
# def upsert_company(company):
#     conn = get_connection()
#     cur = conn.cursor()
#
#     cur.execute("""
#         INSERT INTO companies (
#             company_id, total_assets, total_debt,
#             total_income, non_halal_income, sector
#         )
#         VALUES (%s, %s, %s, %s, %s, %s)
#         ON CONFLICT (company_id)
#         DO UPDATE SET
#             total_assets = EXCLUDED.total_assets,
#             total_debt = EXCLUDED.total_debt,
#             total_income = EXCLUDED.total_income,
#             non_halal_income = EXCLUDED.non_halal_income,
#             sector = EXCLUDED.sector
#     """, (
#         company["company_id"],
#         company["total_assets"],
#         company["total_debt"],
#         company["total_income"],
#         company["non_halal_income"],
#         company["sector"]
#     ))
#
#     conn.commit()
#     cur.close()
#     conn.close()
#
#
# def fetch_company_data(company_id):
#     conn = get_connection()
#     cur = conn.cursor()
#
#     cur.execute("""
#         SELECT company_id, total_assets, total_debt,
#                total_income, non_halal_income, sector
#         FROM companies
#         WHERE company_id = %s
#     """, (company_id,))
#
#     row = cur.fetchone()
#     cur.close()
#     conn.close()
#
#     if not row:
#         raise Exception(f"Company {company_id} not found")
#
#     return {
#         "company_id": row[0],
#         "total_assets": float(row[1]),
#         "total_debt": float(row[2]),
#         "total_income": float(row[3]),
#         "non_halal_income": float(row[4]),
#         "sector": row[5]
#     }
#
#
# def save_compliance_results(result):
#     conn = get_connection()
#     cur = conn.cursor()
#
#     cur.execute("""
#         INSERT INTO compliance_results (
#             company_id,
#             compliance_status,
#             violation_details
#         )
#         VALUES (%s, %s, %s)
#     """, (
#         result["company_id"],
#         result["compliance_status"],
#         result.get("violations", "")
#     ))
#
#     conn.commit()
#     cur.close()
#     conn.close()
#
#
# # -----------------------------
# # INSERT / UPDATE COMPANY DATA
# # -----------------------------
# def upsert_company(company):
#     """
#     company = {
#         "company_id": "C005",
#         "total_assets": 60000000,
#         "total_debt": 15000000,
#         "total_income": 9000000,
#         "non_halal_income": 100000,
#         "sector": "Textiles"
#     }
#     """
#     conn = get_connection()
#     cur = conn.cursor()
#
#     cur.execute("""
#         INSERT INTO companies
#         (company_id, total_assets, total_debt, total_income, non_halal_income, sector)
#         VALUES (%s, %s, %s, %s, %s, %s)
#         ON CONFLICT (company_id)
#         DO UPDATE SET
#             total_assets = EXCLUDED.total_assets,
#             total_debt = EXCLUDED.total_debt,
#             total_income = EXCLUDED.total_income,
#             non_halal_income = EXCLUDED.non_halal_income,
#             sector = EXCLUDED.sector;
#     """, (
#         company["company_id"],
#         company["total_assets"],
#         company["total_debt"],
#         company["total_income"],
#         company["non_halal_income"],
#         company["sector"]
#     ))
#
#     conn.commit()
#     cur.close()
#     conn.close()
#
#
# # -----------------------------
# # FETCH COMPANY DATA
# # -----------------------------
# def fetch_companies():
#     conn = get_connection()
#     cur = conn.cursor()
#
#     cur.execute("""
#         SELECT company_id, total_assets, total_debt,
#                total_income, non_halal_income, sector
#         FROM companies
#     """)
#
#     rows = cur.fetchall()
#     cur.close()
#     conn.close()
#
#     companies = []
#     for r in rows:
#         companies.append({
#             "company_id": r[0],
#             "total_assets": float(r[1]),
#             "total_debt": float(r[2]),
#             "total_income": float(r[3]),
#             "non_halal_income": float(r[4]),
#             "sector": r[5]
#         })
#
#     return companies
#
#
# # -----------------------------
# # SAVE COMPLIANCE RESULTS
# # -----------------------------
# def save_results(results):
#     conn = get_connection()
#     cur = conn.cursor()
#
#     for r in results:
#         cur.execute("""
#             INSERT INTO compliance_results
#             (company_id, compliance_status, violation_details)
#             VALUES (%s, %s, %s)
#         """, (
#             r["company_id"],
#             r["status"],
#             ", ".join(r["violations"]) if r["violations"] else "None"
#         ))
#
#
#
#     conn.commit()
#     cur.close()
#     conn.close()
import psycopg2
from config.db_config import DB_CONFIG


# # ---------------------------------------------------
# # 1️⃣ Database Connection
# # ---------------------------------------------------
#
# def get_connection():
#     return psycopg2.connect(
#         dbname=DB_CONFIG['dbname'],
#         user=DB_CONFIG['user'],
#         password=DB_CONFIG['password'],
#         host=DB_CONFIG['host'],
#         port=DB_CONFIG['port']
#     )
#
#
# # ---------------------------------------------------
# # 2️⃣ Initialize Tables (Auto Schema Safe)
# # ---------------------------------------------------
#
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

