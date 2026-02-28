import os
import joblib
import pandas as pd
from dotenv import load_dotenv
from sklearn.ensemble import IsolationForest
from supabase import create_client, Client

# ----------------------------
# 1️⃣ Load environment
# ----------------------------
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
MODEL_PATH = os.path.join(os.path.dirname(__file__), "anomaly_model_v1.pkl")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase URL or Key not set in .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ----------------------------
# 2️⃣ Fetch company financial data
# ----------------------------
def fetch_company_financials(tenant_id: str):
    response = (
        supabase.table("companies")
        .select("company_id, total_assets, total_debt, total_income, non_halal_income, cash_and_interest_securities")
        .eq("tenant_id", tenant_id)
        .execute()
    )

    data = response.data
    if not data:
        print("⚠️ No records returned by Supabase query")
        return pd.DataFrame()  # empty

    df = pd.DataFrame(data)
    # normalize column names to lowercase and strip spaces
    df.columns = [c.strip().lower() for c in df.columns]
    print("Columns fetched:", df.columns.tolist())
    print("First 3 rows:\n", df.head(3))
    return df

# ----------------------------
# 3️⃣ Prepare features
# ----------------------------
def prepare_features(df: pd.DataFrame):
    features = [
        "total_assets",
        "total_debt",
        "total_income",
        "non_halal_income",
        "cash_and_interest_securities",
    ]

    # Ensure all features exist
    for f in features:
        if f not in df.columns:
            df[f] = 0

    X = df[features].fillna(0)
    return X

# ----------------------------
# 4️⃣ Train anomaly detection model
# ----------------------------
def train_anomaly_model(X: pd.DataFrame):
    model = IsolationForest(
        n_estimators=100,
        contamination=0.05,
        random_state=42
    )
    model.fit(X)
    return model

# ----------------------------
# 5️⃣ Save model
# ----------------------------
def save_model(model):
    joblib.dump(model, MODEL_PATH)
    print(f"✅ Model saved to {MODEL_PATH}")

# ----------------------------
# 6️⃣ Main
# ----------------------------
def main():
    print("🚀 Starting anomaly model training...")
    tenant_id = "378e99af-d0b0-4495-8fd6-53203da2c12e"
    df = fetch_company_financials(tenant_id)

    if df.empty:
        print("⚠️ No company financial data found in Supabase. Skipping training.")
        return

    X = prepare_features(df)

    if X.empty:
        print("⚠️ Feature matrix is empty. Cannot train model.")
        return

    model = train_anomaly_model(X)
    save_model(model)
    print("✅ Anomaly model training completed.")

if __name__ == "__main__":
    main()