import json
import os

from dal.db_connector import save_company, save_result, initialize_tables
from services.anomaly_detector import detect_anomalies
from services.compliance_engine import check_shariah_compliance
from services.risk_engine import calculate_risk_score
from fastapi import FastAPI
from models import train_model

app = FastAPI()

@app.get("/predict")
def predict():
    # load model & return dummy example
    return {"status": "success", "message": "Prediction endpoint ready!"}



def load_thresholds():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(BASE_DIR, "config", "shariah_thresholds.json")
    with open(path, "r") as f:
        return json.load(f)


def evaluate_single_company(company):
    try:
        thresholds = load_thresholds()

        status, violations = check_shariah_compliance(company, thresholds)

        save_company(company)
        save_result(company["company_id"], status, violations)
        status, violations = check_shariah_compliance(company, thresholds)

        risk_result = calculate_risk_score(company)
        anomaly_result = detect_anomalies(company)

        return {
            "status": "SUCCESS",
            "result": {
                "company_id": company["company_id"],
                "compliance_status": status,
                "violations": violations,
                "risk_analysis": risk_result,
                "anomaly_detection": anomaly_result
            }
        }


    except Exception as e:
        return {"status": "ERROR", "message": str(e)}


if __name__ == "__main__":
    initialize_tables()
    print("Shariah Compliance Engine Ready.")
