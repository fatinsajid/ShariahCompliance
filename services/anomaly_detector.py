import numpy as np

def detect_anomalies(company: dict) -> dict:
    """
    Simple anomaly detection using threshold deviation.
    """

    debt_ratio = company["total_debt"] / company["total_assets"]

    # Example anomaly rule
    if debt_ratio > 0.60:
        return {
            "anomaly_detected": True,
            "reason": "Debt ratio extremely high"
        }

    return {
        "anomaly_detected": False,
        "reason": None
    }
