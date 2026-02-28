from datetime import datetime
from dal.db_connector import insert_audit_log


def log_compliance_decision(
    tenant_id: str,
    company_id: str,
    status: str,
    violations: list,
    fatwa_id: str,
):
    """
    Persist an auditable record of a compliance decision.
    Thesis-critical for governance traceability.
    """

    record = {
        "tenant_id": tenant_id,
        "company_id": company_id,
        "status": status,
        "violations": violations,
        "fatwa_id": fatwa_id,
        "timestamp": datetime.utcnow().isoformat(),
    }

    try:
        insert_audit_log(record)
    except Exception as e:
        print(f"[AUDIT] Failed to log compliance decision: {e}")