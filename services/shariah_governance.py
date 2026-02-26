from dal.db_connector import get_cursor


def get_active_fatwa(rule_code: str, tenant_id: str):
    """
    Fetch latest active fatwa version.
    """
    with get_cursor() as cur:
        cur.execute("""
            SELECT fatwa_id, version, ruling
            FROM fatwas
            WHERE rule_code = %s
              AND tenant_id = %s
              AND status = 'active'
            ORDER BY version DESC
            LIMIT 1
        """, (rule_code, tenant_id))

        row = cur.fetchone()
        return row


def log_compliance_decision(
    tenant_id: str,
    company_id: str,
    rule_code: str,
    fatwa_version: int,
    status: str,
):
    """
    Write audit trail.
    """
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO compliance_audit_log (
                tenant_id,
                company_id,
                rule_code,
                fatwa_version,
                compliance_status,
                triggered_by
            )
            VALUES (%s, %s, %s, %s, %s, 'system')
        """, (
            tenant_id,
            company_id,
            rule_code,
            fatwa_version,
            status,
        ))
def fatwa_is_approved(fatwa_id: str) -> bool:
    """
    Minimal majority approval logic.
    Thesis-grade but lightweight.
    """
    with get_cursor() as cur:
        cur.execute("""
            SELECT
                SUM(CASE WHEN decision='approve' THEN 1 ELSE 0 END) AS approves,
                COUNT(*) AS total
            FROM scholar_reviews
            WHERE fatwa_id = %s
        """, (fatwa_id,))

        row = cur.fetchone()

        if not row or row[1] == 0:
            return False

        approves, total = row
        return approves > total / 2