# services/fatwa_registry.py

# ================================
# Fatwa Registry
# ================================

FATWA_REGISTRY = {
    "AAOIFI_STANDARD_V1": {
        "version": "1.0",
        "effective_date": "2024-01-01",
        "source": "AAOIFI Shariah Standard"
    }
}


def attach_fatwa_metadata(result: dict) -> dict:
    """
    Attach fatwa metadata for audit traceability.
    """
    result["fatwa_reference"] = FATWA_REGISTRY["AAOIFI_STANDARD_V1"]
    return result
def get_active_fatwa() -> dict:
    return FATWA_REGISTRY["AAOIFI_STANDARD_V1"]