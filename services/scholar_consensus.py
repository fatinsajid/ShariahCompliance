# services/scholar_consensus.py

# ================================
# Scholar Consensus Engine
# ================================

CONSENSUS_THRESHOLD = 0.66


def compute_scholar_consensus(reviews: list) -> dict:
    """
    reviews example:
    [
        {"scholar_id": "S1", "decision": "APPROVED"},
        {"scholar_id": "S2", "decision": "REJECTED"},
    ]
    """

    if not reviews:
        return {
            "consensus_status": "NO_REVIEWS",
            "approval_ratio": 0.0,
            "disagreement_flag": False
        }

    approvals = sum(
        1 for r in reviews
        if r.get("decision") == "APPROVED"
    )
    total = len(reviews)

    ratio = approvals / total

    return {
        "consensus_status": (
            "CONSENSUS" if ratio >= CONSENSUS_THRESHOLD else "DISAGREEMENT"
        ),
        "approval_ratio": round(ratio, 3),
        "disagreement_flag": ratio < CONSENSUS_THRESHOLD
    }