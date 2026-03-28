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

    weighted_score = sum(
        r["weight"] * (1 if r["decision"] == "APPROVED" else 0)
        for r in reviews
    )

    total_weight = sum(r["weight"] for r in reviews)

    ratio = weighted_score / total_weight

    return {
        "consensus_status": (
            "CONSENSUS" if ratio >= CONSENSUS_THRESHOLD else "DISAGREEMENT"
        ),
        "approval_ratio": round(ratio, 3),
        "disagreement_flag": ratio < CONSENSUS_THRESHOLD
    }