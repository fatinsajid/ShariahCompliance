from sqlalchemy import Column, String, Integer, Float, TIMESTAMP, JSON, Text
from sqlalchemy.dialects.postgresql import UUID
from database import Base  # your declarative base

class ComplianceAuditLog(Base):
    __tablename__ = "compliance_audit_log"

    audit_id = Column(UUID(as_uuid=True), primary_key=True)
    tenant_id = Column(UUID(as_uuid=True), nullable=True)
    company_id = Column(Text, nullable=False)
    rule_code = Column(Text, nullable=True)
    fatwa_version = Column(Integer, nullable=True)
    compliance_status = Column(Text, nullable=True)
    triggered_by = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP, nullable=True)
    company_name = Column(Text, nullable=False)
    company_industry = Column(Text, nullable=True)
    audit_details = Column(JSON, nullable=True)
    violations_count = Column(Integer, nullable=True)
    risk_score = Column(Float, nullable=True)
    explanation = Column(Text, nullable=True)
    scholar_reviews = Column(JSON, nullable=True)
    anomaly_flag = Column(Text, nullable=True)
    total_assets = Column(Float, nullable=True)
    total_debt = Column(Float, nullable=True)
    total_income = Column(Float, nullable=True)
    non_halal_income = Column(Float, nullable=True)
    cash_and_interest_securities = Column(Float, nullable=True)