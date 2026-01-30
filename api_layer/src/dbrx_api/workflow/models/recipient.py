"""
Recipient Model

Database model for Delta Share recipients (D2D and D2O).
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from uuid import UUID
from datetime import datetime


class Recipient(BaseModel):
    """Recipient database model."""

    record_id: UUID
    recipient_id: UUID
    share_pack_id: UUID
    recipient_name: str
    databricks_recipient_id: str  # ID from Databricks SDK
    recipient_contact_email: str
    recipient_type: str  # D2D or D2O
    recipient_databricks_org: Optional[str] = None  # For D2D only
    ip_access_list: List[str] = Field(default_factory=list)  # From JSONB, for D2O only
    token_expiry_days: int = 30
    token_rotation_enabled: bool = False
    activation_url: Optional[str] = None  # For D2O only
    bearer_token: Optional[str] = None  # For D2O only (encrypted)

    # SCD2 columns
    is_deleted: bool = False
    effective_from: datetime
    effective_to: datetime
    is_current: bool
    version: int
    created_by: str
    change_reason: str = ""

    class Config:
        from_attributes = True
