"""Event Schemas for Operational Streams."""
from pydantic import BaseModel
from typing import Dict, Any, Optional

class OperationalEvent(BaseModel):
    event_id: str
    event_type: str
    source_system: str
    payload: Dict[str, Any]
    timestamp_ms: int
    is_deleted: bool = False
