# api/app/schemas.py
from pydantic import BaseModel
from typing import Any, Optional

class GenericPayload(BaseModel):
    provider: Optional[str]
    room_id: Optional[str]
    channel: Optional[str]
    message: Optional[Any]
    sender: Optional[Any]
    timestamp: Optional[str]
