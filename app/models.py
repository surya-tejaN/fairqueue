from datetime import datetime
from typing import Any, Optional
from pydantic import BaseModel, Field


class EnqueueRequest(BaseModel):
    payload: dict[str, Any]
    max_attempts: int = Field(default=3, ge=1, le=10)


class Job(BaseModel):
    id: str
    payload: dict[str, Any]
    status: str
    attempts: int
    max_attempts: int
    visible_at: datetime
    created_at: datetime
    result: Optional[dict[str, Any]] = None
