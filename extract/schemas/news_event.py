from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class NewsSource(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None

class NewsEvent(BaseModel):
    event_id: str = Field(..., description="Unique ID for the event")
    ts: datetime = Field(..., description="Ingestion timestamp (UTC)")
    source: Optional[NewsSource] = None
    author: Optional[str] = None
    title: str = Field(..., description="News headline or title")
    description: Optional[str] = None
    url: Optional[str] = None
    published_at: Optional[datetime] = None
    language: Optional[str] = "en"
    region: Optional[str] = None
