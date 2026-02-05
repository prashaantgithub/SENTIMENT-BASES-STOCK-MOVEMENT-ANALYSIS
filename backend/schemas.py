from pydantic import BaseModel
from typing import List, Optional

class StockPrediction(BaseModel):
    stock: str
    current_price: float
    prediction: str
    confidence: float
    sentiment_score: float
    timestamp: str

class NewsItem(BaseModel):
    stock: str
    title: str
    description: Optional[str] = None
    source: str
    published_at: str
    sentiment_score: float

class StockHistory(BaseModel):
    date: str
    close: float
    volume: int