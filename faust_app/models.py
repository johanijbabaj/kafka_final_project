# models.py
import faust
from typing import Optional


class BanRule(faust.Record, serializer='json'):
    field: str
    operator: str
    value: str
    currency: Optional[str]

