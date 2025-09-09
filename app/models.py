from pydantic import BaseModel, Field
from typing import List, Optional

class ConfigOverrides(BaseModel):
    base_dir: str = Field(default=".")
    materiality_vnd: Optional[int] = None
    recurring_pct_threshold: Optional[float] = None
    revenue_opex_pct_threshold: Optional[float] = None
    bs_pct_threshold: Optional[float] = None
    archive_processed: Optional[bool] = None
    recurring_code_prefixes: Optional[List[str]] = None
    min_trend_periods: Optional[int] = None

class ProcessResult(BaseModel):
    message: str
    total_anomalies: int
    per_subsidiary: dict
    generated_files: List[str]
