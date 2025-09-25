# app/models/analysis.py
"""Pydantic models for analysis requests and responses."""

from typing import List, Dict, Optional, Any, Union
from pydantic import BaseModel, Field
from datetime import datetime

class AnalysisConfigRequest(BaseModel):
    """Configuration parameters for analysis."""
    materiality_vnd: Optional[float] = Field(None, description="Materiality threshold in VND")
    recurring_pct_threshold: Optional[float] = Field(None, description="Recurring percentage threshold")
    revenue_opex_pct_threshold: Optional[float] = Field(None, description="Revenue OPEX percentage threshold")
    bs_pct_threshold: Optional[float] = Field(None, description="Balance sheet percentage threshold")
    recurring_code_prefixes: Optional[str] = Field(None, description="Comma-separated recurring code prefixes")
    min_trend_periods: Optional[int] = Field(None, description="Minimum trend periods")
    gm_drop_threshold_pct: Optional[float] = Field(None, description="Gross margin drop threshold percentage")
    dep_pct_only_prefixes: Optional[str] = Field(None, description="Depreciation percentage only prefixes")
    customer_column_hints: Optional[str] = Field(None, description="Customer column hints")

class AnalysisSession(BaseModel):
    """Analysis session information."""
    session_id: str = Field(..., description="Unique session identifier")
    status: str = Field(..., description="Session status")
    created_at: datetime = Field(default_factory=datetime.now, description="Session creation time")

class AnalysisProgress(BaseModel):
    """Analysis progress information."""
    type: str = Field(..., description="Message type: log, progress, complete, error")
    message: str = Field(..., description="Progress message")
    percentage: Optional[int] = Field(None, description="Progress percentage (0-100)")

class RevenueVarianceChange(BaseModel):
    """Month-over-month revenue change."""
    period_from: str = Field(..., description="Source period")
    period_to: str = Field(..., description="Target period")
    previous_revenue: float = Field(..., description="Previous period revenue")
    current_revenue: float = Field(..., description="Current period revenue")
    absolute_change: float = Field(..., description="Absolute change in VND")
    percentage_change: float = Field(..., description="Percentage change")
    change_direction: str = Field(..., description="Direction: Increase/Decrease/No Change")

class EntityContribution(BaseModel):
    """Individual entity contribution to changes."""
    entity: str = Field(..., description="Entity/vendor/customer name")
    previous_value: float = Field(..., description="Previous period value")
    current_value: float = Field(..., description="Current period value")
    absolute_change: float = Field(..., description="Absolute change")
    percentage_change: float = Field(..., description="Percentage change")
    contribution_to_period_change: float = Field(..., description="Contribution to total period change (%)")

class NetEffectAnalysis(BaseModel):
    """Net effect analysis showing positive and negative contributors."""
    period_from: str = Field(..., description="Source period")
    period_to: str = Field(..., description="Target period")
    total_change: float = Field(..., description="Total period change")
    net_effect_explanation: str = Field(..., description="Human-readable net effect explanation")
    positive_contributors: List[EntityContribution] = Field(default_factory=list, description="Positive contributors")
    negative_contributors: List[EntityContribution] = Field(default_factory=list, description="Negative contributors")
    total_positive_change: float = Field(..., description="Sum of all positive changes")
    total_negative_change: float = Field(..., description="Sum of all negative changes")
    entities_with_significant_change: int = Field(..., description="Number of entities with significant changes")

class RevenueStreamAnalysis(BaseModel):
    """Revenue stream analysis for individual accounts."""
    account_name: str = Field(..., description="Revenue account name")
    total_entities: int = Field(..., description="Total entities in this stream")
    month_changes: List[RevenueVarianceChange] = Field(default_factory=list, description="Month-over-month changes")
    period_impacts: List[NetEffectAnalysis] = Field(default_factory=list, description="Net effect analysis by period")

class RevenueVarianceAnalysisResponse(BaseModel):
    """Complete revenue variance analysis response."""
    subsidiary: str = Field(..., description="Subsidiary name")
    filename: str = Field(..., description="Analyzed file name")
    months_analyzed: List[str] = Field(..., description="List of months analyzed")

    # Executive Summary
    analysis_summary: Dict[str, int] = Field(..., description="High-level analysis summary")
    key_insights: List[str] = Field(default_factory=list, description="Key insights from analysis")

    # Core Analysis Results
    total_revenue_analysis: Dict[str, Any] = Field(..., description="Total revenue month-over-month analysis")
    revenue_stream_analysis: Dict[str, Any] = Field(..., description="Individual revenue stream analysis")
    vendor_customer_impact: Dict[str, Any] = Field(..., description="Vendor/customer impact analysis")

    # Configuration
    configuration_used: Dict[str, Any] = Field(..., description="Configuration used for analysis")

class ErrorResponse(BaseModel):
    """Error response model."""
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")

class HealthResponse(BaseModel):
    """Health check response."""
    status: str = Field(..., description="Service status")
    version: str = Field(..., description="Application version")
    timestamp: datetime = Field(default_factory=datetime.now, description="Health check timestamp")

class FileInfo(BaseModel):
    """File information."""
    key: str = Field(..., description="File key")
    name: str = Field(..., description="Original filename")
    size: int = Field(..., description="File size in bytes")
    download_url: str = Field(..., description="Download URL")

class DebugFilesResponse(BaseModel):
    """Debug files list response."""
    session_id: str = Field(..., description="Session ID")
    files: List[FileInfo] = Field(..., description="List of available files")