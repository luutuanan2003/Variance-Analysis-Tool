# app/core_new.py
"""
Backward compatibility module - re-exports all functions from the modular structure.
This ensures existing code continues to work after the refactoring.
"""

# Re-export everything from the modular structure
from .data_utils import *
from .excel_processing import *
from .anomaly_detection import *
from .revenue_analysis import *
from .main_orchestration import *
from .accounting_rules import *