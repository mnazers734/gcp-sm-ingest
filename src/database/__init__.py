"""
Database module for ETL data ingestion.

This module provides database operations including staging table management
and production database upsert operations.
"""

from .staging_tables import StagingTableManager, StagingTableConfig
from .upsert_operations import (
    ProductionUpsertManager, 
    UpsertResult, 
    ProcessingLedger
)

__all__ = [
    'StagingTableManager',
    'StagingTableConfig', 
    'ProductionUpsertManager',
    'UpsertResult',
    'ProcessingLedger'
]
