"""
Processing ledger service for ETL data ingestion.

This module implements FR-14: Generate processing ledger with row count validation
and transfer summary. Tracks successful vs failed transfers and handles zero-count entities.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import json
from pathlib import Path

from ..database.upsert_operations import ProcessingLedger, UpsertResult

logger = logging.getLogger(__name__)


@dataclass
class EntitySummary:
    """Summary of processing for a single entity type."""
    entity_name: str
    expected_rows: int
    processed_rows: int
    inserted_rows: int
    updated_rows: int
    failed_rows: int
    success: bool
    error_message: Optional[str] = None
    processing_time_seconds: Optional[float] = None


@dataclass
class TransferSummary:
    """Overall transfer summary for the entire load."""
    load_id: str
    start_time: datetime
    end_time: Optional[datetime]
    total_expected_rows: int
    total_processed_rows: int
    total_inserted_rows: int
    total_updated_rows: int
    total_failed_rows: int
    success_rate: float
    overall_success: bool
    entity_summaries: List[EntitySummary]
    processing_duration_seconds: Optional[float] = None


class LedgerService:
    """
    Service for generating processing ledgers and transfer summaries.
    
    Implements FR-14: Generate processing ledger with row count validation
    and transfer summary. Handles zero-count entities from empty CSVs.
    """
    
    def __init__(self, output_directory: str = "/tmp/etl_ledgers"):
        """
        Initialize ledger service.
        
        Args:
            output_directory: Directory to store ledger files
        """
        self.output_directory = Path(output_directory)
        self.output_directory.mkdir(parents=True, exist_ok=True)
    
    def generate_processing_ledger(
        self, 
        load_id: str, 
        processing_ledger: ProcessingLedger,
        manifest_data: Dict[str, Any],
        staging_row_counts: Dict[str, int]
    ) -> TransferSummary:
        """
        Generate comprehensive processing ledger.
        
        Args:
            load_id: ETL load identifier
            processing_ledger: Processing ledger from upsert operations
            manifest_data: Manifest data with expected row counts
            staging_row_counts: Actual row counts from staging tables
            
        Returns:
            TransferSummary with complete processing details
        """
        logger.info(f"Generating processing ledger for load_id: {load_id}")
        
        # Calculate entity summaries
        entity_summaries = self._calculate_entity_summaries(
            processing_ledger, manifest_data, staging_row_counts
        )
        
        # Calculate overall summary
        total_expected = sum(manifest_data.get('files', []), key=lambda x: x.get('rows', 0))
        total_expected = sum([file_info.get('rows', 0) for file_info in manifest_data.get('files', [])])
        
        total_processed = sum([summary.processed_rows for summary in entity_summaries])
        total_inserted = sum([summary.inserted_rows for summary in entity_summaries])
        total_updated = sum([summary.updated_rows for summary in entity_summaries])
        total_failed = sum([summary.failed_rows for summary in entity_summaries])
        
        success_rate = (total_processed / total_expected * 100) if total_expected > 0 else 100.0
        
        # Calculate processing duration
        processing_duration = None
        if processing_ledger.end_time and processing_ledger.start_time:
            processing_duration = (processing_ledger.end_time - processing_ledger.start_time).total_seconds()
        
        transfer_summary = TransferSummary(
            load_id=load_id,
            start_time=processing_ledger.start_time,
            end_time=processing_ledger.end_time,
            total_expected_rows=total_expected,
            total_processed_rows=total_processed,
            total_inserted_rows=total_inserted,
            total_updated_rows=total_updated,
            total_failed_rows=total_failed,
            success_rate=success_rate,
            overall_success=processing_ledger.success,
            entity_summaries=entity_summaries,
            processing_duration_seconds=processing_duration
        )
        
        # Save ledger to file
        self._save_ledger_to_file(transfer_summary)
        
        # Log summary
        self._log_processing_summary(transfer_summary)
        
        return transfer_summary
    
    def _calculate_entity_summaries(
        self, 
        processing_ledger: ProcessingLedger,
        manifest_data: Dict[str, Any],
        staging_row_counts: Dict[str, int]
    ) -> List[EntitySummary]:
        """Calculate entity summaries from processing results."""
        entity_summaries = []
        
        # Map manifest file data to entity names
        manifest_files = {file_info['name']: file_info for file_info in manifest_data.get('files', [])}
        
        # Process each entity type
        for result in processing_ledger.results:
            entity_name = result.entity_name
            
            # Get expected rows from manifest
            expected_rows = 0
            if entity_name == 'customers' and 'customers.csv' in manifest_files:
                expected_rows = manifest_files['customers.csv'].get('rows', 0)
            elif entity_name == 'vehicles' and 'vehicles.csv' in manifest_files:
                expected_rows = manifest_files['vehicles.csv'].get('rows', 0)
            elif entity_name == 'invoices' and 'invoices.csv' in manifest_files:
                expected_rows = manifest_files['invoices.csv'].get('rows', 0)
            elif entity_name == 'line_items' and 'line_items.csv' in manifest_files:
                expected_rows = manifest_files['line_items.csv'].get('rows', 0)
            elif entity_name == 'payments' and 'payments.csv' in manifest_files:
                expected_rows = manifest_files['payments.csv'].get('rows', 0)
            elif entity_name == 'inventory_parts' and 'inventory_parts.csv' in manifest_files:
                expected_rows = manifest_files['inventory_parts.csv'].get('rows', 0)
            elif entity_name == 'suppliers' and 'suppliers.csv' in manifest_files:
                expected_rows = manifest_files['suppliers.csv'].get('rows', 0)
            
            # Get actual processed rows
            processed_rows = result.rows_processed
            inserted_rows = result.rows_inserted
            updated_rows = result.rows_updated
            failed_rows = processed_rows - inserted_rows - updated_rows
            
            # Calculate success rate for this entity
            success = result.success and failed_rows == 0
            
            entity_summary = EntitySummary(
                entity_name=entity_name,
                expected_rows=expected_rows,
                processed_rows=processed_rows,
                inserted_rows=inserted_rows,
                updated_rows=updated_rows,
                failed_rows=failed_rows,
                success=success,
                error_message=result.error_message
            )
            
            entity_summaries.append(entity_summary)
        
        return entity_summaries
    
    def _save_ledger_to_file(self, transfer_summary: TransferSummary):
        """Save ledger to JSON file."""
        try:
            # Convert to dictionary for JSON serialization
            ledger_data = asdict(transfer_summary)
            
            # Convert datetime objects to ISO format
            if ledger_data['start_time']:
                ledger_data['start_time'] = ledger_data['start_time'].isoformat()
            if ledger_data['end_time']:
                ledger_data['end_time'] = ledger_data['end_time'].isoformat()
            
            # Save to file
            filename = f"ledger_{transfer_summary.load_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            filepath = self.output_directory / filename
            
            with open(filepath, 'w') as f:
                json.dump(ledger_data, f, indent=2)
            
            logger.info(f"Ledger saved to: {filepath}")
            
        except Exception as e:
            logger.error(f"Failed to save ledger to file: {e}")
    
    def _log_processing_summary(self, transfer_summary: TransferSummary):
        """Log processing summary to console."""
        logger.info("=" * 80)
        logger.info(f"PROCESSING LEDGER - Load ID: {transfer_summary.load_id}")
        logger.info("=" * 80)
        
        logger.info(f"Start Time: {transfer_summary.start_time}")
        logger.info(f"End Time: {transfer_summary.end_time}")
        logger.info(f"Processing Duration: {transfer_summary.processing_duration_seconds:.2f} seconds")
        logger.info(f"Overall Success: {transfer_summary.overall_success}")
        logger.info(f"Success Rate: {transfer_summary.success_rate:.2f}%")
        
        logger.info("\nSUMMARY:")
        logger.info(f"Total Expected Rows: {transfer_summary.total_expected_rows}")
        logger.info(f"Total Processed Rows: {transfer_summary.total_processed_rows}")
        logger.info(f"Total Inserted Rows: {transfer_summary.total_inserted_rows}")
        logger.info(f"Total Updated Rows: {transfer_summary.total_updated_rows}")
        logger.info(f"Total Failed Rows: {transfer_summary.total_failed_rows}")
        
        logger.info("\nENTITY BREAKDOWN:")
        for summary in transfer_summary.entity_summaries:
            status = "✓ SUCCESS" if summary.success else "✗ FAILED"
            logger.info(f"  {summary.entity_name.upper()}: {status}")
            logger.info(f"    Expected: {summary.expected_rows}")
            logger.info(f"    Processed: {summary.processed_rows}")
            logger.info(f"    Inserted: {summary.inserted_rows}")
            logger.info(f"    Updated: {summary.updated_rows}")
            logger.info(f"    Failed: {summary.failed_rows}")
            if summary.error_message:
                logger.info(f"    Error: {summary.error_message}")
        
        logger.info("=" * 80)
    
    def generate_entity_ledger(self, entity_name: str, load_id: str, staging_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate detailed ledger for a specific entity type.
        
        Args:
            entity_name: Name of the entity type
            load_id: ETL load identifier
            staging_data: Staging data for the entity
            
        Returns:
            Dictionary with entity-specific ledger data
        """
        ledger_data = {
            'entity_name': entity_name,
            'load_id': load_id,
            'timestamp': datetime.now().isoformat(),
            'total_records': len(staging_data),
            'sample_records': staging_data[:5] if staging_data else [],  # First 5 records as sample
            'field_analysis': self._analyze_entity_fields(staging_data)
        }
        
        return ledger_data
    
    def _analyze_entity_fields(self, staging_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze field completeness and data quality."""
        if not staging_data:
            return {'total_fields': 0, 'completeness': {}}
        
        # Get all field names
        all_fields = set()
        for record in staging_data:
            all_fields.update(record.keys())
        
        # Calculate field completeness
        field_completeness = {}
        for field in all_fields:
            non_null_count = sum(1 for record in staging_data if record.get(field) is not None)
            completeness_percent = (non_null_count / len(staging_data)) * 100
            field_completeness[field] = {
                'non_null_count': non_null_count,
                'completeness_percent': completeness_percent
            }
        
        return {
            'total_fields': len(all_fields),
            'completeness': field_completeness
        }
    
    def validate_row_counts(
        self, 
        manifest_data: Dict[str, Any], 
        staging_row_counts: Dict[str, int],
        production_row_counts: Dict[str, int]
    ) -> Dict[str, bool]:
        """
        Validate row counts between manifest, staging, and production.
        
        Args:
            manifest_data: Manifest data with expected row counts
            staging_row_counts: Actual row counts from staging tables
            production_row_counts: Actual row counts from production tables
            
        Returns:
            Dict mapping entity names to validation status
        """
        validation_results = {}
        
        # Map manifest files to entity names
        manifest_files = {file_info['name']: file_info for file_info in manifest_data.get('files', [])}
        
        entity_mapping = {
            'customers': 'customers.csv',
            'vehicles': 'vehicles.csv', 
            'invoices': 'invoices.csv',
            'line_items': 'line_items.csv',
            'payments': 'payments.csv',
            'inventory_parts': 'inventory_parts.csv',
            'suppliers': 'suppliers.csv'
        }
        
        for entity_name, csv_filename in entity_mapping.items():
            try:
                # Get expected count from manifest
                expected_count = manifest_files.get(csv_filename, {}).get('rows', 0)
                
                # Get actual counts
                staging_count = staging_row_counts.get(entity_name, 0)
                production_count = production_row_counts.get(entity_name, 0)
                
                # Validate counts match
                staging_valid = staging_count == expected_count
                production_valid = production_count == expected_count
                
                validation_results[entity_name] = staging_valid and production_valid
                
                if not staging_valid:
                    logger.warning(f"Staging count mismatch for {entity_name}: expected={expected_count}, actual={staging_count}")
                
                if not production_valid:
                    logger.warning(f"Production count mismatch for {entity_name}: expected={expected_count}, actual={production_count}")
                
            except Exception as e:
                logger.error(f"Failed to validate row counts for {entity_name}: {e}")
                validation_results[entity_name] = False
        
        return validation_results
    
    def get_ledger_files(self, load_id: Optional[str] = None) -> List[Path]:
        """
        Get list of ledger files, optionally filtered by load_id.
        
        Args:
            load_id: Optional load_id to filter by
            
        Returns:
            List of ledger file paths
        """
        ledger_files = []
        
        for file_path in self.output_directory.glob("ledger_*.json"):
            if load_id and load_id not in file_path.name:
                continue
            ledger_files.append(file_path)
        
        return sorted(ledger_files, key=lambda x: x.stat().st_mtime, reverse=True)
    
    def load_ledger(self, file_path: Path) -> TransferSummary:
        """
        Load ledger from file.
        
        Args:
            file_path: Path to ledger file
            
        Returns:
            TransferSummary object
        """
        try:
            with open(file_path, 'r') as f:
                ledger_data = json.load(f)
            
            # Convert ISO format strings back to datetime objects
            if ledger_data.get('start_time'):
                ledger_data['start_time'] = datetime.fromisoformat(ledger_data['start_time'])
            if ledger_data.get('end_time'):
                ledger_data['end_time'] = datetime.fromisoformat(ledger_data['end_time'])
            
            return TransferSummary(**ledger_data)
            
        except Exception as e:
            logger.error(f"Failed to load ledger from {file_path}: {e}")
            raise
