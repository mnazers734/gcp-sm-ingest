"""
Exception report generator for ETL data ingestion.

This module implements FR-15: Create exception reports by file type for failed data transfers.
Details specific validation failures and tracks data elements that failed to transfer.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
import json
from pathlib import Path
import csv

from ..database.upsert_operations import UpsertResult

logger = logging.getLogger(__name__)


@dataclass
class ValidationError:
    """Individual validation error details."""
    row_number: int
    field_name: str
    field_value: Any
    error_type: str
    error_message: str
    severity: str = "ERROR"  # ERROR, WARNING, INFO


@dataclass
class EntityExceptionReport:
    """Exception report for a single entity type."""
    entity_name: str
    load_id: str
    total_records: int
    failed_records: int
    success_rate: float
    validation_errors: List[ValidationError]
    processing_errors: List[str]
    timestamp: datetime
    recommendations: List[str]


@dataclass
class ExceptionSummary:
    """Overall exception summary for the entire load."""
    load_id: str
    total_entities: int
    failed_entities: int
    overall_success_rate: float
    entity_reports: List[EntityExceptionReport]
    critical_errors: List[str]
    timestamp: datetime


class ExceptionReporter:
    """
    Service for generating exception reports and tracking validation failures.
    
    Implements FR-15: Create exception reports by file type for failed data transfers.
    Details specific validation failures and tracks data elements that failed to transfer.
    """
    
    def __init__(self, output_directory: str = "/tmp/etl_exceptions"):
        """
        Initialize exception reporter.
        
        Args:
            output_directory: Directory to store exception reports
        """
        self.output_directory = Path(output_directory)
        self.output_directory.mkdir(parents=True, exist_ok=True)
        
        # Define critical fields for each entity type
        self.critical_fields = {
            'customers': ['external_customer_id', 'external_shop_id', 'first_name', 'last_name'],
            'vehicles': ['external_vehicle_id', 'external_shop_id', 'external_customer_id', 'year', 'make', 'model'],
            'invoices': ['external_document_id', 'external_shop_id', 'external_customer_id', 'external_vehicle_id'],
            'line_items': ['external_dataline_id', 'external_shop_id', 'external_document_id'],
            'payments': ['external_payment_id', 'external_shop_id', 'external_document_id'],
            'inventory_parts': ['external_part_id', 'external_shop_id', 'part_number'],
            'suppliers': ['external_supplier_id', 'external_shop_id', 'supplier_name']
        }
    
    def generate_exception_reports(
        self, 
        load_id: str, 
        upsert_results: List[UpsertResult],
        validation_errors: Dict[str, List[ValidationError]],
        processing_errors: Dict[str, List[str]]
    ) -> ExceptionSummary:
        """
        Generate comprehensive exception reports for all entity types.
        
        Args:
            load_id: ETL load identifier
            upsert_results: Results from upsert operations
            validation_errors: Validation errors by entity type
            processing_errors: Processing errors by entity type
            
        Returns:
            ExceptionSummary with complete exception details
        """
        logger.info(f"Generating exception reports for load_id: {load_id}")
        
        entity_reports = []
        critical_errors = []
        
        # Process each entity type
        for result in upsert_results:
            entity_name = result.entity_name
            
            # Get validation errors for this entity
            entity_validation_errors = validation_errors.get(entity_name, [])
            
            # Get processing errors for this entity
            entity_processing_errors = processing_errors.get(entity_name, [])
            
            # Calculate success rate
            total_records = result.rows_processed
            failed_records = result.rows_processed - result.rows_inserted - result.rows_updated
            success_rate = (total_records - failed_records) / total_records * 100 if total_records > 0 else 100.0
            
            # Generate recommendations
            recommendations = self._generate_recommendations(
                entity_name, entity_validation_errors, entity_processing_errors, result
            )
            
            # Create entity report
            entity_report = EntityExceptionReport(
                entity_name=entity_name,
                load_id=load_id,
                total_records=total_records,
                failed_records=failed_records,
                success_rate=success_rate,
                validation_errors=entity_validation_errors,
                processing_errors=entity_processing_errors,
                timestamp=datetime.now(),
                recommendations=recommendations
            )
            
            entity_reports.append(entity_report)
            
            # Collect critical errors
            critical_errors.extend(self._extract_critical_errors(entity_report))
        
        # Calculate overall summary
        total_entities = len(entity_reports)
        failed_entities = sum(1 for report in entity_reports if report.success_rate < 100.0)
        overall_success_rate = sum(report.success_rate for report in entity_reports) / total_entities if total_entities > 0 else 100.0
        
        exception_summary = ExceptionSummary(
            load_id=load_id,
            total_entities=total_entities,
            failed_entities=failed_entities,
            overall_success_rate=overall_success_rate,
            entity_reports=entity_reports,
            critical_errors=critical_errors,
            timestamp=datetime.now()
        )
        
        # Save reports to files
        self._save_exception_reports(exception_summary)
        
        # Log summary
        self._log_exception_summary(exception_summary)
        
        return exception_summary
    
    def _generate_recommendations(
        self, 
        entity_name: str, 
        validation_errors: List[ValidationError],
        processing_errors: List[str],
        upsert_result: UpsertResult
    ) -> List[str]:
        """Generate recommendations based on errors and failures."""
        recommendations = []
        
        # Analyze validation errors
        if validation_errors:
            error_types = {}
            for error in validation_errors:
                error_type = error.error_type
                error_types[error_type] = error_types.get(error_type, 0) + 1
            
            # Generate recommendations based on error types
            if 'required_field_missing' in error_types:
                recommendations.append(f"Review data source for missing required fields in {entity_name}")
            
            if 'invalid_format' in error_types:
                recommendations.append(f"Validate data format for {entity_name} before processing")
            
            if 'constraint_violation' in error_types:
                recommendations.append(f"Check data constraints and business rules for {entity_name}")
            
            if 'type_conversion' in error_types:
                recommendations.append(f"Verify data types match expected schema for {entity_name}")
        
        # Analyze processing errors
        if processing_errors:
            recommendations.append(f"Review processing logic for {entity_name} - {len(processing_errors)} errors occurred")
        
        # Analyze upsert results
        if upsert_result.failed_rows > 0:
            recommendations.append(f"Investigate {upsert_result.failed_rows} failed records in {entity_name}")
        
        if not recommendations:
            recommendations.append(f"No specific issues identified for {entity_name}")
        
        return recommendations
    
    def _extract_critical_errors(self, entity_report: EntityExceptionReport) -> List[str]:
        """Extract critical errors from entity report."""
        critical_errors = []
        
        # Check for critical field errors
        critical_fields = self.critical_fields.get(entity_report.entity_name, [])
        for error in entity_report.validation_errors:
            if error.field_name in critical_fields and error.severity == "ERROR":
                critical_errors.append(
                    f"{entity_report.entity_name}: Critical field '{error.field_name}' error - {error.error_message}"
                )
        
        # Check for high failure rates
        if entity_report.success_rate < 50.0:
            critical_errors.append(
                f"{entity_report.entity_name}: High failure rate ({entity_report.success_rate:.1f}%)"
            )
        
        # Check for processing errors
        if entity_report.processing_errors:
            critical_errors.append(
                f"{entity_report.entity_name}: {len(entity_report.processing_errors)} processing errors"
            )
        
        return critical_errors
    
    def _save_exception_reports(self, exception_summary: ExceptionSummary):
        """Save exception reports to files."""
        try:
            # Save overall summary
            summary_data = asdict(exception_summary)
            summary_data['timestamp'] = summary_data['timestamp'].isoformat()
            
            for report in summary_data['entity_reports']:
                report['timestamp'] = report['timestamp'].isoformat()
                for error in report['validation_errors']:
                    if hasattr(error, 'timestamp'):
                        error['timestamp'] = error['timestamp'].isoformat()
            
            summary_filename = f"exception_summary_{exception_summary.load_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            summary_filepath = self.output_directory / summary_filename
            
            with open(summary_filepath, 'w') as f:
                json.dump(summary_data, f, indent=2)
            
            logger.info(f"Exception summary saved to: {summary_filepath}")
            
            # Save individual entity reports as CSV
            for entity_report in exception_summary.entity_reports:
                self._save_entity_report_csv(entity_report)
            
        except Exception as e:
            logger.error(f"Failed to save exception reports: {e}")
    
    def _save_entity_report_csv(self, entity_report: EntityExceptionReport):
        """Save individual entity report as CSV."""
        try:
            csv_filename = f"exception_{entity_report.entity_name}_{entity_report.load_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            csv_filepath = self.output_directory / csv_filename
            
            with open(csv_filepath, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write header
                writer.writerow([
                    'Row Number', 'Field Name', 'Field Value', 'Error Type', 
                    'Error Message', 'Severity'
                ])
                
                # Write validation errors
                for error in entity_report.validation_errors:
                    writer.writerow([
                        error.row_number,
                        error.field_name,
                        str(error.field_value),
                        error.error_type,
                        error.error_message,
                        error.severity
                    ])
            
            logger.info(f"Entity exception report saved to: {csv_filepath}")
            
        except Exception as e:
            logger.error(f"Failed to save entity report CSV for {entity_report.entity_name}: {e}")
    
    def _log_exception_summary(self, exception_summary: ExceptionSummary):
        """Log exception summary to console."""
        logger.info("=" * 80)
        logger.info(f"EXCEPTION REPORT - Load ID: {exception_summary.load_id}")
        logger.info("=" * 80)
        
        logger.info(f"Total Entities: {exception_summary.total_entities}")
        logger.info(f"Failed Entities: {exception_summary.failed_entities}")
        logger.info(f"Overall Success Rate: {exception_summary.overall_success_rate:.2f}%")
        
        if exception_summary.critical_errors:
            logger.info(f"\nCRITICAL ERRORS ({len(exception_summary.critical_errors)}):")
            for error in exception_summary.critical_errors:
                logger.error(f"  - {error}")
        
        logger.info("\nENTITY BREAKDOWN:")
        for report in exception_summary.entity_reports:
            status = "✓ SUCCESS" if report.success_rate == 100.0 else "✗ FAILED"
            logger.info(f"  {report.entity_name.upper()}: {status}")
            logger.info(f"    Total Records: {report.total_records}")
            logger.info(f"    Failed Records: {report.failed_records}")
            logger.info(f"    Success Rate: {report.success_rate:.2f}%")
            logger.info(f"    Validation Errors: {len(report.validation_errors)}")
            logger.info(f"    Processing Errors: {len(report.processing_errors)}")
            
            if report.recommendations:
                logger.info(f"    Recommendations:")
                for rec in report.recommendations:
                    logger.info(f"      - {rec}")
        
        logger.info("=" * 80)
    
    def validate_csv_headers(self, entity_name: str, csv_headers: List[str]) -> List[ValidationError]:
        """
        Validate CSV headers against expected schema.
        
        Args:
            entity_name: Name of the entity type
            csv_headers: List of CSV column headers
            
        Returns:
            List of validation errors
        """
        validation_errors = []
        
        # Get expected headers from model
        from ..models import MODEL_REGISTRY
        model_class = MODEL_REGISTRY.get(entity_name)
        if not model_class:
            validation_errors.append(ValidationError(
                row_number=0,
                field_name="schema",
                field_value=entity_name,
                error_type="unknown_entity",
                error_message=f"Unknown entity type: {entity_name}"
            ))
            return validation_errors
        
        # Get expected headers from model aliases
        expected_headers = []
        for field_name, field_info in model_class.__fields__.items():
            if hasattr(field_info, 'alias') and field_info.alias:
                expected_headers.append(field_info.alias)
            else:
                expected_headers.append(field_name)
        
        # Check for missing headers
        missing_headers = set(expected_headers) - set(csv_headers)
        for header in missing_headers:
            validation_errors.append(ValidationError(
                row_number=0,
                field_name=header,
                field_value=None,
                error_type="missing_header",
                error_message=f"Missing required header: {header}"
            ))
        
        # Check for extra headers
        extra_headers = set(csv_headers) - set(expected_headers)
        for header in extra_headers:
            validation_errors.append(ValidationError(
                row_number=0,
                field_name=header,
                field_value=None,
                error_type="extra_header",
                error_message=f"Unexpected header: {header}",
                severity="WARNING"
            ))
        
        return validation_errors
    
    def validate_row_data(self, entity_name: str, row_data: Dict[str, Any], row_number: int) -> List[ValidationError]:
        """
        Validate individual row data against model constraints.
        
        Args:
            entity_name: Name of the entity type
            row_data: Dictionary of row data
            row_number: Row number for error reporting
            
        Returns:
            List of validation errors
        """
        validation_errors = []
        
        try:
            from ..models import MODEL_REGISTRY
            model_class = MODEL_REGISTRY.get(entity_name)
            if not model_class:
                validation_errors.append(ValidationError(
                    row_number=row_number,
                    field_name="entity",
                    field_value=entity_name,
                    error_type="unknown_entity",
                    error_message=f"Unknown entity type: {entity_name}"
                ))
                return validation_errors
            
            # Try to create model instance to trigger validation
            model_class(**row_data)
            
        except Exception as e:
            # Parse validation error
            error_message = str(e)
            if "validation error" in error_message.lower():
                # Extract field name from error message
                field_name = "unknown"
                if "field" in error_message:
                    # Try to extract field name from error message
                    parts = error_message.split("field")
                    if len(parts) > 1:
                        field_name = parts[1].split()[0].strip("'\"")
                
                validation_errors.append(ValidationError(
                    row_number=row_number,
                    field_name=field_name,
                    field_value=row_data.get(field_name),
                    error_type="validation_error",
                    error_message=error_message
                ))
            else:
                validation_errors.append(ValidationError(
                    row_number=row_number,
                    field_name="general",
                    field_value=None,
                    error_type="processing_error",
                    error_message=error_message
                ))
        
        return validation_errors
    
    def get_exception_files(self, load_id: Optional[str] = None) -> List[Path]:
        """
        Get list of exception report files, optionally filtered by load_id.
        
        Args:
            load_id: Optional load_id to filter by
            
        Returns:
            List of exception report file paths
        """
        exception_files = []
        
        for file_path in self.output_directory.glob("exception_*.json"):
            if load_id and load_id not in file_path.name:
                continue
            exception_files.append(file_path)
        
        return sorted(exception_files, key=lambda x: x.stat().st_mtime, reverse=True)
    
    def load_exception_summary(self, file_path: Path) -> ExceptionSummary:
        """
        Load exception summary from file.
        
        Args:
            file_path: Path to exception summary file
            
        Returns:
            ExceptionSummary object
        """
        try:
            with open(file_path, 'r') as f:
                summary_data = json.load(f)
            
            # Convert ISO format strings back to datetime objects
            if summary_data.get('timestamp'):
                summary_data['timestamp'] = datetime.fromisoformat(summary_data['timestamp'])
            
            # Convert entity reports
            for report in summary_data.get('entity_reports', []):
                if report.get('timestamp'):
                    report['timestamp'] = datetime.fromisoformat(report['timestamp'])
            
            return ExceptionSummary(**summary_data)
            
        except Exception as e:
            logger.error(f"Failed to load exception summary from {file_path}: {e}")
            raise
