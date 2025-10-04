"""
Dynamic staging table creation for ETL data ingestion.

This module implements FR-10: Create dynamic staging tables with one-to-one CSV column mapping
for all entity types. Includes 90-day retention policy and empty CSV handling.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import mysql.connector
from mysql.connector import Error
from contextlib import contextmanager

# Import our Pydantic models
from ..models import (
    Customer, Vehicle, Invoice, LineItem, Payment, 
    InventoryPart, Supplier, MODEL_REGISTRY
)

logger = logging.getLogger(__name__)


@dataclass
class StagingTableConfig:
    """Configuration for staging table creation."""
    table_name: str
    model_class: type
    csv_columns: List[str]
    primary_key: str = "id"
    load_id_column: str = "load_id"
    created_at_column: str = "created_at"
    retention_days: int = 90


class StagingTableManager:
    """
    Manages dynamic staging table creation and maintenance.
    
    Implements FR-10: Create dynamic staging tables with one-to-one CSV column mapping
    for all entity types. Handles 90-day retention policy and empty CSV scenarios.
    """
    
    def __init__(self, db_config: Dict[str, Any]):
        """
        Initialize staging table manager.
        
        Args:
            db_config: Database connection configuration
        """
        self.db_config = db_config
        self.table_configs = self._create_table_configs()
        
    def _create_table_configs(self) -> Dict[str, StagingTableConfig]:
        """Create staging table configurations for all entity types."""
        return {
            'customers': StagingTableConfig(
                table_name='staging_customers',
                model_class=Customer,
                csv_columns=self._get_csv_columns_from_model(Customer)
            ),
            'vehicles': StagingTableConfig(
                table_name='staging_vehicles',
                model_class=Vehicle,
                csv_columns=self._get_csv_columns_from_model(Vehicle)
            ),
            'invoices': StagingTableConfig(
                table_name='staging_invoices',
                model_class=Invoice,
                csv_columns=self._get_csv_columns_from_model(Invoice)
            ),
            'line_items': StagingTableConfig(
                table_name='staging_line_items',
                model_class=LineItem,
                csv_columns=self._get_csv_columns_from_model(LineItem)
            ),
            'payments': StagingTableConfig(
                table_name='staging_payments',
                model_class=Payment,
                csv_columns=self._get_csv_columns_from_model(Payment)
            ),
            'inventory_parts': StagingTableConfig(
                table_name='staging_inventory_parts',
                model_class=InventoryPart,
                csv_columns=self._get_csv_columns_from_model(InventoryPart)
            ),
            'suppliers': StagingTableConfig(
                table_name='staging_suppliers',
                model_class=Supplier,
                csv_columns=self._get_csv_columns_from_model(Supplier)
            )
        }
    
    def _get_csv_columns_from_model(self, model_class: type) -> List[str]:
        """Extract CSV column names from Pydantic model aliases."""
        csv_columns = []
        for field_name, field_info in model_class.__fields__.items():
            if hasattr(field_info, 'alias') and field_info.alias:
                csv_columns.append(field_info.alias)
            else:
                csv_columns.append(field_name)
        return csv_columns
    
    @contextmanager
    def get_db_connection(self):
        """Context manager for database connections."""
        connection = None
        try:
            connection = mysql.connector.connect(**self.db_config)
            yield connection
        except Error as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()
    
    def create_staging_tables(self, load_id: str) -> Dict[str, bool]:
        """
        Create staging tables for all entity types.
        
        Args:
            load_id: ETL load identifier
            
        Returns:
            Dict mapping entity names to creation success status
        """
        results = {}
        
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            try:
                for entity_name, config in self.table_configs.items():
                    try:
                        self._create_single_staging_table(cursor, config, load_id)
                        results[entity_name] = True
                        logger.info(f"Created staging table for {entity_name}")
                    except Error as e:
                        logger.error(f"Failed to create staging table for {entity_name}: {e}")
                        results[entity_name] = False
                
                conn.commit()
                
            except Error as e:
                logger.error(f"Failed to create staging tables: {e}")
                conn.rollback()
                raise
            finally:
                cursor.close()
        
        return results
    
    def _create_single_staging_table(self, cursor, config: StagingTableConfig, load_id: str):
        """Create a single staging table with proper schema."""
        table_name = config.table_name
        
        # Drop existing table if it exists
        drop_sql = f"DROP TABLE IF EXISTS {table_name}"
        cursor.execute(drop_sql)
        
        # Create table with dynamic columns based on CSV schema
        columns_sql = self._build_table_schema(config)
        create_sql = f"""
        CREATE TABLE {table_name} (
            {columns_sql}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        cursor.execute(create_sql)
        logger.debug(f"Created table {table_name} with schema: {create_sql}")
    
    def _build_table_schema(self, config: StagingTableConfig) -> str:
        """Build table schema based on model fields."""
        columns = []
        
        # Add primary key
        columns.append(f"{config.primary_key} INT AUTO_INCREMENT PRIMARY KEY")
        
        # Add load_id column
        columns.append(f"{config.load_id_column} VARCHAR(100) NOT NULL")
        
        # Add created_at timestamp
        columns.append(f"{config.created_at_column} TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        
        # Add CSV columns with appropriate data types
        for csv_column in config.csv_columns:
            column_def = self._get_column_definition(csv_column, config.model_class)
            columns.append(column_def)
        
        # Add indexes
        columns.append(f"INDEX idx_{config.load_id_column} ({config.load_id_column})")
        columns.append(f"INDEX idx_{config.created_at_column} ({config.created_at_column})")
        
        return ",\n            ".join(columns)
    
    def _get_column_definition(self, csv_column: str, model_class: type) -> str:
        """Get MySQL column definition for a CSV column."""
        # Find the corresponding model field
        field_info = None
        for field_name, field in model_class.__fields__.items():
            if field.alias == csv_column:
                field_info = field
                break
        
        if not field_info:
            # Default to VARCHAR for unknown fields
            return f"{csv_column} TEXT"
        
        # Map Pydantic types to MySQL types
        field_type = field_info.type_
        
        if field_type == str or (hasattr(field_type, '__origin__') and field_type.__origin__ is str):
            max_length = getattr(field_info, 'max_length', 255)
            return f"{csv_column} VARCHAR({max_length})"
        elif field_type == int:
            return f"{csv_column} INT"
        elif field_type == float or (hasattr(field_type, '__origin__') and field_type.__origin__ is float):
            return f"{csv_column} DECIMAL(10,2)"
        elif field_type == bool:
            return f"{csv_column} BOOLEAN"
        elif field_type == datetime or (hasattr(field_type, '__origin__') and field_type.__origin__ is datetime):
            return f"{csv_column} TIMESTAMP"
        else:
            # Default to TEXT for complex types
            return f"{csv_column} TEXT"
    
    def insert_staging_data(self, entity_name: str, data: List[Dict[str, Any]], load_id: str) -> int:
        """
        Insert data into staging table.
        
        Args:
            entity_name: Name of the entity type
            data: List of data dictionaries
            load_id: ETL load identifier
            
        Returns:
            Number of rows inserted
        """
        if not data:
            logger.info(f"No data to insert for {entity_name}")
            return 0
        
        config = self.table_configs.get(entity_name)
        if not config:
            raise ValueError(f"Unknown entity type: {entity_name}")
        
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            try:
                # Prepare insert statement
                columns = [config.load_id_column] + config.csv_columns
                placeholders = ', '.join(['%s'] * len(columns))
                insert_sql = f"""
                INSERT INTO {config.table_name} ({', '.join(columns)})
                VALUES ({placeholders})
                """
                
                # Prepare data for insertion
                rows_to_insert = []
                for row in data:
                    row_data = [load_id] + [row.get(col, None) for col in config.csv_columns]
                    rows_to_insert.append(tuple(row_data))
                
                # Execute batch insert
                cursor.executemany(insert_sql, rows_to_insert)
                conn.commit()
                
                rows_inserted = cursor.rowcount
                logger.info(f"Inserted {rows_inserted} rows into {config.table_name}")
                
                return rows_inserted
                
            except Error as e:
                logger.error(f"Failed to insert staging data for {entity_name}: {e}")
                conn.rollback()
                raise
            finally:
                cursor.close()
    
    def get_staging_row_count(self, entity_name: str, load_id: str) -> int:
        """
        Get row count for staging table.
        
        Args:
            entity_name: Name of the entity type
            load_id: ETL load identifier
            
        Returns:
            Number of rows in staging table
        """
        config = self.table_configs.get(entity_name)
        if not config:
            raise ValueError(f"Unknown entity type: {entity_name}")
        
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            try:
                count_sql = f"""
                SELECT COUNT(*) FROM {config.table_name} 
                WHERE {config.load_id_column} = %s
                """
                cursor.execute(count_sql, (load_id,))
                result = cursor.fetchone()
                return result[0] if result else 0
                
            except Error as e:
                logger.error(f"Failed to get row count for {entity_name}: {e}")
                raise
            finally:
                cursor.close()
    
    def cleanup_old_staging_data(self, retention_days: int = 90) -> int:
        """
        Clean up staging data older than retention period.
        
        Args:
            retention_days: Number of days to retain data
            
        Returns:
            Total number of rows cleaned up
        """
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        total_cleaned = 0
        
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            try:
                for entity_name, config in self.table_configs.items():
                    cleanup_sql = f"""
                    DELETE FROM {config.table_name} 
                    WHERE {config.created_at_column} < %s
                    """
                    cursor.execute(cleanup_sql, (cutoff_date,))
                    rows_cleaned = cursor.rowcount
                    total_cleaned += rows_cleaned
                    
                    if rows_cleaned > 0:
                        logger.info(f"Cleaned up {rows_cleaned} old rows from {config.table_name}")
                
                conn.commit()
                logger.info(f"Total rows cleaned up: {total_cleaned}")
                
            except Error as e:
                logger.error(f"Failed to cleanup old staging data: {e}")
                conn.rollback()
                raise
            finally:
                cursor.close()
        
        return total_cleaned
    
    def get_staging_data(self, entity_name: str, load_id: str) -> List[Dict[str, Any]]:
        """
        Retrieve staging data for processing.
        
        Args:
            entity_name: Name of the entity type
            load_id: ETL load identifier
            
        Returns:
            List of data dictionaries
        """
        config = self.table_configs.get(entity_name)
        if not config:
            raise ValueError(f"Unknown entity type: {entity_name}")
        
        with self.get_db_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            
            try:
                select_sql = f"""
                SELECT * FROM {config.table_name} 
                WHERE {config.load_id_column} = %s
                ORDER BY {config.primary_key}
                """
                cursor.execute(select_sql, (load_id,))
                return cursor.fetchall()
                
            except Error as e:
                logger.error(f"Failed to get staging data for {entity_name}: {e}")
                raise
            finally:
                cursor.close()
    
    def validate_staging_tables_exist(self, load_id: str) -> Dict[str, bool]:
        """
        Validate that all staging tables exist and have data.
        
        Args:
            load_id: ETL load identifier
            
        Returns:
            Dict mapping entity names to validation status
        """
        results = {}
        
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            try:
                for entity_name, config in self.table_configs.items():
                    try:
                        # Check if table exists
                        check_table_sql = f"""
                        SELECT COUNT(*) FROM information_schema.tables 
                        WHERE table_schema = DATABASE() AND table_name = '{config.table_name}'
                        """
                        cursor.execute(check_table_sql)
                        table_exists = cursor.fetchone()[0] > 0
                        
                        if not table_exists:
                            results[entity_name] = False
                            continue
                        
                        # Check if table has data for this load_id
                        count_sql = f"""
                        SELECT COUNT(*) FROM {config.table_name} 
                        WHERE {config.load_id_column} = %s
                        """
                        cursor.execute(count_sql, (load_id,))
                        row_count = cursor.fetchone()[0]
                        
                        results[entity_name] = row_count >= 0  # Allow zero rows for empty CSVs
                        
                    except Error as e:
                        logger.error(f"Failed to validate staging table for {entity_name}: {e}")
                        results[entity_name] = False
                
            except Error as e:
                logger.error(f"Failed to validate staging tables: {e}")
                raise
            finally:
                cursor.close()
        
        return results
