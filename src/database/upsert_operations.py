"""
Production database upsert operations for ETL data ingestion.

This module implements FR-12: Perform transactional upserts to production database
in dependency order. Includes rollback capabilities and row count validation.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import mysql.connector
from mysql.connector import Error
from contextlib import contextmanager

# Import our Pydantic models
from ..models import (
    Customer, Vehicle, Invoice, LineItem, Payment, 
    InventoryPart, Supplier
)

logger = logging.getLogger(__name__)


@dataclass
class UpsertResult:
    """Result of an upsert operation."""
    entity_name: str
    rows_processed: int
    rows_inserted: int
    rows_updated: int
    success: bool
    error_message: Optional[str] = None


@dataclass
class ProcessingLedger:
    """Processing ledger for tracking upsert results."""
    load_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    results: List[UpsertResult] = None
    total_rows_processed: int = 0
    success: bool = False
    
    def __post_init__(self):
        if self.results is None:
            self.results = []


class ProductionUpsertManager:
    """
    Manages production database upsert operations.
    
    Implements FR-12: Perform transactional upserts to production database
    in dependency order. Handles rollback capabilities and row count validation.
    """
    
    # Define processing order based on dependencies
    PROCESSING_ORDER = [
        'customers',      # No dependencies
        'vehicles',       # Depends on customers
        'invoices',       # Depends on customers and vehicles
        'line_items',     # Depends on invoices
        'payments',       # Depends on invoices
        'inventory_parts', # Independent
        'suppliers'       # Independent
    ]
    
    def __init__(self, db_config: Dict[str, Any]):
        """
        Initialize production upsert manager.
        
        Args:
            db_config: Database connection configuration
        """
        self.db_config = db_config
        self.model_mapping = {
            'customers': Customer,
            'vehicles': Vehicle,
            'invoices': Invoice,
            'line_items': LineItem,
            'payments': Payment,
            'inventory_parts': InventoryPart,
            'suppliers': Supplier
        }
    
    @contextmanager
    def get_db_connection(self):
        """Context manager for database connections with transaction support."""
        connection = None
        try:
            connection = mysql.connector.connect(**self.db_config)
            connection.autocommit = False  # Enable transaction mode
            yield connection
        except Error as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()
    
    def process_upserts(self, load_id: str, staging_data: Dict[str, List[Dict[str, Any]]]) -> ProcessingLedger:
        """
        Process upserts in dependency order.
        
        Args:
            load_id: ETL load identifier
            staging_data: Dictionary mapping entity names to staging data
            
        Returns:
            ProcessingLedger with results
        """
        ledger = ProcessingLedger(
            load_id=load_id,
            start_time=datetime.now()
        )
        
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                
                try:
                    # Process entities in dependency order
                    for entity_name in self.PROCESSING_ORDER:
                        if entity_name not in staging_data:
                            logger.warning(f"No staging data found for {entity_name}")
                            continue
                        
                        data = staging_data[entity_name]
                        if not data:
                            logger.info(f"Skipping {entity_name} - no data to process")
                            continue
                        
                        result = self._upsert_entity_data(
                            cursor, entity_name, data, load_id
                        )
                        ledger.results.append(result)
                        ledger.total_rows_processed += result.rows_processed
                        
                        if not result.success:
                            logger.error(f"Failed to process {entity_name}: {result.error_message}")
                            # Continue processing other entities for independent ones
                            if entity_name in ['inventory_parts', 'suppliers']:
                                continue
                            else:
                                # Rollback for dependent entities
                                conn.rollback()
                                ledger.success = False
                                ledger.end_time = datetime.now()
                                return ledger
                    
                    # All upserts successful
                    conn.commit()
                    ledger.success = True
                    ledger.end_time = datetime.now()
                    logger.info(f"Successfully processed {ledger.total_rows_processed} rows for load_id: {load_id}")
                    
                except Error as e:
                    logger.error(f"Transaction failed: {e}")
                    conn.rollback()
                    ledger.success = False
                    ledger.end_time = datetime.now()
                    raise
                finally:
                    cursor.close()
        
        except Exception as e:
            logger.error(f"Unexpected error during upsert processing: {e}")
            ledger.success = False
            ledger.end_time = datetime.now()
        
        return ledger
    
    def _upsert_entity_data(self, cursor, entity_name: str, data: List[Dict[str, Any]], load_id: str) -> UpsertResult:
        """
        Upsert data for a specific entity type.
        
        Args:
            cursor: Database cursor
            entity_name: Name of the entity type
            data: List of data dictionaries
            load_id: ETL load identifier
            
        Returns:
            UpsertResult with operation details
        """
        try:
            model_class = self.model_mapping[entity_name]
            rows_inserted = 0
            rows_updated = 0
            
            for row_data in data:
                try:
                    # Validate data using Pydantic model
                    validated_data = model_class(**row_data)
                    
                    # Perform upsert based on entity type
                    if entity_name == 'customers':
                        result = self._upsert_customer(cursor, validated_data, load_id)
                    elif entity_name == 'vehicles':
                        result = self._upsert_vehicle(cursor, validated_data, load_id)
                    elif entity_name == 'invoices':
                        result = self._upsert_invoice(cursor, validated_data, load_id)
                    elif entity_name == 'line_items':
                        result = self._upsert_line_item(cursor, validated_data, load_id)
                    elif entity_name == 'payments':
                        result = self._upsert_payment(cursor, validated_data, load_id)
                    elif entity_name == 'inventory_parts':
                        result = self._upsert_inventory_part(cursor, validated_data, load_id)
                    elif entity_name == 'suppliers':
                        result = self._upsert_supplier(cursor, validated_data, load_id)
                    else:
                        raise ValueError(f"Unknown entity type: {entity_name}")
                    
                    if result == 'inserted':
                        rows_inserted += 1
                    elif result == 'updated':
                        rows_updated += 1
                    
                except Exception as e:
                    logger.error(f"Failed to process row for {entity_name}: {e}")
                    # Continue processing other rows
                    continue
            
            return UpsertResult(
                entity_name=entity_name,
                rows_processed=len(data),
                rows_inserted=rows_inserted,
                rows_updated=rows_updated,
                success=True
            )
            
        except Exception as e:
            logger.error(f"Failed to upsert {entity_name}: {e}")
            return UpsertResult(
                entity_name=entity_name,
                rows_processed=len(data),
                rows_inserted=0,
                rows_updated=0,
                success=False,
                error_message=str(e)
            )
    
    def _upsert_customer(self, cursor, customer: Customer, load_id: str) -> str:
        """Upsert customer data."""
        # Check if customer exists
        check_sql = """
        SELECT id FROM customers 
        WHERE external_customer_id = %s AND external_shop_id = %s
        """
        cursor.execute(check_sql, (customer.external_customer_id, customer.external_shop_id))
        existing = cursor.fetchone()
        
        if existing:
            # Update existing customer
            update_sql = """
            UPDATE customers SET
                source_app_name = %s, first_name = %s, last_name = %s,
                street_address1 = %s, street_address2 = %s, country = %s,
                state = %s, city = %s, zip_code = %s, contact_cell = %s,
                contact_work = %s, contact_home = %s, contact_email = %s,
                preferred_contact = %s, authorizer_first_name = %s,
                authorizer_last_name = %s, authorizer_phone = %s,
                default_labor_rate = %s, do_not_charge_tax = %s,
                updated_at = NOW()
            WHERE id = %s
            """
            cursor.execute(update_sql, (
                customer.source_app_name, customer.first_name, customer.last_name,
                customer.street_address1, customer.street_address2, customer.country,
                customer.state, customer.city, customer.zip_code, customer.contact_cell,
                customer.contact_work, customer.contact_home, customer.contact_email,
                customer.preferred_contact, customer.authorizer_first_name,
                customer.authorizer_last_name, customer.authorizer_phone,
                customer.default_labor_rate, customer.do_not_charge_tax,
                existing[0]
            ))
            return 'updated'
        else:
            # Insert new customer
            insert_sql = """
            INSERT INTO customers (
                source_app_name, external_shop_id, external_customer_id,
                first_name, last_name, street_address1, street_address2,
                country, state, city, zip_code, contact_cell, contact_work,
                contact_home, contact_email, preferred_contact,
                authorizer_first_name, authorizer_last_name, authorizer_phone,
                default_labor_rate, do_not_charge_tax, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
            )
            """
            cursor.execute(insert_sql, (
                customer.source_app_name, customer.external_shop_id, customer.external_customer_id,
                customer.first_name, customer.last_name, customer.street_address1, customer.street_address2,
                customer.country, customer.state, customer.city, customer.zip_code, customer.contact_cell,
                customer.contact_work, customer.contact_home, customer.contact_email, customer.preferred_contact,
                customer.authorizer_first_name, customer.authorizer_last_name, customer.authorizer_phone,
                customer.default_labor_rate, customer.do_not_charge_tax
            ))
            return 'inserted'
    
    def _upsert_vehicle(self, cursor, vehicle: Vehicle, load_id: str) -> str:
        """Upsert vehicle data."""
        # Check if vehicle exists
        check_sql = """
        SELECT id FROM vehicles 
        WHERE external_vehicle_id = %s AND external_shop_id = %s
        """
        cursor.execute(check_sql, (vehicle.external_vehicle_id, vehicle.external_shop_id))
        existing = cursor.fetchone()
        
        if existing:
            # Update existing vehicle
            update_sql = """
            UPDATE vehicles SET
                source_app_name = %s, external_customer_id = %s, year = %s,
                make = %s, model = %s, engine = %s, vin = %s, license_plate = %s,
                license_state = %s, license_country = %s, odometer_code = %s,
                odometer_label = %s, fuel_type = %s, aces_vehicle_id = %s,
                aces_engine_id = %s, aces_veh_to_eng_cfg_id = %s,
                aces_base_vehicle_id = %s, aces_transmission = %s,
                aces_body_config = %s, aces_description = %s,
                aces_trim_desc = %s, non_aces_body_type_name = %s,
                non_aces_fuel_type_name = %s, non_aces_trim_desc = %s,
                non_aces_transmission_code = %s, updated_at = NOW()
            WHERE id = %s
            """
            cursor.execute(update_sql, (
                vehicle.source_app_name, vehicle.external_customer_id, vehicle.year,
                vehicle.make, vehicle.model, vehicle.engine, vehicle.vin, vehicle.license_plate,
                vehicle.license_state, vehicle.license_country, vehicle.odometer_code,
                vehicle.odometer_label, vehicle.fuel_type, vehicle.aces_vehicle_id,
                vehicle.aces_engine_id, vehicle.aces_veh_to_eng_cfg_id,
                vehicle.aces_base_vehicle_id, vehicle.aces_transmission,
                vehicle.aces_body_config, vehicle.aces_description,
                vehicle.aces_trim_desc, vehicle.non_aces_body_type_name,
                vehicle.non_aces_fuel_type_name, vehicle.non_aces_trim_desc,
                vehicle.non_aces_transmission_code, existing[0]
            ))
            return 'updated'
        else:
            # Insert new vehicle
            insert_sql = """
            INSERT INTO vehicles (
                source_app_name, external_shop_id, external_vehicle_id, external_customer_id,
                year, make, model, engine, vin, license_plate, license_state,
                license_country, odometer_code, odometer_label, fuel_type,
                aces_vehicle_id, aces_engine_id, aces_veh_to_eng_cfg_id,
                aces_base_vehicle_id, aces_transmission, aces_body_config,
                aces_description, aces_trim_desc, non_aces_body_type_name,
                non_aces_fuel_type_name, non_aces_trim_desc, non_aces_transmission_code,
                created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
            )
            """
            cursor.execute(insert_sql, (
                vehicle.source_app_name, vehicle.external_shop_id, vehicle.external_vehicle_id, vehicle.external_customer_id,
                vehicle.year, vehicle.make, vehicle.model, vehicle.engine, vehicle.vin, vehicle.license_plate, vehicle.license_state,
                vehicle.license_country, vehicle.odometer_code, vehicle.odometer_label, vehicle.fuel_type,
                vehicle.aces_vehicle_id, vehicle.aces_engine_id, vehicle.aces_veh_to_eng_cfg_id,
                vehicle.aces_base_vehicle_id, vehicle.aces_transmission, vehicle.aces_body_config,
                vehicle.aces_description, vehicle.aces_trim_desc, vehicle.non_aces_body_type_name,
                vehicle.non_aces_fuel_type_name, vehicle.non_aces_trim_desc, vehicle.non_aces_transmission_code
            ))
            return 'inserted'
    
    def _upsert_invoice(self, cursor, invoice: Invoice, load_id: str) -> str:
        """Upsert invoice data."""
        # Check if invoice exists
        check_sql = """
        SELECT id FROM invoices 
        WHERE external_document_id = %s AND external_shop_id = %s
        """
        cursor.execute(check_sql, (invoice.external_document_id, invoice.external_shop_id))
        existing = cursor.fetchone()
        
        if existing:
            # Update existing invoice
            update_sql = """
            UPDATE invoices SET
                source_app_name = %s, created_on = %s, updated_on = %s,
                state = %s, service_tag = %s, mileage_in = %s, mileage_out = %s,
                odometer_code = %s, odometer_label = %s, shop_note = %s,
                external_customer_id = %s, external_vehicle_id = %s,
                external_service_writer_name = %s, external_service_writer_id = %s,
                external_technician_name = %s, external_technician_id = %s,
                planned_hours = %s, parts_total = %s, parts_total_discount = %s,
                labor_total = %s, labor_total_discount = %s, flat_fee_total = %s,
                flat_fee_total_discount = %s, total_discount = %s,
                shop_supplies_amount = %s, shop_supplies_manually_edited = %s,
                shop_supplies_applied_to_document = %s, haz_mat_amount = %s,
                haz_mat_manually_edited = %s, haz_mat_applied_to_document = %s,
                sub_total = %s, parts_tax = %s, labor_tax = %s, flat_fee_tax = %s,
                haz_mat_tax = %s, shop_sup_tax = %s, tax = %s, total = %s,
                non_taxable_total = %s, updated_at = NOW()
            WHERE id = %s
            """
            cursor.execute(update_sql, (
                invoice.source_app_name, invoice.created_on, invoice.updated_on,
                invoice.state, invoice.service_tag, invoice.mileage_in, invoice.mileage_out,
                invoice.odometer_code, invoice.odometer_label, invoice.shop_note,
                invoice.external_customer_id, invoice.external_vehicle_id,
                invoice.external_service_writer_name, invoice.external_service_writer_id,
                invoice.external_technician_name, invoice.external_technician_id,
                invoice.planned_hours, invoice.parts_total, invoice.parts_total_discount,
                invoice.labor_total, invoice.labor_total_discount, invoice.flat_fee_total,
                invoice.flat_fee_total_discount, invoice.total_discount,
                invoice.shop_supplies_amount, invoice.shop_supplies_manually_edited,
                invoice.shop_supplies_applied_to_document, invoice.haz_mat_amount,
                invoice.haz_mat_manually_edited, invoice.haz_mat_applied_to_document,
                invoice.sub_total, invoice.parts_tax, invoice.labor_tax, invoice.flat_fee_tax,
                invoice.haz_mat_tax, invoice.shop_sup_tax, invoice.tax, invoice.total,
                invoice.non_taxable_total, existing[0]
            ))
            return 'updated'
        else:
            # Insert new invoice
            insert_sql = """
            INSERT INTO invoices (
                source_app_name, external_document_id, external_shop_id, created_on, updated_on,
                state, service_tag, mileage_in, mileage_out, odometer_code, odometer_label,
                shop_note, external_customer_id, external_vehicle_id,
                external_service_writer_name, external_service_writer_id,
                external_technician_name, external_technician_id, planned_hours,
                parts_total, parts_total_discount, labor_total, labor_total_discount,
                flat_fee_total, flat_fee_total_discount, total_discount,
                shop_supplies_amount, shop_supplies_manually_edited,
                shop_supplies_applied_to_document, haz_mat_amount,
                haz_mat_manually_edited, haz_mat_applied_to_document,
                sub_total, parts_tax, labor_tax, flat_fee_tax, haz_mat_tax,
                shop_sup_tax, tax, total, non_taxable_total, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
            )
            """
            cursor.execute(insert_sql, (
                invoice.source_app_name, invoice.external_document_id, invoice.external_shop_id, invoice.created_on, invoice.updated_on,
                invoice.state, invoice.service_tag, invoice.mileage_in, invoice.mileage_out, invoice.odometer_code, invoice.odometer_label,
                invoice.shop_note, invoice.external_customer_id, invoice.external_vehicle_id,
                invoice.external_service_writer_name, invoice.external_service_writer_id,
                invoice.external_technician_name, invoice.external_technician_id, invoice.planned_hours,
                invoice.parts_total, invoice.parts_total_discount, invoice.labor_total, invoice.labor_total_discount,
                invoice.flat_fee_total, invoice.flat_fee_total_discount, invoice.total_discount,
                invoice.shop_supplies_amount, invoice.shop_supplies_manually_edited,
                invoice.shop_supplies_applied_to_document, invoice.haz_mat_amount,
                invoice.haz_mat_manually_edited, invoice.haz_mat_applied_to_document,
                invoice.sub_total, invoice.parts_tax, invoice.labor_tax, invoice.flat_fee_tax, invoice.haz_mat_tax,
                invoice.shop_sup_tax, invoice.tax, invoice.total, invoice.non_taxable_total
            ))
            return 'inserted'
    
    def _upsert_line_item(self, cursor, line_item: LineItem, load_id: str) -> str:
        """Upsert line item data."""
        # Check if line item exists
        check_sql = """
        SELECT id FROM line_items 
        WHERE external_dataline_id = %s AND external_shop_id = %s
        """
        cursor.execute(check_sql, (line_item.external_dataline_id, line_item.external_shop_id))
        existing = cursor.fetchone()
        
        if existing:
            # Update existing line item
            update_sql = """
            UPDATE line_items SET
                source_app_name = %s, external_document_id = %s, external_parent_dataline_id = %s,
                line_number = %s, dataline_type = %s, dataline_name = %s, description = %s,
                part_number = %s, cost = %s, labor_rate = %s, price_rate = %s,
                manual_price_rate = %s, quantity_or_hours = %s, marked_up_hours = %s,
                subtotal = %s, taxable = %s, total = %s, non_taxable_total = %s,
                invalid_discount = %s, markup_amount = %s, markup_calc_method_ref_cd = %s,
                markup_type_ref_cd = %s, updated_at = NOW()
            WHERE id = %s
            """
            cursor.execute(update_sql, (
                line_item.source_app_name, line_item.external_document_id, line_item.external_parent_dataline_id,
                line_item.line_number, line_item.dataline_type, line_item.dataline_name, line_item.description,
                line_item.part_number, line_item.cost, line_item.labor_rate, line_item.price_rate,
                line_item.manual_price_rate, line_item.quantity_or_hours, line_item.marked_up_hours,
                line_item.subtotal, line_item.taxable, line_item.total, line_item.non_taxable_total,
                line_item.invalid_discount, line_item.markup_amount, line_item.markup_calc_method_ref_cd,
                line_item.markup_type_ref_cd, existing[0]
            ))
            return 'updated'
        else:
            # Insert new line item
            insert_sql = """
            INSERT INTO line_items (
                source_app_name, external_shop_id, external_dataline_id, external_document_id,
                external_parent_dataline_id, line_number, dataline_type, dataline_name,
                description, part_number, cost, labor_rate, price_rate, manual_price_rate,
                quantity_or_hours, marked_up_hours, subtotal, taxable, total,
                non_taxable_total, invalid_discount, markup_amount,
                markup_calc_method_ref_cd, markup_type_ref_cd, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
            )
            """
            cursor.execute(insert_sql, (
                line_item.source_app_name, line_item.external_shop_id, line_item.external_dataline_id, line_item.external_document_id,
                line_item.external_parent_dataline_id, line_item.line_number, line_item.dataline_type, line_item.dataline_name,
                line_item.description, line_item.part_number, line_item.cost, line_item.labor_rate, line_item.price_rate, line_item.manual_price_rate,
                line_item.quantity_or_hours, line_item.marked_up_hours, line_item.subtotal, line_item.taxable, line_item.total,
                line_item.non_taxable_total, line_item.invalid_discount, line_item.markup_amount,
                line_item.markup_calc_method_ref_cd, line_item.markup_type_ref_cd
            ))
            return 'inserted'
    
    def _upsert_payment(self, cursor, payment: Payment, load_id: str) -> str:
        """Upsert payment data."""
        # Check if payment exists
        check_sql = """
        SELECT id FROM payments 
        WHERE external_payment_id = %s AND external_shop_id = %s
        """
        cursor.execute(check_sql, (payment.external_payment_id, payment.external_shop_id))
        existing = cursor.fetchone()
        
        if existing:
            # Update existing payment
            update_sql = """
            UPDATE payments SET
                source_app_name = %s, external_document_id = %s, payment_date = %s,
                payment_amount = %s, payment_method = %s, payment_method_type = %s,
                payment_reference_no = %s, payment_notes = %s, payment_status = %s,
                updated_at = NOW()
            WHERE id = %s
            """
            cursor.execute(update_sql, (
                payment.source_app_name, payment.external_document_id, payment.payment_date,
                payment.payment_amount, payment.payment_method, payment.payment_method_type,
                payment.payment_reference_no, payment.payment_notes, payment.payment_status,
                existing[0]
            ))
            return 'updated'
        else:
            # Insert new payment
            insert_sql = """
            INSERT INTO payments (
                source_app_name, external_shop_id, external_payment_id, external_document_id,
                payment_date, payment_amount, payment_method, payment_method_type,
                payment_reference_no, payment_notes, payment_status, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
            )
            """
            cursor.execute(insert_sql, (
                payment.source_app_name, payment.external_shop_id, payment.external_payment_id, payment.external_document_id,
                payment.payment_date, payment.payment_amount, payment.payment_method, payment.payment_method_type,
                payment.payment_reference_no, payment.payment_notes, payment.payment_status
            ))
            return 'inserted'
    
    def _upsert_inventory_part(self, cursor, inventory_part: InventoryPart, load_id: str) -> str:
        """Upsert inventory part data."""
        # Check if inventory part exists
        check_sql = """
        SELECT id FROM inventory_parts 
        WHERE external_part_id = %s AND external_shop_id = %s
        """
        cursor.execute(check_sql, (inventory_part.external_part_id, inventory_part.external_shop_id))
        existing = cursor.fetchone()
        
        if existing:
            # Update existing inventory part
            update_sql = """
            UPDATE inventory_parts SET
                source_app_name = %s, part_number = %s, part_description = %s,
                part_category = %s, unit_cost = %s, unit_price = %s,
                quantity_on_hand = %s, reorder_level = %s, supplier_id = %s,
                supplier_part_number = %s, is_active = %s, updated_at = NOW()
            WHERE id = %s
            """
            cursor.execute(update_sql, (
                inventory_part.source_app_name, inventory_part.part_number, inventory_part.part_description,
                inventory_part.part_category, inventory_part.unit_cost, inventory_part.unit_price,
                inventory_part.quantity_on_hand, inventory_part.reorder_level, inventory_part.supplier_id,
                inventory_part.supplier_part_number, inventory_part.is_active, existing[0]
            ))
            return 'updated'
        else:
            # Insert new inventory part
            insert_sql = """
            INSERT INTO inventory_parts (
                source_app_name, external_shop_id, external_part_id, part_number,
                part_description, part_category, unit_cost, unit_price,
                quantity_on_hand, reorder_level, supplier_id, supplier_part_number,
                is_active, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
            )
            """
            cursor.execute(insert_sql, (
                inventory_part.source_app_name, inventory_part.external_shop_id, inventory_part.external_part_id, inventory_part.part_number,
                inventory_part.part_description, inventory_part.part_category, inventory_part.unit_cost, inventory_part.unit_price,
                inventory_part.quantity_on_hand, inventory_part.reorder_level, inventory_part.supplier_id, inventory_part.supplier_part_number,
                inventory_part.is_active
            ))
            return 'inserted'
    
    def _upsert_supplier(self, cursor, supplier: Supplier, load_id: str) -> str:
        """Upsert supplier data."""
        # Check if supplier exists
        check_sql = """
        SELECT id FROM suppliers 
        WHERE external_supplier_id = %s AND external_shop_id = %s
        """
        cursor.execute(check_sql, (supplier.external_supplier_id, supplier.external_shop_id))
        existing = cursor.fetchone()
        
        if existing:
            # Update existing supplier
            update_sql = """
            UPDATE suppliers SET
                source_app_name = %s, supplier_name = %s, contact_person = %s,
                street_address1 = %s, street_address2 = %s, city = %s, state = %s,
                zip_code = %s, country = %s, phone_number = %s, email_address = %s,
                website = %s, is_active = %s, payment_terms = %s, updated_at = NOW()
            WHERE id = %s
            """
            cursor.execute(update_sql, (
                supplier.source_app_name, supplier.supplier_name, supplier.contact_person,
                supplier.street_address1, supplier.street_address2, supplier.city, supplier.state,
                supplier.zip_code, supplier.country, supplier.phone_number, supplier.email_address,
                supplier.website, supplier.is_active, supplier.payment_terms, existing[0]
            ))
            return 'updated'
        else:
            # Insert new supplier
            insert_sql = """
            INSERT INTO suppliers (
                source_app_name, external_shop_id, external_supplier_id, supplier_name,
                contact_person, street_address1, street_address2, city, state,
                zip_code, country, phone_number, email_address, website,
                is_active, payment_terms, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
            )
            """
            cursor.execute(insert_sql, (
                supplier.source_app_name, supplier.external_shop_id, supplier.external_supplier_id, supplier.supplier_name,
                supplier.contact_person, supplier.street_address1, supplier.street_address2, supplier.city, supplier.state,
                supplier.zip_code, supplier.country, supplier.phone_number, supplier.email_address, supplier.website,
                supplier.is_active, supplier.payment_terms
            ))
            return 'inserted'
    
    def validate_row_counts(self, load_id: str, staging_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, bool]:
        """
        Validate row counts between staging and production.
        
        Args:
            load_id: ETL load identifier
            staging_data: Dictionary mapping entity names to staging data
            
        Returns:
            Dict mapping entity names to validation status
        """
        results = {}
        
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            try:
                for entity_name, data in staging_data.items():
                    if not data:
                        results[entity_name] = True  # Empty data is valid
                        continue
                    
                    # Get production row count
                    if entity_name == 'customers':
                        count_sql = "SELECT COUNT(*) FROM customers WHERE load_id = %s"
                    elif entity_name == 'vehicles':
                        count_sql = "SELECT COUNT(*) FROM vehicles WHERE load_id = %s"
                    elif entity_name == 'invoices':
                        count_sql = "SELECT COUNT(*) FROM invoices WHERE load_id = %s"
                    elif entity_name == 'line_items':
                        count_sql = "SELECT COUNT(*) FROM line_items WHERE load_id = %s"
                    elif entity_name == 'payments':
                        count_sql = "SELECT COUNT(*) FROM payments WHERE load_id = %s"
                    elif entity_name == 'inventory_parts':
                        count_sql = "SELECT COUNT(*) FROM inventory_parts WHERE load_id = %s"
                    elif entity_name == 'suppliers':
                        count_sql = "SELECT COUNT(*) FROM suppliers WHERE load_id = %s"
                    else:
                        results[entity_name] = False
                        continue
                    
                    cursor.execute(count_sql, (load_id,))
                    production_count = cursor.fetchone()[0]
                    staging_count = len(data)
                    
                    results[entity_name] = production_count == staging_count
                    
                    if not results[entity_name]:
                        logger.warning(f"Row count mismatch for {entity_name}: staging={staging_count}, production={production_count}")
                
            except Error as e:
                logger.error(f"Failed to validate row counts: {e}")
                raise
            finally:
                cursor.close()
        
        return results
