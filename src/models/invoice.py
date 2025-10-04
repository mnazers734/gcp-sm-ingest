"""
Invoice/Document Pydantic model for ETL data ingestion.

This model represents invoice/document data from third-party automotive shop management systems.
Includes comprehensive financial and service information.
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, Field, validator
from enum import Enum


class InvoiceState(str, Enum):
    """Invoice state enumeration."""
    DRAFT = "draft"
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    PAID = "paid"


class Invoice(BaseModel):
    """
    Invoice/Document model for automotive shop management system data ingestion.
    
    Represents service invoices including financial details, service information,
    and relationships to customers and vehicles.
    """
    
    # Source system identification
    source_app_name: Optional[str] = Field(
        None,
        alias="sourceAppName",
        max_length=100,
        description="Name of the source application/system"
    )
    external_document_id: Optional[str] = Field(
        None,
        alias="externalDocumentId",
        max_length=50,
        description="External document identifier from source system"
    )
    external_shop_id: Optional[str] = Field(
        None,
        alias="externalShopId",
        max_length=50,
        description="External shop identifier from source system"
    )
    
    # Timestamps
    created_on: Optional[datetime] = Field(
        None,
        alias="createdOn",
        description="Document creation timestamp"
    )
    updated_on: Optional[datetime] = Field(
        None,
        alias="updatedOn",
        description="Document last update timestamp"
    )
    
    # Document status and service information
    state: Optional[InvoiceState] = Field(
        None,
        description="Document state/status"
    )
    service_tag: Optional[str] = Field(
        None,
        alias="serviceTag",
        max_length=100,
        description="Service tag or identifier"
    )
    
    # Mileage information
    mileage_in: Optional[int] = Field(
        None,
        alias="mileageIn",
        ge=0,
        description="Vehicle mileage when service started"
    )
    mileage_out: Optional[int] = Field(
        None,
        alias="mileageOut",
        ge=0,
        description="Vehicle mileage when service completed"
    )
    odometer_code: Optional[str] = Field(
        None,
        alias="odometerCode",
        max_length=20,
        description="Odometer reading code"
    )
    odometer_label: Optional[str] = Field(
        None,
        alias="odometerLabel",
        max_length=50,
        description="Odometer reading label"
    )
    
    # Notes and relationships
    shop_note: Optional[str] = Field(
        None,
        alias="shopNote",
        max_length=2000,
        description="Shop notes for the service"
    )
    external_customer_id: Optional[str] = Field(
        None,
        alias="externalCustomerId",
        max_length=50,
        description="External customer identifier"
    )
    external_vehicle_id: Optional[str] = Field(
        None,
        alias="externalVehicleId",
        max_length=50,
        description="External vehicle identifier"
    )
    
    # Service personnel
    external_service_writer_name: Optional[str] = Field(
        None,
        alias="externalServiceWriterName",
        max_length=100,
        description="Service writer name"
    )
    external_service_writer_id: Optional[str] = Field(
        None,
        alias="externalServiceWriterId",
        max_length=50,
        description="Service writer identifier"
    )
    external_technician_name: Optional[str] = Field(
        None,
        alias="externalTechnicianName",
        max_length=100,
        description="Technician name"
    )
    external_technician_id: Optional[str] = Field(
        None,
        alias="externalTechnicianId",
        max_length=50,
        description="Technician identifier"
    )
    planned_hours: Optional[Decimal] = Field(
        None,
        alias="plannedHours",
        ge=0,
        description="Planned service hours"
    )
    
    # Financial totals - parts
    parts_total: Optional[Decimal] = Field(
        None,
        alias="partsTotal",
        ge=0,
        description="Total parts cost"
    )
    parts_total_discount: Optional[Decimal] = Field(
        None,
        alias="partsTotalDiscount",
        ge=0,
        description="Parts discount amount"
    )
    
    # Financial totals - labor
    labor_total: Optional[Decimal] = Field(
        None,
        alias="laborTotal",
        ge=0,
        description="Total labor cost"
    )
    labor_total_discount: Optional[Decimal] = Field(
        None,
        alias="laborTotalDiscount",
        ge=0,
        description="Labor discount amount"
    )
    
    # Financial totals - flat fees
    flat_fee_total: Optional[Decimal] = Field(
        None,
        alias="flatFeeTotal",
        ge=0,
        description="Total flat fees"
    )
    flat_fee_total_discount: Optional[Decimal] = Field(
        None,
        alias="flatFeeTotalDiscount",
        ge=0,
        description="Flat fee discount amount"
    )
    
    # Overall discounts and totals
    total_discount: Optional[Decimal] = Field(
        None,
        alias="totalDiscount",
        ge=0,
        description="Total discount amount"
    )
    
    # Shop supplies
    shop_supplies_amount: Optional[Decimal] = Field(
        None,
        alias="shopSuppliesAmount",
        ge=0,
        description="Shop supplies amount"
    )
    shop_supplies_manually_edited: Optional[bool] = Field(
        None,
        alias="shopSuppliesManuallyEdited",
        description="Whether shop supplies amount was manually edited"
    )
    shop_supplies_applied_to_document: Optional[bool] = Field(
        None,
        alias="shopSuppliesAppliedToDocument",
        description="Whether shop supplies are applied to this document"
    )
    
    # Hazardous materials
    haz_mat_amount: Optional[Decimal] = Field(
        None,
        alias="hazMatAmount",
        ge=0,
        description="Hazardous materials amount"
    )
    haz_mat_manually_edited: Optional[bool] = Field(
        None,
        alias="hazMatManuallyEdited",
        description="Whether hazmat amount was manually edited"
    )
    haz_mat_applied_to_document: Optional[bool] = Field(
        None,
        alias="hazMatAppliedToDocument",
        description="Whether hazmat is applied to this document"
    )
    
    # Subtotal and tax breakdown
    sub_total: Optional[Decimal] = Field(
        None,
        alias="subTotal",
        ge=0,
        description="Subtotal before tax"
    )
    parts_tax: Optional[Decimal] = Field(
        None,
        alias="partsTax",
        ge=0,
        description="Tax on parts"
    )
    labor_tax: Optional[Decimal] = Field(
        None,
        alias="laborTax",
        ge=0,
        description="Tax on labor"
    )
    flat_fee_tax: Optional[Decimal] = Field(
        None,
        alias="flatFeeTax",
        ge=0,
        description="Tax on flat fees"
    )
    haz_mat_tax: Optional[Decimal] = Field(
        None,
        alias="hazMatTax",
        ge=0,
        description="Tax on hazardous materials"
    )
    shop_sup_tax: Optional[Decimal] = Field(
        None,
        alias="shopSupTax",
        ge=0,
        description="Tax on shop supplies"
    )
    tax: Optional[Decimal] = Field(
        None,
        ge=0,
        description="Total tax amount"
    )
    total: Optional[Decimal] = Field(
        None,
        ge=0,
        description="Total amount including tax"
    )
    non_taxable_total: Optional[Decimal] = Field(
        None,
        alias="nonTaxableTotal",
        ge=0,
        description="Non-taxable total amount"
    )
    
    # ETL metadata
    load_id: Optional[str] = Field(
        None,
        alias="load_id",
        max_length=100,
        description="ETL load identifier for tracking and audit purposes"
    )
    
    @validator('mileage_out')
    def validate_mileage_out(cls, v, values):
        """Validate that mileage out is greater than or equal to mileage in."""
        if v is None:
            return v
        
        mileage_in = values.get('mileage_in')
        if mileage_in is not None and v < mileage_in:
            raise ValueError('Mileage out must be greater than or equal to mileage in')
        
        return v
    
    @validator('created_on', 'updated_on')
    def validate_timestamps(cls, v):
        """Validate timestamp format and reasonableness."""
        if v is None:
            return v
        
        # Check if timestamp is not too far in the future
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        if v > now:
            raise ValueError('Timestamp cannot be in the future')
        
        # Check if timestamp is not too far in the past (before 1900)
        if v.year < 1900:
            raise ValueError('Timestamp cannot be before 1900')
        
        return v
    
    @validator('state')
    def validate_state(cls, v):
        """Validate invoice state."""
        if v is None:
            return v
        
        # Allow string values and convert to enum
        if isinstance(v, str):
            try:
                return InvoiceState(v.lower())
            except ValueError:
                raise ValueError(f'Invalid state: {v}. Must be one of: {[s.value for s in InvoiceState]}')
        
        return v
    
    class Config:
        """Pydantic configuration for Invoice model."""
        allow_population_by_field_name = True
        validate_assignment = True
        use_enum_values = True
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }
    
    def to_csv_dict(self) -> dict:
        """
        Convert model to dictionary suitable for CSV export.
        
        Returns:
            dict: Dictionary with CSV column names as keys
        """
        return self.dict(by_alias=True, exclude_none=True)
    
    @classmethod
    def from_csv_dict(cls, data: dict) -> 'Invoice':
        """
        Create Invoice instance from CSV dictionary.
        
        Args:
            data: Dictionary with CSV column names as keys
            
        Returns:
            Invoice: Invoice instance
        """
        return cls(**data)
