"""
Line Item Pydantic model for ETL data ingestion.

This model represents line item data from third-party automotive shop management systems.
Supports hierarchical line items with parent-child relationships.
"""

from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, Field, validator
from enum import Enum


class DatalineType(str, Enum):
    """Line item type enumeration."""
    PART = "part"
    LABOR = "labor"
    FLAT_FEE = "flat_fee"
    SHOP_SUPPLIES = "shop_supplies"
    HAZMAT = "hazmat"
    DISCOUNT = "discount"
    TAX = "tax"
    OTHER = "other"


class MarkupCalcMethod(str, Enum):
    """Markup calculation method enumeration."""
    PERCENTAGE = "percentage"
    FIXED_AMOUNT = "fixed_amount"
    MULTIPLIER = "multiplier"


class MarkupType(str, Enum):
    """Markup type enumeration."""
    COST_PLUS = "cost_plus"
    RETAIL_MINUS = "retail_minus"
    MARGIN = "margin"


class LineItem(BaseModel):
    """
    Line Item model for automotive shop management system data ingestion.
    
    Represents individual line items within invoices including parts, labor,
    and other charges. Supports hierarchical relationships for complex billing.
    """
    
    # Source system identification
    source_app_name: Optional[str] = Field(
        None,
        alias="sourceAppName",
        max_length=100,
        description="Name of the source application/system"
    )
    external_shop_id: Optional[str] = Field(
        None,
        alias="externalShopId",
        max_length=50,
        description="External shop identifier from source system"
    )
    external_document_id: Optional[str] = Field(
        None,
        alias="externalDocumentId",
        max_length=50,
        description="External document identifier from source system"
    )
    external_dataline_id: Optional[str] = Field(
        None,
        alias="externalDatalineId",
        max_length=50,
        description="External line item identifier from source system"
    )
    external_parent_dataline_id: Optional[str] = Field(
        None,
        alias="externalParentDatalineId",
        max_length=50,
        description="External parent line item identifier for hierarchical items"
    )
    
    # Line item identification and ordering
    line_number: Optional[int] = Field(
        None,
        alias="lineNumber",
        ge=1,
        description="Line number within the document"
    )
    dataline_type: Optional[DatalineType] = Field(
        None,
        alias="datalineType",
        description="Type of line item"
    )
    dataline_name: Optional[str] = Field(
        None,
        alias="datalineName",
        max_length=200,
        description="Name or title of the line item"
    )
    description: Optional[str] = Field(
        None,
        max_length=1000,
        description="Detailed description of the line item"
    )
    part_number: Optional[str] = Field(
        None,
        alias="partNumber",
        max_length=100,
        description="Part number for parts line items"
    )
    
    # Pricing information
    cost: Optional[Decimal] = Field(
        None,
        ge=0,
        description="Cost price of the item"
    )
    labor_rate: Optional[Decimal] = Field(
        None,
        alias="laborRate",
        ge=0,
        description="Hourly labor rate"
    )
    price_rate: Optional[Decimal] = Field(
        None,
        alias="priceRate",
        ge=0,
        description="Standard price rate"
    )
    manual_price_rate: Optional[Decimal] = Field(
        None,
        alias="manualPriceRate",
        ge=0,
        description="Manually set price rate"
    )
    quantity_or_hours: Optional[Decimal] = Field(
        None,
        alias="quantityOrHours",
        ge=0,
        description="Quantity for parts or hours for labor"
    )
    marked_up_hours: Optional[Decimal] = Field(
        None,
        alias="markedUpHours",
        ge=0,
        description="Hours after markup calculation"
    )
    
    # Financial calculations
    subtotal: Optional[Decimal] = Field(
        None,
        ge=0,
        description="Subtotal for this line item"
    )
    taxable: Optional[bool] = Field(
        None,
        description="Whether this line item is taxable"
    )
    total: Optional[Decimal] = Field(
        None,
        ge=0,
        description="Total amount for this line item"
    )
    non_taxable_total: Optional[Decimal] = Field(
        None,
        alias="nonTaxableTotal",
        ge=0,
        description="Non-taxable portion of the total"
    )
    
    # Discount and markup information
    invalid_discount: Optional[bool] = Field(
        None,
        alias="invalidDiscount",
        description="Whether the discount is invalid"
    )
    markup_amount: Optional[Decimal] = Field(
        None,
        alias="markupAmount",
        ge=0,
        description="Markup amount applied"
    )
    markup_calc_method_ref_cd: Optional[MarkupCalcMethod] = Field(
        None,
        alias="markupCalcMethodRefCd",
        description="Markup calculation method reference code"
    )
    markup_type_ref_cd: Optional[MarkupType] = Field(
        None,
        alias="markupTypeRefCd",
        description="Markup type reference code"
    )
    
    # ETL metadata
    load_id: Optional[str] = Field(
        None,
        alias="load_id",
        max_length=100,
        description="ETL load identifier for tracking and audit purposes"
    )
    
    @validator('dataline_type')
    def validate_dataline_type(cls, v):
        """Validate dataline type."""
        if v is None:
            return v
        
        # Allow string values and convert to enum
        if isinstance(v, str):
            try:
                return DatalineType(v.lower())
            except ValueError:
                raise ValueError(f'Invalid dataline type: {v}. Must be one of: {[t.value for t in DatalineType]}')
        
        return v
    
    @validator('markup_calc_method_ref_cd')
    def validate_markup_calc_method(cls, v):
        """Validate markup calculation method."""
        if v is None:
            return v
        
        # Allow string values and convert to enum
        if isinstance(v, str):
            try:
                return MarkupCalcMethod(v.lower())
            except ValueError:
                raise ValueError(f'Invalid markup calc method: {v}. Must be one of: {[m.value for m in MarkupCalcMethod]}')
        
        return v
    
    @validator('markup_type_ref_cd')
    def validate_markup_type(cls, v):
        """Validate markup type."""
        if v is None:
            return v
        
        # Allow string values and convert to enum
        if isinstance(v, str):
            try:
                return MarkupType(v.lower())
            except ValueError:
                raise ValueError(f'Invalid markup type: {v}. Must be one of: {[t.value for t in MarkupType]}')
        
        return v
    
    @validator('quantity_or_hours')
    def validate_quantity_or_hours(cls, v, values):
        """Validate quantity or hours based on dataline type."""
        if v is None:
            return v
        
        dataline_type = values.get('dataline_type')
        if dataline_type == DatalineType.LABOR and v <= 0:
            raise ValueError('Hours must be greater than 0 for labor line items')
        
        return v
    
    @validator('part_number')
    def validate_part_number(cls, v, values):
        """Validate part number format."""
        if v is None:
            return v
        
        # Basic part number validation - alphanumeric with some special characters
        if not v.strip():
            raise ValueError('Part number cannot be empty')
        
        return v.strip()
    
    @validator('total')
    def validate_total_calculation(cls, v, values):
        """Validate total calculation consistency."""
        if v is None:
            return v
        
        subtotal = values.get('subtotal')
        if subtotal is not None and v < subtotal:
            raise ValueError('Total cannot be less than subtotal')
        
        return v
    
    class Config:
        """Pydantic configuration for LineItem model."""
        allow_population_by_field_name = True
        validate_assignment = True
        use_enum_values = True
        json_encoders = {
            Decimal: str
        }
    
    def to_csv_dict(self) -> dict:
        """
        Convert model to dictionary suitable for CSV export.
        
        Returns:
            dict: Dictionary with CSV column names as keys
        """
        return self.dict(by_alias=True, exclude_none=True)
    
    @classmethod
    def from_csv_dict(cls, data: dict) -> 'LineItem':
        """
        Create LineItem instance from CSV dictionary.
        
        Args:
            data: Dictionary with CSV column names as keys
            
        Returns:
            LineItem: LineItem instance
        """
        return cls(**data)
