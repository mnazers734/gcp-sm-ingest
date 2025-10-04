"""
Inventory Part Pydantic model for ETL data ingestion.

This model represents inventory part data from third-party automotive shop management systems.
Includes part specifications, pricing, and inventory management information.
"""

from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, Field, validator


class InventoryPart(BaseModel):
    """
    Inventory Part model for automotive shop management system data ingestion.
    
    Represents parts inventory including specifications, pricing, stock levels,
    and supplier relationships.
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
    external_part_id: Optional[str] = Field(
        None,
        alias="externalPartId",
        max_length=50,
        description="External part identifier from source system"
    )
    
    # Part identification
    part_number: Optional[str] = Field(
        None,
        alias="partNumber",
        max_length=100,
        description="Part number or SKU"
    )
    part_description: Optional[str] = Field(
        None,
        alias="partDescription",
        max_length=500,
        description="Detailed description of the part"
    )
    part_category: Optional[str] = Field(
        None,
        alias="partCategory",
        max_length=100,
        description="Category or classification of the part"
    )
    
    # Pricing information
    unit_cost: Optional[Decimal] = Field(
        None,
        alias="unitCost",
        ge=0,
        description="Unit cost price of the part"
    )
    unit_price: Optional[Decimal] = Field(
        None,
        alias="unitPrice",
        ge=0,
        description="Unit selling price of the part"
    )
    
    # Inventory management
    quantity_on_hand: Optional[Decimal] = Field(
        None,
        alias="quantityOnHand",
        ge=0,
        description="Current quantity in stock"
    )
    reorder_level: Optional[Decimal] = Field(
        None,
        alias="reorderLevel",
        ge=0,
        description="Minimum stock level before reordering"
    )
    
    # Supplier information
    supplier_id: Optional[str] = Field(
        None,
        alias="supplierId",
        max_length=50,
        description="Supplier identifier"
    )
    supplier_part_number: Optional[str] = Field(
        None,
        alias="supplierPartNumber",
        max_length=100,
        description="Part number as provided by supplier"
    )
    
    # Status and metadata
    is_active: Optional[bool] = Field(
        None,
        alias="isActive",
        description="Whether the part is currently active/available"
    )
    
    # ETL metadata
    load_id: Optional[str] = Field(
        None,
        alias="load_id",
        max_length=100,
        description="ETL load identifier for tracking and audit purposes"
    )
    
    @validator('part_number')
    def validate_part_number(cls, v):
        """Validate part number format."""
        if v is None:
            return v
        
        if not v.strip():
            raise ValueError('Part number cannot be empty')
        
        # Basic validation - alphanumeric with some special characters
        if not v.replace('-', '').replace('_', '').replace('.', '').replace('/', '').isalnum():
            raise ValueError('Part number contains invalid characters')
        
        return v.strip()
    
    @validator('supplier_part_number')
    def validate_supplier_part_number(cls, v):
        """Validate supplier part number format."""
        if v is None:
            return v
        
        if not v.strip():
            raise ValueError('Supplier part number cannot be empty')
        
        return v.strip()
    
    @validator('unit_price')
    def validate_unit_price(cls, v, values):
        """Validate unit price against unit cost."""
        if v is None:
            return v
        
        unit_cost = values.get('unit_cost')
        if unit_cost is not None and v < unit_cost:
            # Allow selling below cost (loss leader, clearance, etc.)
            pass
        
        return v
    
    @validator('quantity_on_hand')
    def validate_quantity_on_hand(cls, v):
        """Validate quantity on hand."""
        if v is None:
            return v
        
        # Check for reasonable maximum quantity
        if v > Decimal('999999'):
            raise ValueError('Quantity on hand seems unreasonably high')
        
        return v
    
    @validator('reorder_level')
    def validate_reorder_level(cls, v, values):
        """Validate reorder level against quantity on hand."""
        if v is None:
            return v
        
        quantity_on_hand = values.get('quantity_on_hand')
        if quantity_on_hand is not None and v > quantity_on_hand:
            # Allow reorder level to be higher than current stock
            pass
        
        return v
    
    @validator('part_description')
    def validate_part_description(cls, v):
        """Validate part description."""
        if v is None:
            return v
        
        # Clean up whitespace and validate length
        if v and len(v.strip()) > 500:
            raise ValueError('Part description too long')
        
        return v.strip() if v else v
    
    @validator('part_category')
    def validate_part_category(cls, v):
        """Validate part category."""
        if v is None:
            return v
        
        # Common automotive part categories
        common_categories = [
            'engine', 'transmission', 'brake', 'suspension', 'electrical',
            'body', 'interior', 'exterior', 'tire', 'wheel', 'filter',
            'fluid', 'belt', 'hose', 'gasket', 'seal', 'bearing',
            'sensor', 'actuator', 'motor', 'pump', 'valve', 'other'
        ]
        
        if v.lower() not in common_categories:
            # Allow custom categories but warn
            pass
        
        return v.strip() if v else v
    
    class Config:
        """Pydantic configuration for InventoryPart model."""
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
    def from_csv_dict(cls, data: dict) -> 'InventoryPart':
        """
        Create InventoryPart instance from CSV dictionary.
        
        Args:
            data: Dictionary with CSV column names as keys
            
        Returns:
            InventoryPart: InventoryPart instance
        """
        return cls(**data)
