"""
Pydantic models for ETL data ingestion.

This module provides Pydantic models for all entity types in the automotive
shop management system data ingestion pipeline. Models include comprehensive
validation, field constraints, and CSV serialization/deserialization methods.

Entity Types:
- Customer: Customer information and contact details
- Vehicle: Vehicle specifications and ACES standard fields
- Invoice: Service invoices and financial information
- LineItem: Individual line items within invoices
- Payment: Payment transactions and methods
- InventoryPart: Parts inventory and specifications
- Supplier: Supplier information and contact details

All models support:
- Field validation with business logic constraints
- CSV column name mapping via aliases
- Optional fields to handle incomplete data
- ETL metadata tracking via load_id
- Serialization methods for CSV export/import
"""

from .customer import Customer
from .vehicle import Vehicle
from .invoice import Invoice
from .line_item import LineItem
from .payment import Payment
from .inventory_part import InventoryPart
from .supplier import Supplier

# Export all models for easy importing
__all__ = [
    'Customer',
    'Vehicle', 
    'Invoice',
    'LineItem',
    'Payment',
    'InventoryPart',
    'Supplier'
]

# Model registry for dynamic access
MODEL_REGISTRY = {
    'customer': Customer,
    'vehicle': Vehicle,
    'invoice': Invoice,
    'line_item': LineItem,
    'payment': Payment,
    'inventory_part': InventoryPart,
    'supplier': Supplier
}

def get_model_by_name(model_name: str):
    """
    Get a model class by its name.
    
    Args:
        model_name: Name of the model (e.g., 'customer', 'vehicle')
        
    Returns:
        Model class
        
    Raises:
        KeyError: If model name is not found
    """
    if model_name not in MODEL_REGISTRY:
        available_models = ', '.join(MODEL_REGISTRY.keys())
        raise KeyError(f'Model "{model_name}" not found. Available models: {available_models}')
    
    return MODEL_REGISTRY[model_name]

def get_all_model_names():
    """
    Get list of all available model names.
    
    Returns:
        List of model names
    """
    return list(MODEL_REGISTRY.keys())
