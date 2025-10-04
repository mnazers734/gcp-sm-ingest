"""
Supplier Pydantic model for ETL data ingestion.

This model represents supplier data from third-party automotive shop management systems.
Includes contact information, business details, and payment terms.
"""

from typing import Optional
from pydantic import BaseModel, Field, validator, EmailStr
import re


class Supplier(BaseModel):
    """
    Supplier model for automotive shop management system data ingestion.
    
    Represents supplier information including contact details, business information,
    and payment terms for parts and service providers.
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
    external_supplier_id: Optional[str] = Field(
        None,
        alias="externalSupplierId",
        max_length=50,
        description="External supplier identifier from source system"
    )
    
    # Supplier business information
    supplier_name: Optional[str] = Field(
        None,
        alias="supplierName",
        max_length=200,
        description="Name of the supplier company"
    )
    contact_person: Optional[str] = Field(
        None,
        alias="contactPerson",
        max_length=100,
        description="Primary contact person at the supplier"
    )
    
    # Address information
    street_address1: Optional[str] = Field(
        None,
        alias="streetAddress1",
        max_length=200,
        description="Primary street address"
    )
    street_address2: Optional[str] = Field(
        None,
        alias="streetAddress2",
        max_length=200,
        description="Secondary street address (suite, floor, etc.)"
    )
    city: Optional[str] = Field(
        None,
        max_length=100,
        description="City"
    )
    state: Optional[str] = Field(
        None,
        max_length=100,
        description="State or province"
    )
    zip_code: Optional[str] = Field(
        None,
        alias="zipCode",
        max_length=20,
        description="ZIP or postal code"
    )
    country: Optional[str] = Field(
        None,
        max_length=100,
        description="Country"
    )
    
    # Contact information
    phone_number: Optional[str] = Field(
        None,
        alias="phoneNumber",
        max_length=20,
        description="Primary phone number"
    )
    email_address: Optional[EmailStr] = Field(
        None,
        alias="emailAddress",
        description="Primary email address"
    )
    website: Optional[str] = Field(
        None,
        max_length=200,
        description="Company website URL"
    )
    
    # Business status and terms
    is_active: Optional[bool] = Field(
        None,
        alias="isActive",
        description="Whether the supplier is currently active"
    )
    payment_terms: Optional[str] = Field(
        None,
        alias="paymentTerms",
        max_length=200,
        description="Payment terms and conditions"
    )
    
    # ETL metadata
    load_id: Optional[str] = Field(
        None,
        alias="load_id",
        max_length=100,
        description="ETL load identifier for tracking and audit purposes"
    )
    
    @validator('phone_number')
    def validate_phone_number(cls, v):
        """Validate phone number format."""
        if v is None:
            return v
        
        # Remove all non-digit characters for validation
        digits_only = re.sub(r'\D', '', v)
        
        # Check if it's a valid phone number (7-15 digits)
        if len(digits_only) < 7 or len(digits_only) > 15:
            raise ValueError('Phone number must be between 7 and 15 digits')
        
        return v
    
    @validator('zip_code')
    def validate_zip_code(cls, v):
        """Validate ZIP code format."""
        if v is None:
            return v
        
        # Basic ZIP code validation (US format: 12345 or 12345-6789)
        if not re.match(r'^\d{5}(-\d{4})?$', v):
            # Allow other formats for international addresses
            if len(v) > 20:
                raise ValueError('ZIP code too long')
        
        return v
    
    @validator('website')
    def validate_website(cls, v):
        """Validate website URL format."""
        if v is None:
            return v
        
        # Basic URL validation
        if not v.startswith(('http://', 'https://', 'www.')):
            # Add https:// if no protocol specified
            v = f'https://{v}'
        
        # Basic URL pattern validation
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        
        if not url_pattern.match(v):
            raise ValueError('Invalid website URL format')
        
        return v
    
    @validator('supplier_name')
    def validate_supplier_name(cls, v):
        """Validate supplier name."""
        if v is None:
            return v
        
        if not v.strip():
            raise ValueError('Supplier name cannot be empty')
        
        return v.strip()
    
    @validator('contact_person')
    def validate_contact_person(cls, v):
        """Validate contact person name."""
        if v is None:
            return v
        
        # Basic name validation - letters, spaces, hyphens, apostrophes
        if not re.match(r'^[A-Za-z\s\-\']+$', v):
            raise ValueError('Contact person name contains invalid characters')
        
        return v.strip()
    
    @validator('payment_terms')
    def validate_payment_terms(cls, v):
        """Validate payment terms."""
        if v is None:
            return v
        
        # Common payment terms validation
        common_terms = [
            'net 30', 'net 15', 'net 60', 'net 90', 'due on receipt',
            'cash on delivery', 'prepaid', 'credit card', 'check',
            'wire transfer', 'ach', 'other'
        ]
        
        if v.lower() not in common_terms:
            # Allow custom terms but warn
            pass
        
        return v.strip() if v else v
    
    @validator('email_address')
    def validate_email_address(cls, v):
        """Validate email address format."""
        if v is None:
            return v
        
        # Additional email validation beyond Pydantic's EmailStr
        if len(v) > 254:
            raise ValueError('Email address too long')
        
        return v.lower().strip()
    
    class Config:
        """Pydantic configuration for Supplier model."""
        allow_population_by_field_name = True
        validate_assignment = True
        use_enum_values = True
    
    def to_csv_dict(self) -> dict:
        """
        Convert model to dictionary suitable for CSV export.
        
        Returns:
            dict: Dictionary with CSV column names as keys
        """
        return self.dict(by_alias=True, exclude_none=True)
    
    @classmethod
    def from_csv_dict(cls, data: dict) -> 'Supplier':
        """
        Create Supplier instance from CSV dictionary.
        
        Args:
            data: Dictionary with CSV column names as keys
            
        Returns:
            Supplier: Supplier instance
        """
        return cls(**data)
