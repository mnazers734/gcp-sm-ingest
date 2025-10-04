"""
Customer Pydantic model for ETL data ingestion.

This model represents customer data from third-party automotive shop management systems.
All fields are optional to handle various data completeness scenarios during migration.
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, Field, validator, EmailStr
import re


class Customer(BaseModel):
    """
    Customer model for automotive shop management system data ingestion.
    
    Represents customer information including contact details, addresses,
    authorizer information, and business preferences.
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
    external_customer_id: Optional[str] = Field(
        None,
        alias="externalCustomerId",
        max_length=50, 
        description="External customer identifier from source system"
    )
    
    # Customer personal information
    first_name: Optional[str] = Field(
        None,
        alias="firstName",
        max_length=100,
        description="Customer's first name"
    )
    last_name: Optional[str] = Field(
        None,
        alias="lastName", 
        max_length=100,
        description="Customer's last name"
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
        description="Secondary street address (apartment, suite, etc.)"
    )
    country: Optional[str] = Field(
        None,
        max_length=100,
        description="Country"
    )
    state: Optional[str] = Field(
        None,
        max_length=100,
        description="State or province"
    )
    city: Optional[str] = Field(
        None,
        max_length=100,
        description="City"
    )
    zip_code: Optional[str] = Field(
        None,
        alias="zipCode",
        max_length=20,
        description="ZIP or postal code"
    )
    
    # Contact information
    contact_cell: Optional[str] = Field(
        None,
        alias="contactCell",
        max_length=20,
        description="Mobile phone number"
    )
    contact_work: Optional[str] = Field(
        None,
        alias="contactWork",
        max_length=20,
        description="Work phone number"
    )
    contact_home: Optional[str] = Field(
        None,
        alias="contactHome",
        max_length=20,
        description="Home phone number"
    )
    contact_email: Optional[EmailStr] = Field(
        None,
        alias="contactEmail",
        description="Email address"
    )
    preferred_contact: Optional[str] = Field(
        None,
        alias="preferredContact",
        max_length=20,
        description="Preferred contact method"
    )
    
    # Authorizer information
    authorizer_first_name: Optional[str] = Field(
        None,
        alias="authorizerFirstName",
        max_length=100,
        description="First name of person authorized to approve work"
    )
    authorizer_last_name: Optional[str] = Field(
        None,
        alias="authorizerLastName",
        max_length=100,
        description="Last name of person authorized to approve work"
    )
    authorizer_phone: Optional[str] = Field(
        None,
        alias="authorizerPhone",
        max_length=20,
        description="Phone number of authorized person"
    )
    
    # Business preferences
    default_labor_rate: Optional[Decimal] = Field(
        None,
        alias="defaultLaborRate",
        ge=0,
        description="Default hourly labor rate for this customer"
    )
    do_not_charge_tax: Optional[bool] = Field(
        None,
        alias="doNotChargeTax",
        description="Whether to exclude tax charges for this customer"
    )
    
    # ETL metadata
    load_id: Optional[str] = Field(
        None,
        alias="load_id",
        max_length=100,
        description="ETL load identifier for tracking and audit purposes"
    )
    
    @validator('contact_cell', 'contact_work', 'contact_home', 'authorizer_phone')
    def validate_phone_numbers(cls, v):
        """Validate phone number format."""
        if v is None:
            return v
        
        # Remove all non-digit characters for validation
        digits_only = re.sub(r'\D', '', v)
        
        # Check if it's a valid phone number (7-15 digits)
        if len(digits_only) < 7 or len(digits_only) > 15:
            raise ValueError('Phone number must be between 7 and 15 digits')
        
        return v
    
    @validator('preferred_contact')
    def validate_preferred_contact(cls, v):
        """Validate preferred contact method."""
        if v is None:
            return v
        
        valid_methods = ['cell', 'work', 'home', 'email']
        if v.lower() not in valid_methods:
            raise ValueError(f'Preferred contact must be one of: {", ".join(valid_methods)}')
        
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
    
    class Config:
        """Pydantic configuration for Customer model."""
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
    def from_csv_dict(cls, data: dict) -> 'Customer':
        """
        Create Customer instance from CSV dictionary.
        
        Args:
            data: Dictionary with CSV column names as keys
            
        Returns:
            Customer: Customer instance
        """
        return cls(**data)
