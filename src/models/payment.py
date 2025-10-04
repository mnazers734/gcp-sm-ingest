"""
Payment Pydantic model for ETL data ingestion.

This model represents payment data from third-party automotive shop management systems.
Includes payment method, amount, and status information.
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, Field, validator
from enum import Enum


class PaymentMethod(str, Enum):
    """Payment method enumeration."""
    CASH = "cash"
    CHECK = "check"
    CREDIT_CARD = "credit_card"
    DEBIT_CARD = "debit_card"
    ACH = "ach"
    WIRE_TRANSFER = "wire_transfer"
    PAYPAL = "paypal"
    SQUARE = "square"
    STRIPE = "stripe"
    OTHER = "other"


class PaymentMethodType(str, Enum):
    """Payment method type enumeration."""
    CARD = "card"
    BANK = "bank"
    CASH = "cash"
    CHECK = "check"
    DIGITAL = "digital"
    OTHER = "other"


class PaymentStatus(str, Enum):
    """Payment status enumeration."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"
    PARTIALLY_REFUNDED = "partially_refunded"


class Payment(BaseModel):
    """
    Payment model for automotive shop management system data ingestion.
    
    Represents payment transactions including method, amount, status,
    and relationship to invoices/documents.
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
    external_payment_id: Optional[str] = Field(
        None,
        alias="externalPaymentId",
        max_length=50,
        description="External payment identifier from source system"
    )
    external_document_id: Optional[str] = Field(
        None,
        alias="externalDocumentId",
        max_length=50,
        description="External document identifier from source system"
    )
    
    # Payment details
    payment_date: Optional[datetime] = Field(
        None,
        alias="paymentDate",
        description="Date and time of the payment"
    )
    payment_amount: Optional[Decimal] = Field(
        None,
        alias="paymentAmount",
        gt=0,
        description="Payment amount (must be positive)"
    )
    payment_method: Optional[PaymentMethod] = Field(
        None,
        alias="paymentMethod",
        description="Method used for payment"
    )
    payment_method_type: Optional[PaymentMethodType] = Field(
        None,
        alias="paymentMethodType",
        description="Type/category of payment method"
    )
    payment_reference_no: Optional[str] = Field(
        None,
        alias="paymentReferenceNo",
        max_length=100,
        description="Payment reference number or transaction ID"
    )
    payment_notes: Optional[str] = Field(
        None,
        alias="paymentNotes",
        max_length=1000,
        description="Additional notes about the payment"
    )
    payment_status: Optional[PaymentStatus] = Field(
        None,
        alias="paymentStatus",
        description="Current status of the payment"
    )
    
    # ETL metadata
    load_id: Optional[str] = Field(
        None,
        alias="load_id",
        max_length=100,
        description="ETL load identifier for tracking and audit purposes"
    )
    
    @validator('payment_method')
    def validate_payment_method(cls, v):
        """Validate payment method."""
        if v is None:
            return v
        
        # Allow string values and convert to enum
        if isinstance(v, str):
            try:
                return PaymentMethod(v.lower())
            except ValueError:
                raise ValueError(f'Invalid payment method: {v}. Must be one of: {[m.value for m in PaymentMethod]}')
        
        return v
    
    @validator('payment_method_type')
    def validate_payment_method_type(cls, v):
        """Validate payment method type."""
        if v is None:
            return v
        
        # Allow string values and convert to enum
        if isinstance(v, str):
            try:
                return PaymentMethodType(v.lower())
            except ValueError:
                raise ValueError(f'Invalid payment method type: {v}. Must be one of: {[t.value for t in PaymentMethodType]}')
        
        return v
    
    @validator('payment_status')
    def validate_payment_status(cls, v):
        """Validate payment status."""
        if v is None:
            return v
        
        # Allow string values and convert to enum
        if isinstance(v, str):
            try:
                return PaymentStatus(v.lower())
            except ValueError:
                raise ValueError(f'Invalid payment status: {v}. Must be one of: {[s.value for s in PaymentStatus]}')
        
        return v
    
    @validator('payment_date')
    def validate_payment_date(cls, v):
        """Validate payment date."""
        if v is None:
            return v
        
        # Check if date is not too far in the future
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        if v > now:
            raise ValueError('Payment date cannot be in the future')
        
        # Check if date is not too far in the past (before 1900)
        if v.year < 1900:
            raise ValueError('Payment date cannot be before 1900')
        
        return v
    
    @validator('payment_amount')
    def validate_payment_amount(cls, v):
        """Validate payment amount."""
        if v is None:
            return v
        
        if v <= 0:
            raise ValueError('Payment amount must be greater than 0')
        
        # Check for reasonable maximum amount (e.g., $1M)
        if v > Decimal('1000000'):
            raise ValueError('Payment amount seems unreasonably high')
        
        return v
    
    @validator('payment_reference_no')
    def validate_payment_reference_no(cls, v):
        """Validate payment reference number."""
        if v is None:
            return v
        
        # Basic validation - alphanumeric with some special characters
        if not v.strip():
            raise ValueError('Payment reference number cannot be empty')
        
        return v.strip()
    
    @validator('payment_notes')
    def validate_payment_notes(cls, v):
        """Validate payment notes."""
        if v is None:
            return v
        
        # Clean up whitespace
        return v.strip() if v else v
    
    class Config:
        """Pydantic configuration for Payment model."""
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
    def from_csv_dict(cls, data: dict) -> 'Payment':
        """
        Create Payment instance from CSV dictionary.
        
        Args:
            data: Dictionary with CSV column names as keys
            
        Returns:
            Payment: Payment instance
        """
        return cls(**data)
