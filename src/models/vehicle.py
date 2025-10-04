"""
Vehicle Pydantic model for ETL data ingestion.

This model represents vehicle data from third-party automotive shop management systems.
Includes ACES (Auto Care Association) standard fields for vehicle identification.
"""

from typing import Optional
from pydantic import BaseModel, Field, validator
import re


class Vehicle(BaseModel):
    """
    Vehicle model for automotive shop management system data ingestion.
    
    Represents vehicle information including identification, specifications,
    and ACES (Auto Care Association) standard fields for parts compatibility.
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
    external_vehicle_id: Optional[str] = Field(
        None,
        alias="externalVehicleId",
        max_length=50,
        description="External vehicle identifier from source system"
    )
    external_customer_id: Optional[str] = Field(
        None,
        alias="externalCustomerId",
        max_length=50,
        description="External customer identifier from source system"
    )
    
    # Basic vehicle information
    year: Optional[int] = Field(
        None,
        ge=1900,
        le=2030,
        description="Vehicle model year"
    )
    make: Optional[str] = Field(
        None,
        max_length=100,
        description="Vehicle manufacturer"
    )
    model: Optional[str] = Field(
        None,
        max_length=100,
        description="Vehicle model"
    )
    engine: Optional[str] = Field(
        None,
        max_length=200,
        description="Engine specification"
    )
    vin: Optional[str] = Field(
        None,
        max_length=17,
        description="Vehicle Identification Number"
    )
    
    # License information
    license_plate: Optional[str] = Field(
        None,
        alias="licensePlate",
        max_length=20,
        description="License plate number"
    )
    license_state: Optional[str] = Field(
        None,
        alias="licenseState",
        max_length=50,
        description="State where vehicle is licensed"
    )
    license_country: Optional[str] = Field(
        None,
        alias="licenseCountry",
        max_length=50,
        description="Country where vehicle is licensed"
    )
    
    # Odometer information
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
    fuel_type: Optional[str] = Field(
        None,
        alias="fuelType",
        max_length=50,
        description="Vehicle fuel type"
    )
    
    # ACES (Auto Care Association) standard fields
    aces_vehicle_id: Optional[str] = Field(
        None,
        alias="acesVehicleId",
        max_length=50,
        description="ACES vehicle identifier"
    )
    aces_engine_id: Optional[str] = Field(
        None,
        alias="acesEngineId",
        max_length=50,
        description="ACES engine identifier"
    )
    aces_veh_to_eng_cfg_id: Optional[str] = Field(
        None,
        alias="acesVehToEngCfgId",
        max_length=50,
        description="ACES vehicle to engine configuration identifier"
    )
    aces_base_vehicle_id: Optional[str] = Field(
        None,
        alias="acesBaseVehicleId",
        max_length=50,
        description="ACES base vehicle identifier"
    )
    aces_transmission: Optional[str] = Field(
        None,
        alias="acesTransmission",
        max_length=100,
        description="ACES transmission specification"
    )
    aces_body_config: Optional[str] = Field(
        None,
        alias="acesBodyConfig",
        max_length=100,
        description="ACES body configuration"
    )
    aces_description: Optional[str] = Field(
        None,
        alias="acesDescription",
        max_length=500,
        description="ACES vehicle description"
    )
    aces_trim_desc: Optional[str] = Field(
        None,
        alias="acesTrimDesc",
        max_length=200,
        description="ACES trim description"
    )
    
    # Non-ACES fields for legacy data
    non_aces_body_type_name: Optional[str] = Field(
        None,
        alias="nonAcesBodyTypeName",
        max_length=100,
        description="Non-ACES body type name"
    )
    non_aces_fuel_type_name: Optional[str] = Field(
        None,
        alias="nonAcesFuelTypeName",
        max_length=100,
        description="Non-ACES fuel type name"
    )
    non_aces_trim_desc: Optional[str] = Field(
        None,
        alias="nonAcesTrimDesc",
        max_length=200,
        description="Non-ACES trim description"
    )
    non_aces_transmission_code: Optional[str] = Field(
        None,
        alias="nonAcesTransmissionCode",
        max_length=50,
        description="Non-ACES transmission code"
    )
    
    # ETL metadata
    load_id: Optional[str] = Field(
        None,
        alias="load_id",
        max_length=100,
        description="ETL load identifier for tracking and audit purposes"
    )
    
    @validator('vin')
    def validate_vin(cls, v):
        """Validate VIN format."""
        if v is None:
            return v
        
        # Remove spaces and convert to uppercase
        vin = v.strip().upper()
        
        # VIN must be exactly 17 characters
        if len(vin) != 17:
            raise ValueError('VIN must be exactly 17 characters')
        
        # VIN cannot contain I, O, or Q
        if any(char in 'IOQ' for char in vin):
            raise ValueError('VIN cannot contain letters I, O, or Q')
        
        return vin
    
    @validator('year')
    def validate_year(cls, v):
        """Validate vehicle year."""
        if v is None:
            return v
        
        current_year = 2025
        if v < 1900 or v > current_year + 2:
            raise ValueError(f'Vehicle year must be between 1900 and {current_year + 2}')
        
        return v
    
    @validator('fuel_type')
    def validate_fuel_type(cls, v):
        """Validate fuel type."""
        if v is None:
            return v
        
        valid_fuel_types = [
            'gasoline', 'diesel', 'electric', 'hybrid', 'lpg', 'cng', 
            'ethanol', 'biodiesel', 'hydrogen', 'other'
        ]
        
        if v.lower() not in valid_fuel_types:
            # Allow custom fuel types but warn
            pass
        
        return v
    
    @validator('license_plate')
    def validate_license_plate(cls, v):
        """Validate license plate format."""
        if v is None:
            return v
        
        # Basic validation - alphanumeric with some special characters
        if not re.match(r'^[A-Za-z0-9\s\-\.]+$', v):
            raise ValueError('License plate contains invalid characters')
        
        return v.strip()
    
    class Config:
        """Pydantic configuration for Vehicle model."""
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
    def from_csv_dict(cls, data: dict) -> 'Vehicle':
        """
        Create Vehicle instance from CSV dictionary.
        
        Args:
            data: Dictionary with CSV column names as keys
            
        Returns:
            Vehicle: Vehicle instance
        """
        return cls(**data)
