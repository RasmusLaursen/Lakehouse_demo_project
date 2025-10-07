from dataclasses import dataclass
from datetime import date

@dataclass
class Lakehouses:
    lakehouse_name_id:int
    lakehouse_name:str

@dataclass
class Regions:
    region_name_id: int
    region_name: str

@dataclass
class LoyaltyTier:
    loyalty_tier_id: int
    loyalty_tier: str

@dataclass
class PaymentMethod:
    payment_method_id: int
    payment_method: str

@dataclass
class CustomerProfile:
    customer_id: int
    name: str
    email: str
    phone_number: str
    birth_date: date
    gender: str
    address: str
    city: str
    postal_code: str    
    country: str
    registration_date: date
    loyalty_tier_id: int
    preferred_payment_method_id: int
    account_manager: str
    is_subscribed_to_newsletter: bool
    last_purchase_date: date
    total_spend: float
    number_of_purchases: int

@dataclass
class LakehouseProfile:
    lakehouse_id: int
    lakehouse_name_id: int
    location: str
    region_name_id: int
    address: str
    postal_code: str
    country: str
    bedrooms: int
    bathrooms: int
    max_guests: int
    nightly_rate: float
    is_pet_friendly: bool
    has_sauna: bool
    has_hot_tub: bool
    has_lake_view: bool
    amenities: list
    owner_name: str
    owner_email: str
    listing_date: date
    rating: float
    is_active: bool    

@dataclass
class SellerProfile:
    seller_id: int
    name: str
    email: str
    phone_number: str
    hire_date: date
    region_name_id: int
    assigned_lakehouses: list
    total_sales: float
    number_of_bookings: int
    commission_rate: float
    monthly_target: float
    is_active: bool
    last_booking_date: date
    performance_rating: float
    manager_name: str
    preferred_contact_method: str    

@dataclass
class LakehouseRental:
    rental_id: int
    seller_id: int
    customer_id: int
    lakehouse_id: int
    customer_name: str
    email: str
    check_in_date: date
    check_out_date: date
    lakehouse_name_id: int
    location: str
    bedrooms: int
    nightly_rate: float
    total_cost: float
    is_pet_friendly: bool
    rating: float
    order_date: date
    seller_name: str
    seller_email: str
    account_manager: str
    payment_method: str
    transaction_id: str
    discount_code: str
    booking_channel: str
    currency: str
    tax_amount: float
    total_cost_with_tax: float    