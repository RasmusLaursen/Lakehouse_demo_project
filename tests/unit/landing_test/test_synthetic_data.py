"""Unit tests for lakehouse synthetic data generation functions."""

# File: test.py
from datetime import date
import sys
from pathlib import Path

# Add the project root directory to Python path
project_root = Path(__file__).parents[3]  # Gets the directory containing test.py
sys.path.insert(0, str(project_root))

import pytest
from unittest.mock import Mock, patch
from faker import Faker
from typing import List

# Import the classes and functions to test
from src.landing.lakehouse_synthetic_data import LakehouseSyntheticData

from src.helper.synthetic_data_config import (
    CustomerProfile,
    LakehouseProfile,
    SellerProfile,
    LakehouseRental,
    LoyaltyTier,
    PaymentMethod,
    Lakehouses,
    Regions,
)


@pytest.fixture
def sample_loyalty_tiers():
    """Sample loyalty tiers for testing."""
    return [
        LoyaltyTier(loyalty_tier_id=1, loyalty_tier="Bronze"),
        LoyaltyTier(loyalty_tier_id=2, loyalty_tier="Silver"),
        LoyaltyTier(loyalty_tier_id=3, loyalty_tier="Gold"),
        LoyaltyTier(loyalty_tier_id=4, loyalty_tier="Platinum"),
    ]


@pytest.fixture
def sample_payment_methods():
    """Sample payment methods for testing."""
    return [
        PaymentMethod(payment_method_id=1, payment_method="Credit Card"),
        PaymentMethod(payment_method_id=2, payment_method="Debit Card"),
        PaymentMethod(payment_method_id=3, payment_method="PayPal"),
    ]


@pytest.fixture
def sample_meta_lakehouses():
    """Sample lakehouse metadata for testing."""
    return [
        Lakehouses(lakehouse_name_id=1, lakehouse_name="Cozy Cabin"),
        Lakehouses(lakehouse_name_id=2, lakehouse_name="Luxury Villa"),
        Lakehouses(lakehouse_name_id=3, lakehouse_name="Rustic Retreat"),
        Lakehouses(lakehouse_name_id=4, lakehouse_name="Beachfront Bungalow"),
    ]


@pytest.fixture
def sample_meta_regions():
    """Sample region metadata for testing as Regions dataclass instances for consistency."""
    return [
        Regions(region_name_id=1, region_name="North Region"),
        Regions(region_name_id=2, region_name="South Region"),
        Regions(region_name_id=3, region_name="East Region"),
        Regions(region_name_id=4, region_name="West Region"),
    ]


@pytest.fixture
def synthetic_data_generator():
    """LakehouseSyntheticData instance for testing."""
    return LakehouseSyntheticData()


@pytest.fixture
def sample_lakehouse_records(fake, sample_meta_lakehouses, sample_meta_regions):
    """Sample lakehouse profile records for testing dependencies."""
    generator = LakehouseSyntheticData()
    return generator.generate_lakehouse_profile(
        meta_lakehouses=sample_meta_lakehouses,
        meta_regions=sample_meta_regions,
        fake=fake,
        random_number_of_records=False,
    )[
        :10
    ]  # Just first 10 for testing


@pytest.fixture
def sample_customer_records(fake, sample_loyalty_tiers, sample_payment_methods):
    """Sample customer records for testing dependencies."""
    generator = LakehouseSyntheticData()
    return generator.generate_customer(
        loyalty_tiers=sample_loyalty_tiers,
        payment_methods=sample_payment_methods,
        fake=fake,
        random_number_of_records=False,
    )[
        :10
    ]  # Just first 10 for testing


@pytest.fixture
def sample_seller_records(fake, sample_lakehouse_records, sample_meta_regions):
    """Sample seller records for testing dependencies."""
    generator = LakehouseSyntheticData()
    return generator.generate_seller(
        lakehouses_records=sample_lakehouse_records,
        meta_regions=sample_meta_regions,
        fake=fake,
        random_number_of_records=False,
    )


class TestLakehouseSyntheticDataMetadata:
    """Test metadata generation methods."""

    def test_generate_meta_lakehouses(self, synthetic_data_generator):
        """Test lakehouse metadata generation."""
        result = synthetic_data_generator.generate_meta_lakehouses()

        assert isinstance(result, list)
        assert len(result) > 0
        assert all(isinstance(item, Lakehouses) for item in result)

        # Check some expected lakehouse names are in the list
        expected_names = ["Silver Fjord Retreat", "Nordlys Cabin", "Skovsø Lodge"]
        for lakehouse in result:
            assert hasattr(lakehouse, "lakehouse_name_id")
            assert hasattr(lakehouse, "lakehouse_name")
            assert isinstance(lakehouse.lakehouse_name_id, int)
            assert isinstance(lakehouse.lakehouse_name, str)

    def test_generate_meta_regions(self, synthetic_data_generator):
        """Test region metadata generation."""
        result = synthetic_data_generator.generate_meta_regions()

        assert isinstance(result, list)
        assert len(result) > 0
        assert all(isinstance(item, Regions) for item in result)

        # Check some expected regions are in the list
        expected_regions = [
            "Midtjylland",
            "Nordjylland",
            "Syddanmark",
            "Hovedstaden",
            "Sjælland",
        ]
        for region in result:
            assert hasattr(region, "region_name_id")
            assert hasattr(region, "region_name")
            assert region.region_name in expected_regions

    def test_generate_loyalty_tiers(self, synthetic_data_generator):
        """Test loyalty tier generation."""
        result = synthetic_data_generator.generate_loyalty_tiers()

        assert isinstance(result, list)
        assert len(result) > 0
        assert all(isinstance(item, LoyaltyTier) for item in result)

        # Check all tiers have required fields
        for tier in result:
            assert hasattr(tier, "loyalty_tier_id")
            assert hasattr(tier, "loyalty_tier")
            assert isinstance(tier.loyalty_tier_id, int)
            assert isinstance(tier.loyalty_tier, str)

    def test_generate_payment_methods(self, synthetic_data_generator):
        """Test payment method generation."""
        result = synthetic_data_generator.generate_payment_methods()

        assert isinstance(result, list)
        assert len(result) > 0
        assert all(isinstance(item, PaymentMethod) for item in result)

        # Check all payment methods have required fields
        for method in result:
            assert hasattr(method, "payment_method_id")
            assert hasattr(method, "payment_method")
            assert isinstance(method.payment_method_id, int)
            assert isinstance(method.payment_method, str)


class TestGenerateCustomer:
    """Test customer generation functionality."""

    def test_generate_customer_fixed_count(
        self,
        synthetic_data_generator,
        fake,
        sample_loyalty_tiers,
        sample_payment_methods,
    ):
        """Test customer generation with fixed record count."""
        result = synthetic_data_generator.generate_customer(
            loyalty_tiers=sample_loyalty_tiers,
            payment_methods=sample_payment_methods,
            fake=fake,
            random_number_of_records=False,
        )

        assert isinstance(result, list)
        # Fixed count should generate 99,001 customers (range 1000 to 100,001)
        assert len(result) == 99001
        assert all(isinstance(customer, CustomerProfile) for customer in result)

        # Test first few customers for proper structure
        for customer in result[:5]:
            assert hasattr(customer, "customer_id")
            assert hasattr(customer, "name")
            assert hasattr(customer, "email")
            assert hasattr(customer, "phone_number")
            assert hasattr(customer, "address")
            assert hasattr(customer, "city")
            assert hasattr(customer, "postal_code")
            assert hasattr(customer, "country")
            assert hasattr(customer, "loyalty_tier_id")
            assert hasattr(customer, "preferred_payment_method_id")
            assert hasattr(customer, "account_manager")

            # Validate types
            assert isinstance(customer.customer_id, int)
            assert isinstance(customer.name, str)
            assert isinstance(customer.email, str)

            # Check loyalty tier is from provided options
            loyalty_tier_ids = [tier.loyalty_tier_id for tier in sample_loyalty_tiers]
            assert customer.loyalty_tier_id in loyalty_tier_ids

            # Check payment method is from provided options
            payment_method_ids = [
                method.payment_method_id for method in sample_payment_methods
            ]
            assert customer.preferred_payment_method_id in payment_method_ids

            # Check account manager is from expected list
            expected_managers = ["Anders Holm", "Maria Lund", "Thomas Vestergaard"]
            assert customer.account_manager in expected_managers

    def test_generate_customer_random_count(
        self,
        synthetic_data_generator,
        fake,
        sample_loyalty_tiers,
        sample_payment_methods,
    ):
        """Test customer generation with random record count."""
        # Patch random.randint to control the random number
        with patch.object(fake.random, "randint", return_value=150):
            result = synthetic_data_generator.generate_customer(
                loyalty_tiers=sample_loyalty_tiers,
                payment_methods=sample_payment_methods,
                fake=fake,
                random_number_of_records=True,
            )

        assert isinstance(result, list)
        # Should generate 150 customers (mocked random value)
        assert len(result) == 150
        assert all(isinstance(customer, CustomerProfile) for customer in result)

        # Check customer IDs start from 1000
        assert result[0].customer_id == 1000
        assert result[-1].customer_id == 1149  # 1000 + 150 - 1

    def test_generate_customer_empty_loyalty_tiers(
        self, synthetic_data_generator, fake, sample_payment_methods
    ):
        """Test customer generation with empty loyalty tiers."""
        with pytest.raises(ValueError):
            synthetic_data_generator.generate_customer(
                loyalty_tiers=[],
                payment_methods=sample_payment_methods,
                fake=fake,
                random_number_of_records=False,
            )

    def test_generate_customer_empty_payment_methods(
        self, synthetic_data_generator, fake, sample_loyalty_tiers
    ):
        """Test customer generation with empty payment methods."""
        with pytest.raises(ValueError):
            synthetic_data_generator.generate_customer(
                loyalty_tiers=sample_loyalty_tiers,
                payment_methods=[],
                fake=fake,
                random_number_of_records=False,
            )


class TestGenerateLakehouseProfile:
    """Test lakehouse profile generation functionality."""

    def test_generate_lakehouse_profile_fixed_count(
        self,
        synthetic_data_generator,
        fake,
        sample_meta_lakehouses,
        sample_meta_regions,
    ):
        """Test lakehouse profile generation with fixed record count."""
        result = synthetic_data_generator.generate_lakehouse_profile(
            meta_lakehouses=sample_meta_lakehouses,
            meta_regions=sample_meta_regions,
            fake=fake,
            random_number_of_records=False,
        )

        assert isinstance(result, list)
        # Check that we get some records (exact count depends on implementation)
        assert len(result) > 0
        assert all(isinstance(lakehouse, LakehouseProfile) for lakehouse in result)

        # Test first few lakehouses for proper structure
        for lakehouse in result[:5]:
            assert hasattr(lakehouse, "lakehouse_id")
            assert hasattr(lakehouse, "lakehouse_name_id")
            assert hasattr(lakehouse, "region_name_id")
            assert hasattr(lakehouse, "bedrooms")
            assert hasattr(lakehouse, "bathrooms")
            assert hasattr(lakehouse, "max_guests")
            assert hasattr(lakehouse, "amenities")
            assert hasattr(lakehouse, "listing_date")

            # Validate types
            assert isinstance(lakehouse.lakehouse_id, int)
            assert isinstance(lakehouse.lakehouse_name_id, int)
            assert isinstance(lakehouse.region_name_id, int)
            assert isinstance(lakehouse.bedrooms, int)
            assert isinstance(lakehouse.bathrooms, int)
            assert isinstance(lakehouse.max_guests, int)
            assert isinstance(lakehouse.amenities, list)

            # Check region is from provided options
            assert lakehouse.region_name_id in [
                region.region_name_id for region in sample_meta_regions
            ]

            # Check price is reasonable
            assert lakehouse.bedrooms > 0
            assert lakehouse.bathrooms > 0
            assert lakehouse.max_guests > 0

    def test_generate_lakehouse_profile_random_count(
        self,
        synthetic_data_generator,
        fake,
        sample_meta_lakehouses,
        sample_meta_regions,
    ):
        """Test lakehouse profile generation with random record count."""
        with patch.object(fake.random, "randint", return_value=25):
            result = synthetic_data_generator.generate_lakehouse_profile(
                meta_lakehouses=sample_meta_lakehouses,
                meta_regions=sample_meta_regions,
                fake=fake,
                random_number_of_records=True,
            )

        assert isinstance(result, list)
        # Should generate 25 lakehouses (mocked random value)
        assert len(result) == 25
        assert all(isinstance(lakehouse, LakehouseProfile) for lakehouse in result)

    def test_generate_lakehouse_profile_amenities(
        self,
        synthetic_data_generator,
        fake,
        sample_meta_lakehouses,
        sample_meta_regions,
    ):
        """Test that amenities are properly generated and formatted."""
        result = synthetic_data_generator.generate_lakehouse_profile(
            meta_lakehouses=sample_meta_lakehouses,
            meta_regions=sample_meta_regions,
            fake=fake,
            random_number_of_records=False,
        )

        # Check amenities format
        expected_amenities = [
            "WiFi",
            "Fireplace",
            "Kayaks",
            "BBQ Grill",
            "Smart TV",
            "Washer/Dryer",
            "Dock",
            "Bicycles",
        ]

        for lakehouse in result[:5]:
            # Amenities should be a list
            assert isinstance(lakehouse.amenities, list)
            # Should contain some of the expected amenities
            amenity_list = [a.strip() for a in lakehouse.amenities]
            assert len(amenity_list) > 0
            assert all(amenity in expected_amenities for amenity in amenity_list)


class TestGenerateSeller:
    """Test seller generation functionality."""

    def test_generate_seller_fixed_count(
        self,
        synthetic_data_generator,
        fake,
        sample_lakehouse_records,
        sample_meta_regions,
    ):
        """Test seller generation with fixed record count."""
        result = synthetic_data_generator.generate_seller(
            lakehouses_records=sample_lakehouse_records,
            meta_regions=sample_meta_regions,
            fake=fake,
            random_number_of_records=False,
        )

        assert isinstance(result, list)
        # Fixed count should generate 200 sellers
        assert len(result) == 200
        assert all(isinstance(seller, SellerProfile) for seller in result)

        # Test first few sellers for proper structure
        for seller in result[:5]:
            assert hasattr(seller, "seller_id")
            assert hasattr(seller, "name")
            assert hasattr(seller, "email")
            assert hasattr(seller, "phone_number")
            assert hasattr(seller, "region_name_id")
            assert hasattr(seller, "commission_rate")
            assert hasattr(seller, "hire_date")
            assert hasattr(seller, "manager_name")
            assert hasattr(seller, "assigned_lakehouses")

            # Validate types
            assert isinstance(seller.seller_id, int)
            assert isinstance(seller.name, str)
            assert isinstance(seller.email, str)
            assert isinstance(seller.phone_number, str)
            assert isinstance(seller.region_name_id, int)
            assert isinstance(seller.commission_rate, (int, float))

            # Check region is from provided options
            region_ids = [region.region_name_id for region in sample_meta_regions]
            assert seller.region_name_id in region_ids

            # Check manager is from expected list
            expected_managers = [
                "Rasmus Holm",
                "Camilla Vestergaard",
                "Jonas Mikkelsen",
            ]
            assert seller.manager_name in expected_managers

            # Check commission rate is reasonable (typically 0-1)
            assert 0 <= seller.commission_rate <= 1

            # Check assigned lakehouse ID exists in provided lakehouse records
            lakehouse_ids = [lh.lakehouse_id for lh in sample_lakehouse_records]
            assert all(
                lakehouse_id in lakehouse_ids
                for lakehouse_id in seller.assigned_lakehouses
            )

    def test_generate_seller_random_count(
        self,
        synthetic_data_generator,
        fake,
        sample_lakehouse_records,
        sample_meta_regions,
    ):
        """Test seller generation with random record count."""
        with patch.object(fake.random, "randint", return_value=75):
            result = synthetic_data_generator.generate_seller(
                lakehouses_records=sample_lakehouse_records,
                meta_regions=sample_meta_regions,
                fake=fake,
                random_number_of_records=True,
            )

        assert isinstance(result, list)
        # Should generate 75 sellers (mocked random value)
        assert len(result) == 75
        assert all(isinstance(seller, SellerProfile) for seller in result)

        # Check seller IDs are sequential starting from 1
        assert result[0].seller_id == 1
        assert result[-1].seller_id == 75


class TestGenerateLakehouseRentals:
    """Test lakehouse rental generation functionality."""

    def test_generate_lakehouse_rentals_fixed_count(
        self,
        synthetic_data_generator,
        fake,
        sample_lakehouse_records,
        sample_customer_records,
        sample_seller_records,
    ):
        """Test lakehouse rental generation with fixed record count."""
        result = synthetic_data_generator.generate_lakehouse_rentals(
            lakehouses_records=sample_lakehouse_records,
            customers_records=sample_customer_records,
            sellers_records=sample_seller_records,
            fake=fake,
            historic_years=0,
            random_number_of_records=False,
        )

        assert isinstance(result, list)
        # Fixed count should generate 4999 rentals for current period
        assert len(result) == 4999
        assert all(isinstance(rental, LakehouseRental) for rental in result)

        # Test first few rentals for proper structure
        for rental in result[:5]:
            assert hasattr(rental, "rental_id")
            assert hasattr(rental, "lakehouse_id")
            assert hasattr(rental, "customer_id")
            assert hasattr(rental, "seller_id")
            assert hasattr(rental, "check_in_date")
            assert hasattr(rental, "check_out_date")
            assert hasattr(rental, "total_cost")

            # Validate types
            assert isinstance(rental.rental_id, int)
            assert isinstance(rental.lakehouse_id, int)
            assert isinstance(rental.customer_id, int)
            assert isinstance(rental.seller_id, int)
            assert isinstance(rental.total_cost, (int, float))

            # Check IDs reference provided records
            lakehouse_ids = [lh.lakehouse_id for lh in sample_lakehouse_records]
            customer_ids = [c.customer_id for c in sample_customer_records]
            seller_ids = [s.seller_id for s in sample_seller_records]

            assert rental.lakehouse_id in lakehouse_ids
            assert rental.customer_id in customer_ids
            assert rental.seller_id in seller_ids

            # Check reasonable values
            assert rental.total_cost > 0
            assert rental.check_out_date > rental.check_in_date

    def test_generate_lakehouse_rentals_random_count(
        self,
        synthetic_data_generator,
        fake,
        sample_lakehouse_records,
        sample_customer_records,
        sample_seller_records,
    ):
        """Test lakehouse rental generation with random record count."""
        with patch.object(fake.random, "randint", return_value=100):
            result = synthetic_data_generator.generate_lakehouse_rentals(
                lakehouses_records=sample_lakehouse_records,
                customers_records=sample_customer_records,
                sellers_records=sample_seller_records,
                fake=fake,
                historic_years=0,
                random_number_of_records=True,
            )

        assert isinstance(result, list)
        # Should generate 100 rentals (mocked random value)
        assert len(result) == 100
        assert all(isinstance(rental, LakehouseRental) for rental in result)

    def test_generate_lakehouse_rentals_with_historic_years(
        self,
        synthetic_data_generator,
        fake,
        sample_lakehouse_records,
        sample_customer_records,
        sample_seller_records,
    ):
        """Test lakehouse rental generation with historic years."""
        with patch.object(fake.random, "randint", return_value=50):
            result = synthetic_data_generator.generate_lakehouse_rentals(
                lakehouses_records=sample_lakehouse_records,
                customers_records=sample_customer_records,
                sellers_records=sample_seller_records,
                fake=fake,
                historic_years=2,  # Generate for 2 additional years
                random_number_of_records=True,
            )

        assert isinstance(result, list)
        # Should generate rentals for current year + 2 historic years = 3 periods
        # Each period generates 50 rentals (mocked), so total = 150
        assert len(result) == 150
        assert all(isinstance(rental, LakehouseRental) for rental in result)

        # Check that rental IDs are sequential and unique
        rental_ids = [rental.rental_id for rental in result]
        assert len(set(rental_ids)) == len(rental_ids)  # All unique
        assert min(rental_ids) == 1
        assert max(rental_ids) == 150

    def test_generate_lakehouse_rentals_payment_status(
        self,
        synthetic_data_generator,
        fake,
        sample_lakehouse_records,
        sample_customer_records,
        sample_seller_records,
    ):
        """Test that payment status is properly assigned."""
        result = synthetic_data_generator.generate_lakehouse_rentals(
            lakehouses_records=sample_lakehouse_records,
            customers_records=sample_customer_records,
            sellers_records=sample_seller_records,
            fake=fake,
            historic_years=0,
            random_number_of_records=False,
        )

        # Check payment status values are reasonable
        payment_statuses = set(rental.payment_status for rental in result[:100])

        # Payment status should be strings
        assert all(isinstance(status, str) for status in payment_statuses)
        assert len(payment_statuses) > 0

        # Should have some variety in payment statuses
        expected_statuses = ["Paid", "Pending", "Cancelled", "Refunded"]
        assert any(status in expected_statuses for status in payment_statuses)
