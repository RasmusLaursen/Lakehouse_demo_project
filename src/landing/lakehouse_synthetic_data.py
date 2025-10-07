from dataclasses import dataclass
from datetime import timedelta
from faker import Faker
import random
from typing import List
from src.helper import write
from src.helper.synthetic_data_config import LoyaltyTier, PaymentMethod, CustomerProfile, LakehouseProfile, SellerProfile, LakehouseRental, Regions, Lakehouses
from src.helper import logging_helper
from src.helper import databricks_helper

# Initialize logger
logger = logging_helper.get_logger(__name__)
spark = databricks_helper.get_spark()

# Databricks notebook source

lakehouses_global_list = [
    "Silver Fjord Retreat",
    "Nordlys Cabin",
    "Skovsø Lodge",
    "Fjeldhavn Hideaway",
    "Søhjerte Chalet",
    "Himmelbryn Cottage",
    "Elvely Cabin",
    "Granholm Refuge",
    "Vinterlys Villa",
    "Mosemarken Haven",
    "Vandhjem Villa",
    "Blåbølge Bungalow",
    "Søblink Retreat",
    "Havglimt House",
    "Ørnedam Escape",
    "Bølgesang Lodge",
    "Søklare Cabin",
    "Lysfjord Landing",
    "Tågesø Terrace",
    "Kystkilde Cottage",
    "Hyggely Cabin",
    "Stjernely Retreat",
    "Lykkeholm Lodge",
    "Mælkevejen Manor",
    "Rosenlund Refuge",
    "Solsiden Chalet",
    "Nattergal Nest",
    "Skovfred Villa",
    "Hjerterum Haven",
    "Drømmebo Cottage",
    "Fjordlys Cabin",
    "Tindely Retreat",
    "Søglimt Lodge",
    "Granly Villa",
    "Elvesang Chalet",
    "Skovhjerte Haven",
    "Birkely Bungalow",
    "Vandkilde Refuge",
    "Stillefjord House",
    "Søfred Cabin",
    "Fjeldsø Retreat",
    "Mosehavn Lodge",
    "Solblink Villa",
    "Tågedal Cottage",
    "Søhavn Chalet",
    "Nordkyst Nest",
    "Skovsøen Hideaway",
    "Lyseng Cabin",
    "Vinterhavn Retreat",
    "Søklint Lodge",
    "Havfred Villa",
    "Stjernedal Cottage",
    "Granlund Chalet",
    "Søholm Refuge",
    "Fjordhjerte Cabin",
    "Elvedal Retreat",
    "Tindesø Lodge",
    "Mosefred Villa",
    "Skovklint Cottage",
    "Søeng Chalet",
    "Nordlys Nest",
    "Sømarken Haven",
    "Birkesø Retreat",
    "Solhavn Lodge",
    "Tågesøen Villa",
    "Fjordblink Cottage",
    "Skoveng Chalet",
    "Søglans Refuge",
    "Granensø Cabin",
    "Elvesø Retreat",
    "Stillehavn Lodge",
    "Søhjerte Villa",
    "Nordeng Cottage",
    "Skovfred Chalet",
    "Søklare Haven",
    "Fjordeng Cabin",
    "Tindehavn Retreat",
    "Moseklint Lodge",
    "Solfred Villa",
    "Søblink Cottage",
    "Haveng Chalet",
    "Stjernely Haven",
    "Granensøen Cabin",
    "Elveglimt Retreat",
    "Tågedal Lodge",
    "Søfred Villa",
    "Nordhavn Cottage",
    "Skovsø Retreat",
    "Søhavn Lodge",
    "Fjordmarken Chalet",
    "Elvesøen Cabin",
    "Tindely Haven",
    "Mosehjerte Retreat",
    "Soleng Lodge",
    "Søglimt Villa",
    "Havklint Cottage",
    "Stillemarken Chalet",
    "Granly Haven",
    "Elvedal Cabin",
    "Tågesø Retreat",
    "Søeng Lodge",
    "Nordmarken Villa",
    "Skovhavn Cottage",
    "Søklint Chalet",
    "Fjordhjerte Haven",
    "Elvesang Cabin",
    "Tindesø Retreat",
    "Mosefred Lodge",
    "Solblink Villa",
    "Søhjerte Cottage",
    "Havfred Chalet",
    "Stjernedal Haven",
    "Granlund Cabin",
    "Søholm Retreat",
    "Fjordlys Lodge",
    "Elvedal Villa",
    "Tindely Cottage",
    "Mosehavn Chalet",
    "Solfred Haven",
    "Søklare Cabin",
    "Nordlys Retreat",
    "Skovsøen Lodge",
    "Sømarken Villa",
    "Fjordeng Cottage",
    "Elvesø Chalet",
    "Tågedal Haven",
    "Moseklint Cabin",
    "Solhavn Retreat",
    "Søglans Lodge",
    "Haveng Villa",
    "Stillehavn Cottage",
    "Granensø Chalet",
    "Elveglimt Haven",
    "Tindehavn Cabin",
    "Søfred Retreat",
    "Nordeng Lodge",
    "Skovfred Villa",
    "Søhavn Cottage",
    "Fjordmarken Chalet",
    "Elvesøen Haven",
    "Tindely Cabin",
    "Mosehjerte Lodge",
    "Soleng Villa",
    "Søglimt Cottage",
    "Havklint Chalet",
    "Stillemarken Haven",
    "Granly Cabin",
    "Elvedal Retreat",
    "Tågesø Lodge",
    "Søeng Villa",
    "Nordmarken Cottage",
    "Skovhavn Chalet",
    "Søklint Haven",
    "Fjordhjerte Cabin",
    "Elvesang Retreat",
    "Tindesø Lodge",
    "Mosefred Villa",
    "Solblink Cottage",
    "Søhjerte Chalet",
    "Havfred Haven",
    "Stjernedal Cabin",
    "Granlund Retreat",
    "Søholm Lodge",
    "Fjordlys Villa",
    "Elvedal Cottage",
    "Tindely Chalet",
    "Mosehavn Haven",
    "Solfred Cabin",
    "Søklare Retreat",
    "Nordlys Lodge",
    "Skovsøen Villa",
    "Sømarken Cottage",
    "Fjordeng Chalet",
    "Elvesø Haven",
    "Tågedal Cabin",
    "Moseklint Retreat",
    "Solhavn Lodge",
    "Søglans Villa",
    "Haveng Cottage",
    "Stillehavn Chalet",
    "Granensø Haven",
    "Elveglimt Cabin",
    "Tindehavn Retreat",
    "Søfred Lodge",
    "Nordeng Villa",
]

class LakehouseSyntheticData:

    def __init__(self) -> None:
        self.meta_lakehouses = None
        self.meta_regions = None

        Faker.seed(42)
        self.faker = Faker()

        self.generate_loyalty_tiers_records = self.generate_loyalty_tiers()
        self.generate_payment_methods_records = self.generate_payment_methods()         
        self.meta_lakehouses = self.generate_meta_lakehouses()
        self.meta_regions = self.generate_meta_regions()

        self.lakehouses_records = self.generate_lakehouse_profile(
            meta_lakehouses=self.meta_lakehouses, meta_regions=self.meta_regions, fake=self.faker
        )

        self.customer_records = self.generate_customer(
            fake=self.faker,
            loyalty_tiers=self.generate_loyalty_tiers_records,
            payment_methods=self.generate_payment_methods_records
        )

        self.seller_records = self.generate_seller(
            lakehouses_records=self.lakehouses_records,
            meta_regions=self.meta_regions,
            fake=self.faker,
        )

        self.lakehouse_rentals_records = self.generate_lakehouse_rentals(
            lakehouses_records=self.lakehouses_records,
            customers_records=self.customer_records,
            sellers_records=self.seller_records,
            fake=self.faker,
        )

    def get_all_records(self) -> dict:
        """
        Returns all generated records as a dictionary.
        """
        return {
            "lakehouses": self.lakehouses_records,
            "customer": self.customer_records,
            "seller": self.seller_records,
            "lakehouse_rentals": self.lakehouse_rentals_records,
            "loyalty_tiers": self.generate_loyalty_tiers_records,
            "payment_method": self.generate_payment_methods_records,
            "meta_regions": self.meta_regions,
            "meta_lakehouses": self.meta_lakehouses,
        }

    def lakehouse_generate_data(
        self,
        landing_catalog: str,
        landing_schema: str,
        records: dict,
    ):
        entities = {
            "lakehouse_rentals": records["lakehouse_rentals"],
            "customer": records["customer"],
            "seller": records["seller"],
            "lakehouse": records["lakehouses"],
            "loyalty_tier": records["loyalty_tiers"],
            "payment_method": records["payment_method"],
            "meta_region": records["meta_regions"],
            "meta_lakehouses": records["meta_lakehouses"],
        }

        for entity_name, entity_records in entities.items():
            logger.info(
                f"Creating volume for {landing_catalog}.{landing_schema}.{entity_name}..."
            )
            df_entity = spark.createDataFrame(entity_records)

            write.write_volume(
                target_catalog=landing_catalog,
                target_schema=landing_schema,
                target_name=entity_name,
                source_dataframe=df_entity,
                mode="overwrite",
                file_format="parquet",
            )

            logger.info(f"{entity_name.capitalize()} data written successfully.")
        return entities["lakehouse_rentals"]

    def generate_meta_lakehouses(self):
        return [Lakehouses(i, name) for i, name in enumerate(lakehouses_global_list, start=1)]

    def generate_meta_regions(self):
        return [
            Regions(1, "Midtjylland"),
            Regions(2, "Sjælland"),
            Regions(3, "Nordjylland"),
            Regions(4, "Syddanmark"),
            Regions(5, "Hovedstaden"),
        ]
    
    def generate_loyalty_tiers(self):
        return [
            LoyaltyTier(1, "Bronze"),
            LoyaltyTier(2, "Silver"),
            LoyaltyTier(3, "Gold"),
            LoyaltyTier(4, "Platinum"),
        ]

    def generate_payment_methods(self):
        return [
            PaymentMethod(1, "Credit Card"),
            PaymentMethod(2, "MobilePay"),
            PaymentMethod(3, "Bank Transfer"),
        ]

    def generate_customer(self, loyalty_tiers: list, payment_methods: list, fake: Faker):
        account_managers = ["Anders Holm", "Maria Lund", "Thomas Vestergaard"]
        customers = []
        for i in range(1, 1001):
            birth_date = fake.date_of_birth(minimum_age=18, maximum_age=75)
            reg_date = fake.date_between(start_date="-5y", end_date="-1M")
            last_purchase = fake.date_between(start_date=reg_date, end_date="today")
            total_spend = round(random.uniform(500, 50000), 2)
            num_purchases = random.randint(1, 100)

            customer = CustomerProfile(
                customer_id=i,
                name=fake.name(),
                email=fake.email(),
                phone_number=fake.phone_number(),
                birth_date=birth_date,
                gender=random.choice(["Male", "Female"]),
                address=fake.street_address(),
                city=fake.city(),
                postal_code=fake.postcode(),
                country="Denmark",
                registration_date=reg_date,
                loyalty_tier_id=random.choice(loyalty_tiers).loyalty_tier_id,
                preferred_payment_method_id=random.choice(payment_methods).payment_method_id,
                account_manager=random.choice(account_managers),
                is_subscribed_to_newsletter=random.choice([True, False]),
                last_purchase_date=last_purchase,
                total_spend=total_spend,
                number_of_purchases=num_purchases,
            )
            customers.append(customer)

        return customers

    def generate_lakehouse_profile(
        self, meta_lakehouses: List[str], meta_regions: List[str], fake: Faker
    ):
        amenity_pool = [
            "WiFi",
            "Fireplace",
            "Kayaks",
            "BBQ Grill",
            "Smart TV",
            "Washer/Dryer",
            "Dock",
            "Bicycles",
        ]

        lakehouse_records = []
        for i in range(1, 201):  # 200 lakehouses
            lakehouse_name_id = random.choice(meta_lakehouses).lakehouse_name_id
            location = fake.city()
            region = random.choice(meta_regions)
            address = fake.street_address()
            postal = fake.postcode()
            bedrooms = random.randint(1, 6)
            bathrooms = random.randint(1, 3)
            guests = bedrooms * random.randint(2, 3)
            rate = round(random.uniform(850, 2500), 2)
            pet_friendly = random.choice([True, False])
            sauna = random.choice([True, False])
            hot_tub = random.choice([True, False])
            lake_view = random.choice([True, False])
            amenities = random.sample(amenity_pool, random.randint(3, 6))
            owner_name = fake.name()
            owner_email = fake.email()
            listing_date = fake.date_between(start_date="-3y", end_date="today")
            rating = round(random.uniform(3.5, 5.0), 1)
            is_active = random.choice([True, True, False])  # mostly active

            lakehouse = LakehouseProfile(
                lakehouse_id=i,
                lakehouse_name_id=lakehouse_name_id,
                location=location,
                region_name_id=region.region_name_id,
                address=address,
                postal_code=postal,
                country="Denmark",
                bedrooms=bedrooms,
                bathrooms=bathrooms,
                max_guests=guests,
                nightly_rate=rate,
                is_pet_friendly=pet_friendly,
                has_sauna=sauna,
                has_hot_tub=hot_tub,
                has_lake_view=lake_view,
                amenities=amenities,
                owner_name=owner_name,
                owner_email=owner_email,
                listing_date=listing_date,
                rating=rating,
                is_active=is_active,
            )
            lakehouse_records.append(lakehouse)

        return lakehouse_records

    def generate_seller(self, lakehouses_records, meta_regions: List[str], fake: Faker):
        managers = ["Rasmus Holm", "Camilla Vestergaard", "Jonas Mikkelsen"]

        seller_records = []
        for i in range(1, 201):  # 200 sellers
            hire_date = fake.date_between(start_date="-5y", end_date="-6M")
            last_booking = fake.date_between(start_date=hire_date, end_date="today")
            total_sales = round(random.uniform(100000, 1500000), 2)
            bookings = random.randint(50, 500)
            commission = round(random.uniform(0.05, 0.15), 2)
            target = round(random.uniform(50000, 200000), 2)
            assigned_lakehouses = [
                lakehouse.lakehouse_name_id
                for lakehouse in random.choices(
                    lakehouses_records, k=random.randint(1, 3)
                )
            ]

            seller = SellerProfile(
                seller_id=i,
                name=fake.name(),
                email=fake.email(),
                phone_number=fake.phone_number(),
                hire_date=hire_date,
                region_name_id=random.choice(meta_regions).region_name_id,
                assigned_lakehouses=assigned_lakehouses,
                total_sales=total_sales,
                number_of_bookings=bookings,
                commission_rate=commission,
                monthly_target=target,
                is_active=random.choice([True, True, False]),  # mostly active
                last_booking_date=last_booking,
                performance_rating=round(random.uniform(3.0, 5.0), 1),
                manager_name=random.choice(managers),
                preferred_contact_method=random.choice(["Email", "Phone", "Slack"]),
            )
            seller_records.append(seller)
        return seller_records

    def generate_lakehouse_rentals(
        self, lakehouses_records, customers_records, sellers_records, fake: Faker
    ):
        lakehouse_rental_records = []
        for i in range(1, 5000):
            check_in = fake.date_between(start_date="-6M", end_date="today")
            stay_length = random.randint(2, 7)
            check_out = check_in + timedelta(days=stay_length)
            nightly_rate = random.randint(900, 1800)
            total_cost = nightly_rate * stay_length
            tax_amount = round(total_cost * 0.125, 2)
            total_with_tax = round(total_cost + tax_amount, 2)
            order_date = fake.date_between(start_date="-6M", end_date=check_in)
            customer = random.choice(customers_records)
            seller = random.choice(sellers_records)
            lakehouse = random.choice(lakehouses_records)

            rental = LakehouseRental(
                rental_id=i,
                seller_id=seller.seller_id,
                customer_id=customer.customer_id,
                lakehouse_id=lakehouse.lakehouse_id,
                customer_name=customer.name,
                email=customer.email,
                check_in_date=check_in,
                check_out_date=check_out,
                lakehouse_name_id=lakehouse.lakehouse_name_id,
                location=fake.city(),
                bedrooms=random.randint(1, 5),
                nightly_rate=nightly_rate,
                total_cost=total_cost,
                is_pet_friendly=random.choice([True, False]),
                rating=round(random.uniform(3.0, 5.0), 1),
                order_date=order_date,
                seller_name=seller.name,
                seller_email=seller.email,
                account_manager=random.choice(
                    ["Anders Holm", "Maria Lund", "Thomas Vestergaard"]
                ),
                payment_method=random.choice(
                    ["Credit Card", "MobilePay", "Bank Transfer"]
                ),
                transaction_id=f"TXN-{order_date.strftime('%Y%m%d')}-{random.randint(10000,99999)}",
                discount_code=random.choice(["SUMMER25", "WELCOME10", "", "", ""]),
                booking_channel=random.choice(["Website", "Mobile App", "Phone"]),
                currency="DKK",
                tax_amount=tax_amount,
                total_cost_with_tax=total_with_tax,
            )
            lakehouse_rental_records.append(rental)

        return lakehouse_rental_records
