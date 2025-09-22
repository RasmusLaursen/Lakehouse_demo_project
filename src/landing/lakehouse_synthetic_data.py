from dataclasses import dataclass
from datetime import date, timedelta
from faker import Faker
import random
from typing import List

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

regions_global_list = [
    "Midtjylland",
    "Sjælland",
    "Nordjylland",
    "Syddanmark",
    "Hovedstaden",
]


class lakehouse_synthetic_data:

    def __init__(self) -> None:
        self.lakehouses_list = lakehouses_global_list
        self.regions = regions_global_list

        Faker.seed(42)
        self.faker = Faker()

        # Initialize internal data structures
        self.lakehouses_records = None
        self.customer_records = None
        self.seller_records = None
        self.lakehouse_rentals_records = None

    def generate_lakehouse_synthetic_data(self):
        self.lakehouses_records = self.lakehouse_profile(
            lakehouses_list=self.lakehouses_list, regions=self.regions, fake=self.faker
        )

        self.customer_records = self.generate_customer(fake=self.faker)

        self.seller_records = self.generate_seller(
            lakehouses_records=self.lakehouses_records,
            regions=self.regions,
            fake=self.faker,
        )

        self.lakehouse_rentals_records = self.generate_lakehouse_rentals(
            lakehouses_records=self.lakehouses_records,
            customers_records=self.customer_records,
            sellers_records=self.seller_records,
            fake=self.faker,
        )

        return {
            "lakehouses": self.lakehouses_records,
            "customers": self.customer_records,
            "sellers": self.seller_records,
            "lakehouse_rentals": self.lakehouse_rentals_records,
        }

    def generate_data(
        landing_catalog: str,
        landing_schema: str,
        records: dict,
    ):
        entities = {
            "lakehouse_rentals": records["lakehouse_rentals"],
            "customer": records["customers"],
            "seller": records["sellers"],
            "lakehouse": records["lakehouses"],
        }

        for entity_name, entity_records in entities.items():
            logger.info(
                f"Creating volume for {landing_catalog}.{landing_schema}.{entity_name}..."
            )
            df_entity = spark.createDataFrame(entity_records)
            df_entity.printSchema()
            df_entity.write.mode("overwrite").format("parquet").save(
                f"dbfs:/Volumes/{landing_catalog}/{landing_schema}/{entity_name}"
            )
            logger.info(f"{entity_name.capitalize()} data written successfully.")

    def generate_customer(self, fake: Faker):
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
            loyalty_tier: str
            preferred_payment_method: str
            account_manager: str
            is_subscribed_to_newsletter: bool
            last_purchase_date: date
            total_spend: float
            number_of_purchases: int

        loyalty_tiers = ["Bronze", "Silver", "Gold", "Platinum"]
        payment_methods = ["Credit Card", "MobilePay", "Bank Transfer"]
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
                loyalty_tier=random.choice(loyalty_tiers),
                preferred_payment_method=random.choice(payment_methods),
                account_manager=random.choice(account_managers),
                is_subscribed_to_newsletter=random.choice([True, False]),
                last_purchase_date=last_purchase,
                total_spend=total_spend,
                number_of_purchases=num_purchases,
            )
            customers.append(customer)

        return customers

    def lakehouse_profile(
        self, lakehouses_list: List[str], regions: List[str], fake: Faker
    ):
        @dataclass
        class LakehouseProfile:
            lakehouse_id: int
            name: str
            location: str
            region: str
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
            name = random.choice(lakehouses_list)
            location = fake.city()
            region = random.choice(regions)
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
                name=name,
                location=location,
                region=region,
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

    def generate_seller(self, lakehouses_records, regions: List[str], fake: Faker):
        @dataclass
        class SellerProfile:
            seller_id: int
            name: str
            email: str
            phone_number: str
            hire_date: date
            region: str
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
                lakehouse.name
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
                region=random.choice(regions),
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
            lakehouse_name: str
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
                lakehouse_name=lakehouse.name,
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
