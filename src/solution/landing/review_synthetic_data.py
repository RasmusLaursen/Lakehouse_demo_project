from dataclasses import dataclass
from datetime import date, timedelta
from faker import Faker
import random
from typing import List


# Define the Review data class with additional fields
@dataclass
class Review:
    rental_id: int
    review_date: date
    rating: int
    review_text: str
    review_title: str
    helpful_votes: int


# Define the LakehouseReviews class
class LakehouseReviews:
    def __init__(self, lakehouse_rentals: List[dict]):
        """
        Initialize the LakehouseReviews class with rental visit data.

        Args:
            lakehouse_rentals (List[dict]): List of rental visit data.
        """
        self.lakehouse_rentals = lakehouse_rentals
        Faker.seed(42)
        self.faker = Faker()

    def generate_reviews_synthetic_data(self) -> List[Review]:
        """
        Generate fake reviews based on the visits in lakehouse_rentals.

        Returns:
            List[Review]: A list of generated reviews.
        """
        reviews = []
        for rental in self.lakehouse_rentals:
            # Extract visit ID and rental date
            rental_date = rental.check_out_date

            # Generate a review date (1-7 days after the rental date)
            review_date = rental_date + timedelta(days=random.randint(1, 7))

            # Generate a random rating (1 to 5 stars)
            rating = random.randint(1, 5)

            # Generate a fake review text
            review_text = self.faker.text(max_nb_chars=200)

            # Generate additional fields
            review_title = self.faker.sentence(nb_words=6)
            helpful_votes = random.randint(0, 100)

            # Create a Review object and add it to the list
            reviews.append(
                Review(
                    rental_id=rental.rental_id,
                    review_date=review_date,
                    rating=rating,
                    review_text=review_text,
                    review_title=review_title,
                    helpful_votes=helpful_votes,
                )
            )

        return reviews
