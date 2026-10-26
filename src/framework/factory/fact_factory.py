"""Factory for creating curated fact DLT tables."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, current_user
from typing import Callable

try:
    import dlt  # type: ignore
except ImportError:
    dlt = None  # type: ignore

from src.framework.helper import get_logger, dimension_keys_lookup
from src.framework.helper.dataframe.curated_audit import add_curated_audit_columns
from src.framework.config import CentralizedPipelineConfig, CatalogSchemaManager

logger = get_logger(__name__)


class FactFactory:
    """Factory for creating curated fact tables with dimension key lookups."""

    def __init__(self, spark: SparkSession, source_system: str = "lakehouse"):
        """Initialize the factory.

        Args:
            spark: Active SparkSession
            source_system: Source system name (default: lakehouse)
        """
        self.spark = spark
        self.centralized_config = CentralizedPipelineConfig.from_spark(
            spark, source_system
        )
        self.centralized_config.validate()
        self.catalog_manager = CatalogSchemaManager.from_pipeline_config(
            self.centralized_config
        )

        logger.info(f"Using curated catalog: {self.centralized_config.curated_catalog}")
        logger.info(
            f"Using dimensions schema: {self.centralized_config.dimensions_schema}"
        )

        logger.info(f"Initialized FactFactory for source_system: {source_system}")

    def create_fact(
        self, fact_name: str, transform_fn: Callable[[], DataFrame]
    ) -> None:
        """Create a fact table with dimension key lookups.

        This method creates a fact table following this flow:
        1. Apply transformation function to return source DataFrame
        2. Apply dimension key lookups
        3. Add audit columns (created_at, updated_at, created_by)

        Args:
            fact_name: Name of fact table (e.g., 'fact_lakehouse_rentals')
            transform_fn: Transformation function that returns source DataFrame
                         Function signature: def transform() -> DataFrame

        Example:
            def get_rental_data() -> DataFrame:
                from src.framework.helper import get_spark
                spark = get_spark()
                df = spark.readStream.table("base_catalog.schema.rentals")
                # Apply transformations, joins, column renames
                df = df.withColumnsRenamed({'seller_id': 'seller_key', 'customer_id': 'customer_key'})
                return df

            factory.create_fact(
                fact_name="fact_lakehouse_rentals",
                transform_fn=get_rental_data
            )
        """
        logger.info(f"Creating fact table: {fact_name}")

        # Create the DLT table
        @dlt.table(  # type: ignore
            name=self.catalog_manager.get_fact_table_path(fact_name),
            comment=f"Curated layer fact table for {fact_name.replace('fact_', '')}",
        )
        def _fact_table():
            """Fact table with dimension key lookups and audit columns."""
            logger.info(f"Applying transformation function for {fact_name}")
            df = transform_fn()

            # Perform dimension key lookups
            df = dimension_keys_lookup(
                curated_catalog=self.centralized_config.curated_catalog,
                curated_dimension_schema=self.centralized_config.dimensions_schema,
                fact_df=df,
            )
            logger.debug(f"Applied dimension key lookups for {fact_name}")

            # Add audit columns
            df = add_curated_audit_columns(df)
            logger.debug(f"Added audit columns to {fact_name}")

            return df

        logger.info(f"Successfully created fact table: {fact_name}")


def create_fact(
    fact_name: str,
    transform_fn: Callable[[], DataFrame],
    source_system: str = "lakehouse",
) -> None:
    """Convenience function to create a fact table.

    Args:
        fact_name: Name of fact table (e.g., 'fact_lakehouse_rentals')
        transform_fn: Transformation function that returns source DataFrame
        source_system: Source system name (default: 'lakehouse')
    """
    from src.framework.helper import get_spark

    spark = get_spark()
    factory = FactFactory(spark, source_system)
    factory.create_fact(fact_name=fact_name, transform_fn=transform_fn)
