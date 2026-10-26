"""Factory for creating curated dimension DLT tables using auto_cdc pattern.

This factory implements a declarative CDC-based approach where:
1. Custom transformation functions enrich data BEFORE CDC
2. dlt.auto_cdc handles change tracking with hash-based surrogate keys
3. All dimensions follow standardized naming and structure
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, current_user, col, hash
from typing import Optional, Callable, List

try:
    import dlt  # type: ignore
except ImportError:
    dlt = None  # type: ignore

from src.framework.helper import get_logger, ldp_change_data_capture
from src.framework.helper.dataframe.curated_audit import add_curated_audit_columns
from src.framework.config import CentralizedPipelineConfig, CatalogSchemaManager

logger = get_logger(__name__)


class DimensionFactory:
    """Factory for creating curated dimension tables using auto_cdc with hash-based surrogate keys."""

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

        logger.info(f"Initialized DimensionFactory for source_system: {source_system}")

    def create_dimension(
        self,
        dimension_name: str,
        keys: List[str],
        sequence_column: str,
        transform_fn: Callable[[], DataFrame],
        scd_type: int = 1,
        except_column_list: Optional[List[str]] = None,
    ) -> None:
        """Create a dimension table using auto_cdc pattern with transformation function.

        This method creates a dimension table following this flow:
        1. Apply transformation function to return source DataFrame
        2. Add hash-based surrogate key
        3. Add audit columns (created_at, updated_at, created_by)
        4. Apply auto_cdc

        Args:
            dimension_name: Name of dimension table (e.g., 'dim_customer')
            keys: Business keys for CDC tracking (e.g., ['customer_id'])
            sequence_column: Column for CDC sequencing (e.g., '__START_AT')
            transform_fn: Transformation function that returns source DataFrame
                         Function signature: def transform() -> DataFrame
            scd_type: SCD type - 1 (overwrite) or 2 (history). Default: 1
            except_column_list: Source columns to exclude from the dimension table

        Example:
            def get_customer_data() -> DataFrame:
                from src.framework.helper import get_spark
                spark = get_spark()
                df = spark.readStream.table("base_catalog.schema.customer")
                df = df.filter(col("__END_AT").isNull())  # Filter active
                # Apply transformations, joins, enrichments
                return df

            factory.create_dimension(
                dimension_name="dim_customer",
                keys=["customer_id"],
                sequence_column="__START_AT",
                transform_fn=get_customer_data,
                scd_type=2
            )
        """
        logger.info(f"Creating dimension: {dimension_name} (SCD Type {scd_type})")

        # Calculate surrogate key column name
        identity_column_name = self._get_surrogate_key_name(dimension_name)

        # Step 1: Create transformed source table (pre-CDC enrichment)
        transformed_source_table = f"{dimension_name}_transformed"
        transformed_source_path = f"{self.catalog_manager.curated_catalog}.{self.catalog_manager.dimensions_schema}.{transformed_source_table}"

        @dlt.table(  # type: ignore
            name=transformed_source_path,
            comment=f"Transformed source for {dimension_name} with enrichments applied",
            temporary=True,  # Intermediate table
        )
        def _transformed_source():
            """Apply transformation function and add audit columns before CDC."""
            logger.info(f"Applying transformation function for {dimension_name}")
            df = transform_fn()

            if scd_type == 1:
                df = df.withColumn(identity_column_name, hash(*keys))
            elif scd_type == 2:
                df = df.withColumn(
                    identity_column_name, hash(*keys, col(sequence_column))
                )

            # Add audit columns
            df = add_curated_audit_columns(df)
            logger.debug(
                f"Added audit columns to transformed source for {dimension_name}"
            )

            return df

        # Step 2: Create dimension table with CDC
        dimension_table_path = self.catalog_manager.get_dimension_table_path(
            dimension_name
        )

        logger.info(
            f"Creating CDC dimension table: {dimension_table_path} "
            f"with surrogate key: {identity_column_name}"
        )

        # Use ldp_change_data_capture which wraps dlt.create_auto_cdc_flow
        ldp_change_data_capture(
            source=transformed_source_path,
            target_catalog=self.centralized_config.curated_catalog,
            target_schema=self.centralized_config.dimensions_schema,
            target_object=dimension_name,
            keys=keys,
            sequence_column=sequence_column,
            stored_as_scd_type=scd_type,
            except_column_list=except_column_list,
            name=f"dimension_cdc_{dimension_name}",
        )

        logger.info(f"Successfully created dimension: {dimension_name}")

    def _get_surrogate_key_name(self, dimension_name: str) -> str:
        """Generate surrogate key column name from dimension name.

        Args:
            dimension_name: Name of dimension (e.g., 'dim_customer')

        Returns:
            Surrogate key column name (e.g., 'customer_id')
        """
        # Remove 'dim_' prefix and add '_id' suffix
        entity_name = dimension_name.replace("dim_", "")
        return f"{entity_name}_id"


def create_dimension(
    dimension_name: str,
    keys: List[str],
    sequence_column: str,
    transform_fn: Callable[[], DataFrame],
    scd_type: int = 1,
    source_system: str = "lakehouse",
    except_column_list: Optional[List[str]] = None,
) -> None:
    """Convenience function to create a dimension table.

    Args:
        dimension_name: Name of dimension table (e.g., 'dim_customer')
        keys: Business keys for CDC tracking
        sequence_column: Column for CDC sequencing (e.g., '__START_AT')
        transform_fn: Transformation function that returns source DataFrame
        scd_type: SCD type (1 or 2). Default: 1
        source_system: Source system name (default: 'lakehouse')
    """
    from src.framework.helper import get_spark

    spark = get_spark()
    factory = DimensionFactory(spark, source_system)
    factory.create_dimension(
        dimension_name=dimension_name,
        keys=keys,
        sequence_column=sequence_column,
        transform_fn=transform_fn,
        scd_type=scd_type,
        except_column_list=except_column_list,
    )
