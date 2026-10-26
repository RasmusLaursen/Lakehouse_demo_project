"""Catalog and schema path management."""
from dataclasses import dataclass
from src.framework.helper import logging_helper

logger = logging_helper.get_logger(__name__)


@dataclass
class CatalogSchemaManager:
    """Manages catalog and schema path construction for all table types.
    
    Centralizes the logic for building fully qualified table paths
    across all layers (raw, base, curated, enriched).
    """
    
    landing_catalog: str
    raw_catalog: str
    base_catalog: str
    curated_catalog: str
    enriched_catalog: str
    
    landing_schema: str
    raw_schema: str
    base_schema: str
    dimensions_schema: str
    facts_schema: str
    enriched_schema: str
    
    def get_table_path(self, catalog: str, schema: str, table_name: str) -> str:
        """Get fully qualified path for any table.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table_name: Table name
            
        Returns:
            Fully qualified table path (catalog.schema.table_name)
        """
        return f"{catalog}.{schema}.{table_name}"
    
    def get_base_table_path(self, table_name: str) -> str:
        """Get fully qualified path to a base layer table.
        
        Args:
            table_name: Name of the table in base layer
            
        Returns:
            Fully qualified table path
            
        Example:
            >>> manager.get_base_table_path("customer")
            'dev_base.lakehouse_base.customer'
        """
        return self.get_table_path(self.base_catalog, self.base_schema, table_name)
    
    def get_dimension_table_path(self, dimension_name: str) -> str:
        """Get fully qualified path to a dimension table.
        
        Args:
            dimension_name: Name of the dimension (e.g., 'dim_customer')
            
        Returns:
            Fully qualified table path
            
        Example:
            >>> manager.get_dimension_table_path("dim_customer")
            'dev_curated.dimensions.dim_customer'
        """
        return self.get_table_path(self.curated_catalog, self.dimensions_schema, dimension_name)
    
    def get_fact_table_path(self, fact_name: str) -> str:
        """Get fully qualified path to a fact table.
        
        Args:
            fact_name: Name of the fact (e.g., 'fact_sales')
            
        Returns:
            Fully qualified table path
            
        Example:
            >>> manager.get_fact_table_path("fact_sales")
            'dev_curated.facts.fact_sales'
        """
        return self.get_table_path(self.curated_catalog, self.facts_schema, fact_name)
    
    def get_landing_table_path(self, table_name: str) -> str:
        """Get fully qualified path to a landing layer table.
        
        Args:
            table_name: Name of the table in landing layer
            
        Returns:
            Fully qualified table path
        """
        return self.get_table_path(self.landing_catalog, self.landing_schema, table_name)
    
    def get_raw_table_path(self, table_name: str) -> str:
        """Get fully qualified path to a raw layer table.
        
        Args:
            table_name: Name of the table in raw layer
            
        Returns:
            Fully qualified table path
        """
        return self.get_table_path(self.raw_catalog, self.raw_schema, table_name)
    
    def get_enriched_table_path(self, table_name: str) -> str:
        """Get fully qualified path to an enriched layer table.
        
        Args:
            table_name: Name of the table in enriched layer
            
        Returns:
            Fully qualified table path
        """
        return self.get_table_path(self.enriched_catalog, self.enriched_schema, table_name)
    
    @staticmethod
    def from_pipeline_config(config: 'CentralizedPipelineConfig') -> 'CatalogSchemaManager':
        """Create from CentralizedPipelineConfig.
        
        Args:
            config: CentralizedPipelineConfig instance
            
        Returns:
            CatalogSchemaManager instance
        """
        return CatalogSchemaManager(
            landing_catalog=config.landing_catalog,
            raw_catalog=config.raw_catalog,
            base_catalog=config.base_catalog,
            curated_catalog=config.curated_catalog,
            enriched_catalog=config.enriched_catalog,
            landing_schema=config.landing_schema,
            raw_schema=config.raw_schema,
            base_schema=config.base_schema,
            dimensions_schema=config.dimensions_schema,
            facts_schema=config.facts_schema,
            enriched_schema=config.enriched_schema,
        )
