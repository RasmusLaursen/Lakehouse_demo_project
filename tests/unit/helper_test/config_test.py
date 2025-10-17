import pytest
import sys
from pathlib import Path
from pydantic import ValidationError

# Add project root to path
project_root = Path(__file__).parents[3]
sys.path.insert(0, str(project_root))

from src.helper.config import (
    TableConfig,
    LayerConfig,
    DefaultTblProperties,
    InternalAuditColumns,
)


class TestTableConfigModelValidator:
    """Test class for TableConfig model validator validate_track_history_lists."""

    # ===== POSITIVE TESTS (Should Pass) =====

    def test_scd_type_1_with_no_track_history_lists_valid(self):
        """Test SCD Type 1 with no track history lists - should be valid."""
        config = TableConfig(
            keys=["customer_id"],
            sequence_column="updated_at",
            stored_as_scd_type=1,
            track_history_column_list=None,
            track_history_except_column_list=None,
        )

        assert config.stored_as_scd_type == 1
        assert config.track_history_column_list is None
        assert config.track_history_except_column_list is None

    def test_scd_type_2_with_track_history_column_list_valid(self):
        """Test SCD Type 2 with only track_history_column_list - should be valid."""
        config = TableConfig(
            keys=["customer_id"],
            sequence_column="updated_at",
            stored_as_scd_type=2,
            track_history_column_list=["name", "email", "phone"],
            track_history_except_column_list=None,
        )

        assert config.stored_as_scd_type == 2
        assert config.track_history_column_list == ["name", "email", "phone"]
        assert config.track_history_except_column_list is None

    def test_scd_type_2_with_track_history_except_column_list_valid(self):
        """Test SCD Type 2 with only track_history_except_column_list - should be valid."""
        config = TableConfig(
            keys=["customer_id"],
            sequence_column="updated_at",
            stored_as_scd_type=2,
            track_history_column_list=None,
            track_history_except_column_list=["internal_id", "created_at"],
        )

        assert config.stored_as_scd_type == 2
        assert config.track_history_column_list is None
        assert config.track_history_except_column_list == ["internal_id", "created_at"]

    def test_scd_type_2_with_no_track_history_lists_valid(self):
        """Test SCD Type 2 with no track history lists - should be valid."""
        config = TableConfig(
            keys=["order_id"],
            sequence_column="created_at",
            stored_as_scd_type=2,
            track_history_column_list=None,
            track_history_except_column_list=None,
        )

        assert config.stored_as_scd_type == 2
        assert config.track_history_column_list is None
        assert config.track_history_except_column_list is None

    def test_column_list_only_valid(self):
        """Test with only column_list set - should be valid."""
        config = TableConfig(
            keys=["product_id"],
            sequence_column="modified_at",
            stored_as_scd_type=1,
            column_list=["name", "price", "description"],
            except_column_list=None,
        )

        assert config.column_list == ["name", "price", "description"]
        assert config.except_column_list is None

    def test_except_column_list_only_valid(self):
        """Test with only except_column_list set - should be valid."""
        config = TableConfig(
            keys=["user_id"],
            sequence_column="last_updated",
            stored_as_scd_type=1,
            column_list=None,
            except_column_list=["password", "internal_notes"],
        )

        assert config.column_list is None
        assert config.except_column_list == ["password", "internal_notes"]

    def test_no_column_lists_valid(self):
        """Test with no column lists set - should be valid."""
        config = TableConfig(
            keys=["transaction_id"],
            sequence_column="timestamp",
            stored_as_scd_type=2,
            column_list=None,
            except_column_list=None,
            track_history_column_list=None,
            track_history_except_column_list=None,
        )

        assert config.column_list is None
        assert config.except_column_list is None
        assert config.track_history_column_list is None
        assert config.track_history_except_column_list is None

    def test_empty_lists_valid(self):
        """Test with empty lists - should be valid (empty lists are falsy)."""
        config = TableConfig(
            keys=["id"],
            sequence_column="updated_at",
            stored_as_scd_type=2,
            track_history_column_list=[],
            track_history_except_column_list=None,
            column_list=[],
            except_column_list=None,
        )

        assert config.track_history_column_list == []
        assert config.column_list == []

    # ===== NEGATIVE TESTS (Should Fail) =====

    def test_scd_type_1_with_track_history_column_list_invalid(self):
        """Test SCD Type 1 with track_history_column_list - should fail."""
        with pytest.raises(ValidationError) as exc_info:
            TableConfig(
                keys=["customer_id"],
                sequence_column="updated_at",
                stored_as_scd_type=1,
                track_history_column_list=["name", "email"],
            )

        error_str = str(exc_info.value)
        assert (
            "track_history_column_list and track_history_except_column_list must be None when stored_as_scd_type is 1"
            in error_str
        )

    def test_scd_type_1_with_track_history_except_column_list_invalid(self):
        """Test SCD Type 1 with track_history_except_column_list - should fail."""
        with pytest.raises(ValidationError) as exc_info:
            TableConfig(
                keys=["customer_id"],
                sequence_column="updated_at",
                stored_as_scd_type=1,
                track_history_except_column_list=["internal_id"],
            )

        error_str = str(exc_info.value)
        assert (
            "track_history_column_list and track_history_except_column_list must be None when stored_as_scd_type is 1"
            in error_str
        )

    def test_scd_type_1_with_both_track_history_lists_invalid(self):
        """Test SCD Type 1 with both track history lists - should fail."""
        with pytest.raises(ValidationError) as exc_info:
            TableConfig(
                keys=["customer_id"],
                sequence_column="updated_at",
                stored_as_scd_type=1,
                track_history_column_list=["name"],
                track_history_except_column_list=["id"],
            )

        error_str = str(exc_info.value)
        assert (
            "track_history_column_list and track_history_except_column_list must be None when stored_as_scd_type is 1"
            in error_str
        )

    def test_both_track_history_lists_set_invalid(self):
        """Test SCD Type 2 with both track history lists set - should fail."""
        with pytest.raises(ValidationError) as exc_info:
            TableConfig(
                keys=["order_id"],
                sequence_column="created_at",
                stored_as_scd_type=2,
                track_history_column_list=["customer_name", "amount"],
                track_history_except_column_list=["internal_notes"],
            )

        error_str = str(exc_info.value)
        assert (
            "Only one of track_history_column_list or track_history_except_column_list can be set"
            in error_str
        )

    def test_both_column_lists_set_invalid(self):
        """Test with both column_list and except_column_list set - should fail."""
        with pytest.raises(ValidationError) as exc_info:
            TableConfig(
                keys=["product_id"],
                sequence_column="modified_at",
                stored_as_scd_type=1,
                column_list=["name", "price"],
                except_column_list=["internal_id"],
            )

        error_str = str(exc_info.value)
        assert "Only one of column_list or except_column_list can be set" in error_str

    def test_multiple_validation_errors_combined(self):
        """Test multiple validation errors combined - should fail with first error."""
        with pytest.raises(ValidationError) as exc_info:
            TableConfig(
                keys=["id"],
                sequence_column="updated_at",
                stored_as_scd_type=1,  # SCD Type 1
                track_history_column_list=["name"],  # Should cause error 1
                track_history_except_column_list=["id"],  # Should cause error 1
                column_list=["col1"],  # Should cause error 3
                except_column_list=["col2"],  # Should cause error 3
            )

        error_str = str(exc_info.value)
        # Should catch the first validation error
        assert (
            "track_history_column_list and track_history_except_column_list must be None when stored_as_scd_type is 1"
            in error_str
        )

    # ===== EDGE CASE TESTS =====

    def test_empty_track_history_column_list_with_scd_type_1_valid(self):
        """Test empty track_history_column_list with SCD Type 1 - should be valid (empty list is falsy)."""
        config = TableConfig(
            keys=["id"],
            sequence_column="updated_at",
            stored_as_scd_type=1,
            track_history_column_list=[],  # Empty list is falsy in Python
            track_history_except_column_list=None,
        )

        assert config.stored_as_scd_type == 1
        assert config.track_history_column_list == []

    def test_single_item_lists_valid(self):
        """Test with single item in lists - should be valid."""
        config = TableConfig(
            keys=["id"],
            sequence_column="updated_at",
            stored_as_scd_type=2,
            track_history_column_list=["single_column"],
            column_list=None,
            except_column_list=None,
        )

        assert config.track_history_column_list == ["single_column"]

    def test_large_lists_valid(self):
        """Test with large lists - should be valid."""
        large_column_list = [f"column_{i}" for i in range(100)]

        config = TableConfig(
            keys=["id"],
            sequence_column="updated_at",
            stored_as_scd_type=2,
            track_history_column_list=large_column_list,
            column_list=None,
            except_column_list=None,
        )

        assert len(config.track_history_column_list) == 100
        assert config.track_history_column_list[0] == "column_0"
        assert config.track_history_column_list[99] == "column_99"


class TestTableConfigIntegrationWithValidator:
    """Integration tests for TableConfig with validator."""

    def test_complete_valid_configuration_scd_type_1(self):
        """Test complete valid configuration for SCD Type 1."""
        config = TableConfig(
            keys=["customer_id", "account_id"],
            sequence_column="last_modified",
            stored_as_scd_type=1,
            backfill="2023-01-01",
            track_history_column_list=None,
            track_history_except_column_list=None,
            column_list=["name", "email", "phone"],
            except_column_list=None,
        )

        assert config.keys == ["customer_id", "account_id"]
        assert config.backfill == "2023-01-01"
        assert config.stored_as_scd_type == 1
        assert config.column_list == ["name", "email", "phone"]

    def test_complete_valid_configuration_scd_type_2(self):
        """Test complete valid configuration for SCD Type 2."""
        config = TableConfig(
            keys=["order_id"],
            sequence_column="updated_timestamp",
            stored_as_scd_type=2,
            backfill="2022-06-01",
            track_history_column_list=None,
            track_history_except_column_list=["created_by", "internal_notes"],
            column_list=None,
            except_column_list=["sensitive_data"],
        )

        assert config.keys == ["order_id"]
        assert config.backfill == "2022-06-01"
        assert config.stored_as_scd_type == 2
        assert config.track_history_except_column_list == [
            "created_by",
            "internal_notes",
        ]
        assert config.except_column_list == ["sensitive_data"]

    def test_json_serialization_with_validation(self):
        """Test JSON serialization works with validated model."""
        config = TableConfig(
            keys=["id"],
            sequence_column="updated_at",
            stored_as_scd_type=2,
            track_history_column_list=["col1", "col2"],
        )

        json_data = config.model_dump()

        # Verify JSON structure
        assert json_data["stored_as_scd_type"] == 2
        assert json_data["track_history_column_list"] == ["col1", "col2"]
        assert json_data["track_history_except_column_list"] is None


class TestTableConfig:
    """Test class for TableConfig model."""

    def test_table_config_valid(self):
        """Test TableConfig with valid data."""
        config = TableConfig(
            keys=["customer_id"], sequence_column="updated_at", stored_as_scd_type=1
        )

        assert config.keys == ["customer_id"]
        assert config.sequence_column == "updated_at"
        assert config.stored_as_scd_type == 1
        assert config.backfill is None
        assert config.track_history_column_list is None
        assert config.track_history_except_column_list is None

    def test_table_config_with_optional_fields(self):
        """Test TableConfig with all optional fields."""
        config = TableConfig(
            keys=["customer_id", "order_id"],
            sequence_column="created_at",
            stored_as_scd_type=2,
            backfill="2023-01-01",
            track_history_column_list=["name", "email"],
        )

        assert config.keys == ["customer_id", "order_id"]
        assert config.sequence_column == "created_at"
        assert config.stored_as_scd_type == 2
        assert config.backfill == "2023-01-01"
        assert config.track_history_column_list == ["name", "email"]

    def test_table_config_invalid_scd_type(self):
        """Test TableConfig with invalid SCD type."""
        with pytest.raises(ValidationError) as exc_info:
            TableConfig(
                keys=["id"],
                sequence_column="updated_at",
                stored_as_scd_type=3,  # Invalid SCD type
            )

        assert "stored_as_scd_type must be 1 or 2" in str(exc_info.value)

    def test_table_config_missing_required_fields(self):
        """Test TableConfig with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            TableConfig(
                keys=["id"]
                # Missing sequence_column and stored_as_scd_type
            )

        assert "Field required" in str(exc_info.value)

    def test_table_config_empty_keys(self):
        """Test TableConfig with empty keys list."""
        config = TableConfig(
            keys=[],  # Empty list should be valid
            sequence_column="updated_at",
            stored_as_scd_type=1,
        )

        assert config.keys == []

    def test_table_config_json_serialization(self):
        """Test TableConfig JSON serialization."""
        config = TableConfig(
            keys=["customer_id"],
            sequence_column="updated_at",
            stored_as_scd_type=1,
            backfill="2023-01-01",
        )

        json_data = config.model_dump()
        expected = {
            "keys": ["customer_id"],
            "sequence_column": "updated_at",
            "stored_as_scd_type": 1,
            "backfill": "2023-01-01",
            "column_list": None,
            "except_column_list": None,
            "track_history_column_list": None,
            "track_history_except_column_list": None,
            "apply_as_deletes": None,
            "apply_as_truncates": None,
            "ignore_null_updates": False,
            "data_quality": False

        }

        assert json_data == expected


class TestLayerConfig:
    """Test class for LayerConfig model."""

    def test_layer_config_minimal(self):
        """Test LayerConfig with minimal required data."""
        table_config = TableConfig(
            keys=["id"], sequence_column="updated_at", stored_as_scd_type=1
        )

        config = LayerConfig(objects={"test_table": table_config})

        assert config.source_system_name is None
        assert config.load_type is None
        assert config.file_type is None
        assert "test_table" in config.objects
        assert isinstance(config.objects["test_table"], TableConfig)

    def test_layer_config_with_all_fields(self):
        """Test LayerConfig with all fields populated."""
        table_config = TableConfig(
            keys=["customer_id"], sequence_column="created_at", stored_as_scd_type=2
        )

        config = LayerConfig(
            source_system_name="rental_system",
            load_type="incremental",
            file_type="parquet",
            objects={"customers": table_config, "orders": table_config},
        )

        assert config.source_system_name == "rental_system"
        assert config.load_type == "incremental"
        assert config.file_type == "parquet"
        assert len(config.objects) == 2
        assert "customers" in config.objects
        assert "orders" in config.objects

    def test_layer_config_empty_objects(self):
        """Test LayerConfig with empty objects dictionary."""
        config = LayerConfig(objects={})

        assert config.objects == {}

    def test_layer_config_nested_validation(self):
        """Test LayerConfig with invalid nested TableConfig."""
        with pytest.raises(ValidationError) as exc_info:
            LayerConfig(
                objects={
                    "invalid_table": TableConfig(
                        keys=["id"], sequence_column="updated_at", stored_as_scd_type=5
                    )
                }
            )

        assert "stored_as_scd_type must be 1 or 2" in str(exc_info.value)

    def test_layer_config_multiple_tables(self):
        """Test LayerConfig with multiple table configurations."""
        table_configs = {
            "customers": TableConfig(
                keys=["customer_id"],
                sequence_column="updated_at",
                stored_as_scd_type=2,
                track_history_column_list=["name", "email"],
            ),
            "orders": TableConfig(
                keys=["order_id"], sequence_column="created_at", stored_as_scd_type=1
            ),
            "products": TableConfig(
                keys=["product_id"],
                sequence_column="modified_at",
                stored_as_scd_type=2,
                backfill="2023-01-01",
            ),
        }

        config = LayerConfig(
            source_system_name="ecommerce", load_type="batch", objects=table_configs
        )

        assert len(config.objects) == 3
        assert config.objects["customers"].stored_as_scd_type == 2
        assert config.objects["orders"].stored_as_scd_type == 1
        assert config.objects["products"].backfill == "2023-01-01"


class TestDefaultTblProperties:
    """Test class for DefaultTblProperties dataclass."""

    def test_default_tbl_properties_defaults(self):
        """Test DefaultTblProperties with default values."""
        props = DefaultTblProperties()

        assert props.delta_enableDeletionVectors == "true"
        assert props.delta_enableRowTracking == "true"
        assert props.delta_enableChangeDataFeed == "true"
        assert props.pipelines_changeDataCaptureMode == "TRACK_CHANGES"

    def test_default_tbl_properties_custom_values(self):
        """Test DefaultTblProperties with custom values."""
        props = DefaultTblProperties(
            delta_enableDeletionVectors="false",
            delta_enableRowTracking="false",
            delta_enableChangeDataFeed="false",
            pipelines_changeDataCaptureMode="DISABLED",
        )

        assert props.delta_enableDeletionVectors == "false"
        assert props.delta_enableRowTracking == "false"
        assert props.delta_enableChangeDataFeed == "false"
        assert props.pipelines_changeDataCaptureMode == "DISABLED"

    def test_default_tbl_properties_as_dict(self):
        """Test DefaultTblProperties as_dict method."""
        props = DefaultTblProperties()
        result = props.as_dict()

        expected = {
            "delta.enableDeletionVectors": "true",
            "delta.enableRowTracking": "true",
            "delta.enableChangeDataFeed": "true",
            "pipelines.changeDataCaptureMode": "TRACK_CHANGES",
        }

        assert result == expected

    def test_default_tbl_properties_immutable(self):
        """Test that DefaultTblProperties is immutable (frozen)."""
        props = DefaultTblProperties()

        with pytest.raises(AttributeError):
            props.delta_enableDeletionVectors = "false"


class TestInternalAuditColumns:
    """Test class for InternalAuditColumns dataclass."""

    def test_internal_audit_columns_default(self):
        """Test InternalAuditColumns with default value."""
        audit = InternalAuditColumns()

        assert audit.audit_column == "_metadata_ldp"

    def test_internal_audit_columns_custom(self):
        """Test InternalAuditColumns with custom value."""
        audit = InternalAuditColumns(audit_column="_custom_audit")

        assert audit.audit_column == "_custom_audit"

    def test_internal_audit_columns_immutable(self):
        """Test that InternalAuditColumns is immutable (frozen)."""
        audit = InternalAuditColumns()

        with pytest.raises(AttributeError):
            audit.audit_column = "_new_audit"


class TestConfigIntegration:
    """Integration tests for configuration classes."""

    def test_complete_configuration_example(self):
        """Test a complete configuration example."""
        # Create table configurations
        customer_config = TableConfig(
            keys=["customer_id"],
            sequence_column="updated_at",
            stored_as_scd_type=2,
            track_history_column_list=["name", "email", "phone"],
        )

        order_config = TableConfig(
            keys=["order_id"],
            sequence_column="created_at",
            stored_as_scd_type=1,
            backfill="2023-01-01",
        )

        # Create layer configuration
        layer_config = LayerConfig(
            source_system_name="lakehouse_rental",
            load_type="streaming",
            file_type="delta",
            objects={"customers": customer_config, "orders": order_config},
        )

        # Create properties
        props = DefaultTblProperties()
        audit = InternalAuditColumns()

        # Verify the complete configuration
        assert layer_config.source_system_name == "lakehouse_rental"
        assert len(layer_config.objects) == 2
        assert layer_config.objects["customers"].stored_as_scd_type == 2
        assert layer_config.objects["orders"].backfill == "2023-01-01"
        assert props.as_dict()["delta.enableChangeDataFeed"] == "true"
        assert audit.audit_column == "_metadata_ldp"

    def test_configuration_from_dict(self):
        """Test creating configuration from dictionary (simulating YAML load)."""
        config_dict = {
            "source_system_name": "test_system",
            "load_type": "batch",
            "file_type": "parquet",
            "objects": {
                "table1": {
                    "keys": ["id"],
                    "sequence_column": "updated_at",
                    "stored_as_scd_type": 1,
                },
                "table2": {
                    "keys": ["id", "version"],
                    "sequence_column": "created_at",
                    "stored_as_scd_type": 2,
                    "backfill": "2023-01-01",
                },
            },
        }

        # This simulates loading from YAML
        layer_config = LayerConfig(**config_dict)

        assert layer_config.source_system_name == "test_system"
        assert layer_config.load_type == "batch"
        assert len(layer_config.objects) == 2
        assert isinstance(layer_config.objects["table1"], TableConfig)
        assert layer_config.objects["table2"].backfill == "2023-01-01"
