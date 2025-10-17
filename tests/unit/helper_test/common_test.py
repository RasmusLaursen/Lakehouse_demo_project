import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, patch, mock_open, MagicMock
import yaml
import tempfile
import os

# Add project root to path
project_root = Path(__file__).parents[3]
sys.path.insert(0, str(project_root))

with patch.dict(
    "sys.modules",
    {
        "src.helper.logging_helper": Mock(),
    },
):
    from src.helper.common import (
        get_path_for_data_configuration,
        get_data_configuration,
        add_audit_columns,
        _load_yaml_file,
        try_load_ingest_config,
        parse_arguments,
        list_volumes_in_schema,
        list_tables_in_schema,
    )


class TestCommonUtilities:
    """Test class for common utility functions."""

    def test_get_path_for_data_configuration(self):
        """Test path construction for data configuration."""
        catalog = "test_catalog"
        object_name = "test_object"

        result = get_path_for_data_configuration(catalog, object_name)

        expected_path = Path(f"../data_configuration/{catalog}/{object_name}.yml")
        assert result == expected_path
        assert isinstance(result, Path)

    @patch("src.helper.common.get_path_for_data_configuration")
    def test_get_data_configuration_file_not_found(self, mock_get_path):
        """Test data configuration when file doesn't exist."""
        # Setup mock
        mock_path = Mock()
        mock_path.is_file.return_value = False
        mock_get_path.return_value = mock_path

        # Execute and verify exception
        with pytest.raises(
            FileNotFoundError, match="Data configuration file not found"
        ):
            get_data_configuration("test_catalog", "test_object")

    def test_load_yaml_file_success(self):
        """Test successful YAML file loading."""
        test_data = {"test": "data", "nested": {"key": "value"}}
        yaml_content = yaml.dump(test_data)

        with patch("builtins.open", mock_open(read_data=yaml_content)):
            result = _load_yaml_file("test_file.yml")

        assert result == test_data

    def test_load_yaml_file_not_found(self):
        """Test YAML file loading when file doesn't exist."""
        with patch("builtins.open", side_effect=FileNotFoundError):
            with pytest.raises(
                FileNotFoundError, match="The file at test_file.yml was not found"
            ):
                _load_yaml_file("test_file.yml")

    def test_load_yaml_file_invalid_yaml(self):
        """Test YAML file loading with invalid YAML content."""
        invalid_yaml = "invalid: yaml: content: ["

        with patch("builtins.open", mock_open(read_data=invalid_yaml)):
            with pytest.raises(ValueError, match="Error parsing YAML file"):
                _load_yaml_file("test_file.yml")

    @patch("src.helper.common._load_yaml_file")
    def test_try_load_ingest_config_failure(self, mock_load_yaml):
        """Test ingest config loading when file loading fails."""
        mock_load_yaml.side_effect = FileNotFoundError("File not found")

        result = try_load_ingest_config(Path("test_path.yml"))

        assert result == {}
