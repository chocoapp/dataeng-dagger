"""Unit tests for DatabricksIO."""

import unittest

import yaml

from dagger.pipeline.ios import databricks_io
from dagger.utilities.exceptions import DaggerMissingFieldException


class TestDatabricksIO(unittest.TestCase):
    """Test cases for DatabricksIO."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        with open("tests/fixtures/pipeline/ios/databricks_io.yaml", "r") as stream:
            config = yaml.safe_load(stream)

        self.db_io = databricks_io.DatabricksIO(config, "/")

    def test_ref_name(self) -> None:
        """Test that ref_name is correctly set."""
        self.assertEqual(databricks_io.DatabricksIO.ref_name, "databricks")

    def test_catalog(self) -> None:
        """Test catalog property."""
        self.assertEqual(self.db_io.catalog, "test_catalog")

    def test_schema(self) -> None:
        """Test schema property."""
        self.assertEqual(self.db_io.schema, "test_schema")

    def test_table(self) -> None:
        """Test table property."""
        self.assertEqual(self.db_io.table, "test_table")

    def test_alias(self) -> None:
        """Test alias method returns databricks:// URI format."""
        self.assertEqual(
            self.db_io.alias(), "databricks://test_catalog/test_schema/test_table"
        )

    def test_rendered_name(self) -> None:
        """Test rendered_name returns dot-separated format."""
        self.assertEqual(
            self.db_io.rendered_name, "test_catalog.test_schema.test_table"
        )

    def test_airflow_name(self) -> None:
        """Test airflow_name returns hyphen-separated format."""
        self.assertEqual(
            self.db_io.airflow_name, "databricks-test_catalog-test_schema-test_table"
        )

    def test_name(self) -> None:
        """Test name property from base IO class."""
        self.assertEqual(self.db_io.name, "test")

    def test_has_dependency_default(self) -> None:
        """Test that has_dependency defaults to True."""
        self.assertTrue(self.db_io.has_dependency)


class TestDatabricksIOInlineConfig(unittest.TestCase):
    """Test cases for DatabricksIO with inline configuration."""

    def test_with_minimal_config(self) -> None:
        """Test DatabricksIO with minimal required configuration."""
        config = {
            "type": "databricks",
            "name": "minimal_table",
            "catalog": "my_catalog",
            "schema": "my_schema",
            "table": "my_table",
        }

        db_io = databricks_io.DatabricksIO(config, "/test/path")

        self.assertEqual(db_io.catalog, "my_catalog")
        self.assertEqual(db_io.schema, "my_schema")
        self.assertEqual(db_io.table, "my_table")
        self.assertEqual(db_io.name, "minimal_table")

    def test_alias_format_with_special_characters(self) -> None:
        """Test alias format with underscores and numbers."""
        config = {
            "type": "databricks",
            "name": "output_123",
            "catalog": "prod_catalog_v2",
            "schema": "analytics_schema",
            "table": "user_events_2024",
        }

        db_io = databricks_io.DatabricksIO(config, "/")

        self.assertEqual(
            db_io.alias(),
            "databricks://prod_catalog_v2/analytics_schema/user_events_2024",
        )
        self.assertEqual(
            db_io.rendered_name, "prod_catalog_v2.analytics_schema.user_events_2024"
        )
        self.assertEqual(
            db_io.airflow_name,
            "databricks-prod_catalog_v2-analytics_schema-user_events_2024",
        )

    def test_has_dependency_false(self) -> None:
        """Test that has_dependency can be set to False."""
        config = {
            "type": "databricks",
            "name": "no_dep_table",
            "catalog": "cat",
            "schema": "sch",
            "table": "tbl",
            "has_dependency": False,
        }

        db_io = databricks_io.DatabricksIO(config, "/")

        self.assertFalse(db_io.has_dependency)


class TestDatabricksIOMissingFields(unittest.TestCase):
    """Test cases for DatabricksIO error handling."""

    def test_missing_catalog_raises_exception(self) -> None:
        """Test that missing catalog raises DaggerMissingFieldException."""
        config = {
            "type": "databricks",
            "name": "test_table",
            "schema": "test_schema",
            "table": "test_table",
        }

        with self.assertRaises(DaggerMissingFieldException):
            databricks_io.DatabricksIO(config, "/test/config.yaml")

    def test_missing_schema_raises_exception(self) -> None:
        """Test that missing schema raises DaggerMissingFieldException."""
        config = {
            "type": "databricks",
            "name": "test_table",
            "catalog": "test_catalog",
            "table": "test_table",
        }

        with self.assertRaises(DaggerMissingFieldException):
            databricks_io.DatabricksIO(config, "/test/config.yaml")

    def test_missing_table_raises_exception(self) -> None:
        """Test that missing table raises DaggerMissingFieldException."""
        config = {
            "type": "databricks",
            "name": "test_table",
            "catalog": "test_catalog",
            "schema": "test_schema",
        }

        with self.assertRaises(DaggerMissingFieldException):
            databricks_io.DatabricksIO(config, "/test/config.yaml")

    def test_missing_name_raises_exception(self) -> None:
        """Test that missing name raises DaggerMissingFieldException."""
        config = {
            "type": "databricks",
            "catalog": "test_catalog",
            "schema": "test_schema",
            "table": "test_table",
        }

        with self.assertRaises(DaggerMissingFieldException):
            databricks_io.DatabricksIO(config, "/test/config.yaml")


class TestDatabricksIOEquality(unittest.TestCase):
    """Test cases for DatabricksIO equality comparison."""

    def test_equal_ios_are_equal(self) -> None:
        """Test that two IOs with same alias are equal."""
        config1 = {
            "type": "databricks",
            "name": "table1",
            "catalog": "cat",
            "schema": "sch",
            "table": "tbl",
        }
        config2 = {
            "type": "databricks",
            "name": "table2",  # Different name, same catalog.schema.table
            "catalog": "cat",
            "schema": "sch",
            "table": "tbl",
        }

        io1 = databricks_io.DatabricksIO(config1, "/")
        io2 = databricks_io.DatabricksIO(config2, "/")

        self.assertEqual(io1, io2)

    def test_different_ios_are_not_equal(self) -> None:
        """Test that two IOs with different aliases are not equal."""
        config1 = {
            "type": "databricks",
            "name": "table1",
            "catalog": "cat1",
            "schema": "sch",
            "table": "tbl",
        }
        config2 = {
            "type": "databricks",
            "name": "table2",
            "catalog": "cat2",
            "schema": "sch",
            "table": "tbl",
        }

        io1 = databricks_io.DatabricksIO(config1, "/")
        io2 = databricks_io.DatabricksIO(config2, "/")

        self.assertNotEqual(io1, io2)


if __name__ == "__main__":
    unittest.main()
