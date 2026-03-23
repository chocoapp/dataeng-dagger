"""IO representation for Databricks Unity Catalog tables."""

from typing import Any

from dagger.pipeline.io import IO
from dagger.utilities.config_validator import Attribute


class DatabricksIO(IO):
    """IO representation for Databricks Unity Catalog tables.

    Represents a table in Databricks Unity Catalog with catalog.schema.table naming.
    Used to define inputs and outputs for tasks that read from or write to
    Databricks tables.

    Attributes:
        ref_name: Reference name used by IOFactory to instantiate this IO type.
        catalog: Databricks Unity Catalog name.
        schema: Schema/database name within the catalog.
        table: Table name.

    Example YAML configuration:
        type: databricks
        name: my_output_table
        catalog: prod_catalog
        schema: analytics
        table: user_metrics
    """

    ref_name: str = "databricks"

    @classmethod
    def init_attributes(cls, orig_cls: type) -> None:
        """Initialize configuration attributes for YAML parsing.

        Registers all attributes that can be specified in the YAML configuration.
        Called by the IO metaclass during class creation.

        Args:
            orig_cls: The original class being initialized (used for attribute
                registration).
        """
        cls.add_config_attributes(
            [
                Attribute(attribute_name="catalog"),
                Attribute(attribute_name="schema"),
                Attribute(attribute_name="table"),
            ]
        )

    def __init__(self, io_config: dict[str, Any], config_location: str) -> None:
        """Initialize a DatabricksIO instance.

        Args:
            io_config: Dictionary containing the IO configuration from YAML.
            config_location: Path to the configuration file for error reporting.

        Raises:
            DaggerMissingFieldException: If required fields (catalog, schema, table)
                are missing from the configuration.
        """
        super().__init__(io_config, config_location)

        self._catalog: str = self.parse_attribute("catalog")
        self._schema: str = self.parse_attribute("schema")
        self._table: str = self.parse_attribute("table")

    def alias(self) -> str:
        """Return the unique alias for this IO in databricks:// URI format.

        The alias is used for dataset lineage tracking and dependency resolution
        across pipelines.

        Returns:
            A unique identifier string in the format
            'databricks://{catalog}/{schema}/{table}'.
        """
        return f"databricks://{self._catalog}/{self._schema}/{self._table}"

    @property
    def rendered_name(self) -> str:
        """Return the fully qualified table name in dot notation.

        This format is used in SQL queries and Databricks API calls.

        Returns:
            The table name in '{catalog}.{schema}.{table}' format.
        """
        return f"{self._catalog}.{self._schema}.{self._table}"

    @property
    def airflow_name(self) -> str:
        """Return an Airflow-safe identifier for this table.

        Airflow task/dataset IDs cannot contain dots, so this returns a
        hyphen-separated format suitable for use in Airflow contexts.

        Returns:
            The table name in 'databricks-{catalog}-{schema}-{table}' format.
        """
        return f"databricks-{self._catalog}-{self._schema}-{self._table}"

    @property
    def catalog(self) -> str:
        """Return the Databricks Unity Catalog name.

        Returns:
            The catalog name.
        """
        return self._catalog

    @property
    def schema(self) -> str:
        """Return the schema/database name within the catalog.

        Returns:
            The schema name.
        """
        return self._schema

    @property
    def table(self) -> str:
        """Return the table name.

        Returns:
            The table name.
        """
        return self._table
