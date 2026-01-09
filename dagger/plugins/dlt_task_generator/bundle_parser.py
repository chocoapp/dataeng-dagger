"""Parse Databricks Asset Bundle YAML files for DLT pipeline configuration."""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

_logger = logging.getLogger(__name__)


@dataclass
class TableConfig:
    """Configuration for a single table in a DLT pipeline.

    Attributes:
        database: Source database name.
        table: Source table name.
        changelog_type: Type of changelog source ('dynamodb' or 'postgres').
        unique_keys: List of columns that uniquely identify a row.
        scd_type: Slowly changing dimension type (1 or 2).
    """

    database: str
    table: str
    changelog_type: str  # 'dynamodb' or 'postgres'
    unique_keys: list[str] = field(default_factory=list)
    scd_type: int = 1

    @property
    def source_schema(self) -> str:
        """Get the source schema name for Athena/Glue catalog.

        For DynamoDB: ddb_changelogs
        For PostgreSQL: pg_changelogs_kafka_{database_normalized}
        """
        if self.changelog_type == "dynamodb":
            return self.database
        elif self.changelog_type == "postgres":
            # Normalize database name (replace hyphens with underscores)
            db_normalized = self.database.replace("-", "_")
            return f"pg_changelogs_kafka_{db_normalized}"
        else:
            return self.database

    @property
    def silver_table_name(self) -> str:
        """Get the silver table name produced by DLT."""
        return f"silver_{self.table}"

    @property
    def bronze_table_name(self) -> str:
        """Get the bronze table name produced by DLT."""
        return f"bronze_{self.table}"


@dataclass
class PipelineConfig:
    """Configuration for a DLT pipeline parsed from Databricks Asset Bundle.

    Attributes:
        name: Pipeline/bundle name.
        catalog: Target Unity Catalog name.
        schema: Target schema name.
        tables: List of table configurations for the pipeline.
        targets: Target environment configurations (dev/prod).
        variables: Variable definitions from databricks.yml.
        tags: Pipeline tags.
    """

    name: str
    catalog: str
    schema: str
    tables: list[TableConfig] = field(default_factory=list)
    targets: dict[str, Any] = field(default_factory=dict)
    variables: dict[str, Any] = field(default_factory=dict)
    tags: dict[str, str] = field(default_factory=dict)


class DatabricksBundleParser:
    """Parse Databricks Asset Bundle YAML files (databricks.yml and tables.yml).

    This parser extracts pipeline configuration from Databricks Asset Bundles,
    including the target catalog, schema, table definitions, and job configuration.
    It resolves variable references to Dagger environment variable format.

    Attributes:
        _databricks_yml_path: Path to the databricks.yml file.
        _tables_yml_path: Path to the tables.yml file.
        _databricks_config: Parsed databricks.yml content.
        _tables_config: Parsed tables.yml content.
        _pipeline_config: Cached PipelineConfig instance.
    """

    def __init__(
        self,
        databricks_yml_path: Path,
        tables_yml_path: Optional[Path] = None,
    ) -> None:
        """Initialize the parser with paths to bundle YAML files.

        Args:
            databricks_yml_path: Path to the databricks.yml file.
            tables_yml_path: Optional path to the tables.yml file. If not provided,
                will look for tables.yml in the same directory.
        """
        self._databricks_yml_path = Path(databricks_yml_path)
        self._tables_yml_path = (
            Path(tables_yml_path)
            if tables_yml_path
            else self._databricks_yml_path.parent / "tables.yml"
        )

        self._databricks_config = self._load_yaml(self._databricks_yml_path)
        self._tables_config = (
            self._load_yaml(self._tables_yml_path)
            if self._tables_yml_path.exists()
            else {}
        )

        self._pipeline_config: Optional[PipelineConfig] = None

    @staticmethod
    def _load_yaml(path: Path) -> dict[str, Any]:
        """Load and parse a YAML file.

        Args:
            path: Path to the YAML file.

        Returns:
            Parsed YAML content as a dictionary.

        Raises:
            yaml.YAMLError: If the YAML file is malformed.
        """
        try:
            with open(path, "r") as f:
                return yaml.safe_load(f) or {}
        except FileNotFoundError:
            _logger.warning(f"YAML file not found: {path}")
            return {}
        except yaml.YAMLError as e:
            _logger.error(f"Error parsing YAML file {path}: {e}")
            raise

    def _resolve_variable(self, value: str) -> str:
        """Resolve Databricks bundle variable references like ${var.catalog}.

        Args:
            value: String that may contain variable references

        Returns:
            Resolved string with environment variable format for Dagger
        """
        if not isinstance(value, str):
            return value

        # Match ${var.variable_name} pattern
        var_pattern = re.compile(r"\$\{var\.(\w+)\}")

        def replace_var(match):
            var_name = match.group(1)
            # Get the default value from variables section
            var_config = self._databricks_config.get("variables", {}).get(var_name, {})
            default_value = var_config.get("default", "")

            # Map to Dagger environment variables
            if var_name == "catalog":
                # Map catalog to Dagger's ${ENV_MARTS} pattern
                return "${ENV_MARTS}"
            return default_value

        return var_pattern.sub(replace_var, value)

    def _parse_tables(self) -> list[TableConfig]:
        """Parse table configurations from tables.yml.

        Returns:
            List of TableConfig instances for each table defined in the bundle.
        """
        tables = []
        defaults = self._tables_config.get("defaults", {})
        default_scd_type = defaults.get("scd_type", 1)

        for table_config in self._tables_config.get("tables", []):
            tables.append(
                TableConfig(
                    database=table_config.get("database", ""),
                    table=table_config.get("table", ""),
                    changelog_type=table_config.get("changelog_type", "dynamodb"),
                    unique_keys=table_config.get("unique_keys", []),
                    scd_type=table_config.get("scd_type", default_scd_type),
                )
            )

        return tables

    def _parse_pipeline(self) -> PipelineConfig:
        """Parse pipeline configuration from databricks.yml.

        Extracts bundle name, variables, targets, and pipeline-specific settings
        from the Databricks Asset Bundle configuration.

        Returns:
            PipelineConfig instance with all parsed configuration.
        """
        bundle_name = self._databricks_config.get("bundle", {}).get("name", "")
        variables = self._databricks_config.get("variables", {})
        targets = self._databricks_config.get("targets", {})

        # Get pipeline configuration from resources
        resources = self._databricks_config.get("resources", {})
        pipelines = resources.get("pipelines", {})

        # Get the first pipeline (usually matches bundle name)
        pipeline_key = bundle_name or next(iter(pipelines.keys()), "")
        pipeline_config = pipelines.get(pipeline_key, {})

        catalog = self._resolve_variable(pipeline_config.get("catalog", ""))
        schema = pipeline_config.get("schema", "")
        tags = pipeline_config.get("tags", {})

        return PipelineConfig(
            name=bundle_name,
            catalog=catalog,
            schema=schema,
            tables=self._parse_tables(),
            targets=targets,
            variables=variables,
            tags=tags,
        )

    def parse(self) -> PipelineConfig:
        """Parse the Databricks Asset Bundle and return pipeline configuration.

        Returns:
            PipelineConfig with all parsed configuration
        """
        if self._pipeline_config is None:
            self._pipeline_config = self._parse_pipeline()
        return self._pipeline_config

    def get_bundle_name(self) -> str:
        """Return the bundle/pipeline name.

        Returns:
            The bundle name from databricks.yml.
        """
        return self.parse().name

    def get_catalog(self) -> str:
        """Return the target catalog with Dagger environment variable format.

        Returns:
            Catalog name with variables resolved to Dagger format (e.g., ${ENV_MARTS}).
        """
        return self.parse().catalog

    def get_schema(self) -> str:
        """Return the target schema.

        Returns:
            Target schema name for the DLT pipeline.
        """
        return self.parse().schema

    def get_tables(self) -> list[TableConfig]:
        """Return the list of table configurations.

        Returns:
            List of TableConfig instances for all tables in the pipeline.
        """
        return self.parse().tables

    def get_targets(self) -> dict[str, Any]:
        """Return target configurations (dev/prod).

        Returns:
            Dictionary of target environment configurations.
        """
        return self.parse().targets

    def get_variables(self) -> dict[str, Any]:
        """Return variable definitions.

        Returns:
            Dictionary of variable definitions from databricks.yml.
        """
        return self.parse().variables

    def get_job_name(self) -> str:
        """Get the Databricks Job name that triggers this pipeline.

        Looks for a job defined in resources.jobs that wraps the pipeline.
        Falls back to a default naming convention if no job is defined.

        Returns:
            The job name to use with DatabricksRunNowOperator
        """
        resources = self._databricks_config.get("resources", {})
        jobs = resources.get("jobs", {})

        # Return the first job's name, or use default naming convention
        for job_config in jobs.values():
            return job_config.get("name", f"dlt-{self.get_bundle_name()}")

        return f"dlt-{self.get_bundle_name()}"
