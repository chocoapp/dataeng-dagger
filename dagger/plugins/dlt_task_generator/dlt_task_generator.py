"""Generate Dagger task configurations from Databricks DLT bundle definitions."""

import logging
import os
from pathlib import Path
from typing import Any, Optional

import yaml

from dagger.plugins.dlt_task_generator.bundle_parser import (
    DatabricksBundleParser,
    PipelineConfig,
    TableConfig,
)

_logger = logging.getLogger(__name__)

# Default path to the DLT pipelines repository (can be overridden via env var)
DEFAULT_DLT_PIPELINES_REPO = os.getenv(
    "DLT_PIPELINES_REPO",
    str(Path(__file__).parent.parent.parent.parent.parent / "dataeng-databricks-dlt-pipelines"),
)


class DLTTaskGenerator:
    """Generate Dagger task configurations from Databricks DLT bundle definitions.

    This generator reads Databricks Asset Bundle configurations and produces
    Dagger-compatible YAML task configurations for DLT pipelines.

    Attributes:
        ATHENA_TASK_BASE: Base configuration for Athena input tasks.
        DATABRICKS_TASK_BASE: Base configuration for Databricks output tasks.
        DUMMY_TASK_BASE: Base configuration for dummy tasks.
    """

    ATHENA_TASK_BASE: dict[str, str] = {"type": "athena"}
    DATABRICKS_TASK_BASE: dict[str, str] = {"type": "databricks"}
    DUMMY_TASK_BASE: dict[str, str] = {"type": "dummy"}

    def __init__(self, dlt_repo_path: Optional[str] = None) -> None:
        """Initialize the generator with path to DLT pipelines repository.

        Args:
            dlt_repo_path: Path to the dataeng-databricks-dlt-pipelines repository.
                Defaults to DLT_PIPELINES_REPO env var or sibling directory.
        """
        self._dlt_repo_path = Path(dlt_repo_path or DEFAULT_DLT_PIPELINES_REPO)
        self._pipelines: dict[str, DatabricksBundleParser] = {}
        self._load_all_pipelines()

    def _load_all_pipelines(self) -> None:
        """Load all pipeline bundles from the DLT repository.

        Scans the pipelines directory and loads each valid Databricks Asset Bundle
        found. Bundles are identified by the presence of a databricks.yml file.
        """
        pipelines_dir = self._dlt_repo_path / "pipelines"

        if not pipelines_dir.exists():
            _logger.warning(f"DLT pipelines directory not found: {pipelines_dir}")
            return

        for pipeline_dir in pipelines_dir.iterdir():
            if not pipeline_dir.is_dir():
                continue

            databricks_yml = pipeline_dir / "databricks.yml"
            if not databricks_yml.exists():
                continue

            tables_yml = pipeline_dir / "tables.yml"
            try:
                parser = DatabricksBundleParser(databricks_yml, tables_yml)
                pipeline_name = parser.get_bundle_name() or pipeline_dir.name
                self._pipelines[pipeline_name] = parser
                _logger.info(f"Loaded DLT pipeline: {pipeline_name}")
            except Exception as e:
                _logger.error(f"Error loading pipeline from {pipeline_dir}: {e}")

    def get_pipeline_names(self) -> list[str]:
        """Return list of available DLT pipeline names."""
        return list(self._pipelines.keys())

    def get_pipeline_config(self, pipeline_name: str) -> PipelineConfig:
        """Get the parsed pipeline configuration.

        Args:
            pipeline_name: Name of the pipeline

        Returns:
            PipelineConfig object

        Raises:
            ValueError: If pipeline not found
        """
        if pipeline_name not in self._pipelines:
            raise ValueError(
                f"Unknown pipeline: {pipeline_name}. "
                f"Available pipelines: {self.get_pipeline_names()}"
            )
        return self._pipelines[pipeline_name].parse()

    def _get_athena_input(
        self, table: TableConfig, follow_external_dependency: bool = True
    ) -> dict[str, Any]:
        """Generate an Athena input task for a source changelog table.

        Args:
            table: Table configuration from the DLT bundle.
            follow_external_dependency: Whether to create an ExternalTaskSensor
                for cross-pipeline dependency tracking.

        Returns:
            Dagger Athena task configuration dict.
        """
        task = self.ATHENA_TASK_BASE.copy()
        task.update(
            {
                "schema": table.source_schema,
                "table": table.table,
                "name": f"{table.source_schema}__{table.table}_athena",
            }
        )
        if follow_external_dependency:
            task["follow_external_dependency"] = True
        return task

    def _get_databricks_output(
        self, table: TableConfig, catalog: str, schema: str
    ) -> dict[str, Any]:
        """Generate a Databricks output task for a silver table.

        Args:
            table: Table configuration from the DLT bundle.
            catalog: Target Unity Catalog name (e.g., ${ENV_MARTS}).
            schema: Target schema name.

        Returns:
            Dagger Databricks task configuration dict.
        """
        task = self.DATABRICKS_TASK_BASE.copy()
        # Normalize catalog name for task naming
        catalog_name = catalog.replace("${", "").replace("}", "").lower()
        task.update(
            {
                "catalog": catalog,
                "schema": schema,
                "table": table.silver_table_name,
                "name": f"{catalog_name}__{schema}__{table.silver_table_name}_databricks",
            }
        )
        return task

    def get_inputs(
        self, pipeline_name: str, follow_external_dependency: bool = True
    ) -> list[dict[str, Any]]:
        """Generate input dependencies for a DLT pipeline task.

        These are the source changelog tables that the DLT pipeline reads from.

        Args:
            pipeline_name: Name of the DLT pipeline.
            follow_external_dependency: Whether to create ExternalTaskSensors
                for cross-pipeline dependency tracking.

        Returns:
            List of Dagger input task configurations.
        """
        config = self.get_pipeline_config(pipeline_name)
        inputs = []

        for table in config.tables:
            input_task = self._get_athena_input(table, follow_external_dependency)
            inputs.append(input_task)

        return inputs

    def get_outputs(self, pipeline_name: str) -> list[dict[str, Any]]:
        """Generate output declarations for a DLT pipeline task.

        These are the silver tables produced by the DLT pipeline.

        Args:
            pipeline_name: Name of the DLT pipeline.

        Returns:
            List of Dagger output task configurations.
        """
        config = self.get_pipeline_config(pipeline_name)
        outputs = []

        for table in config.tables:
            output_task = self._get_databricks_output(
                table, config.catalog, config.schema
            )
            outputs.append(output_task)

        return outputs

    def get_task_parameters(self, pipeline_name: str) -> dict[str, Any]:
        """Generate task parameters for triggering a DLT pipeline via Databricks Job.

        Args:
            pipeline_name: Name of the DLT pipeline.

        Returns:
            Dict of task parameters for the DatabricksRunNowOperator.
        """
        parser = self._pipelines[pipeline_name]
        return {
            "job_name": parser.get_job_name(),
            "databricks_conn_id": "${DATABRICKS_CONN_ID}",
            "wait_for_completion": True,
            "poll_interval_seconds": 30,
            "timeout_seconds": 3600,
        }

    def generate_task_config(
        self,
        pipeline_name: str,
        description: Optional[str] = None,
        follow_external_dependency: bool = True,
    ) -> dict[str, Any]:
        """Generate a complete Dagger task configuration for a DLT pipeline.

        Args:
            pipeline_name: Name of the DLT pipeline.
            description: Optional task description. Defaults to auto-generated.
            follow_external_dependency: Whether to create ExternalTaskSensors for inputs.

        Returns:
            Complete Dagger task configuration dict ready for YAML serialization.
        """
        config = self.get_pipeline_config(pipeline_name)

        task_config = {
            "type": "databricks_dlt",
            "description": description or f"Run DLT pipeline {pipeline_name}",
            "inputs": self.get_inputs(pipeline_name, follow_external_dependency),
            "outputs": self.get_outputs(pipeline_name),
            "airflow_task_parameters": {
                "retries": 2,
                "retry_delay": 300,
            },
            "template_parameters": {},
            "task_parameters": self.get_task_parameters(pipeline_name),
        }

        return task_config

    def generate_pipeline_config(
        self,
        pipeline_name: str,
        schedule: str = "0 * * * *",
        owner: str = "dataeng@choco.com",
    ) -> dict[str, Any]:
        """Generate a Dagger pipeline.yaml configuration for a DLT pipeline DAG.

        Args:
            pipeline_name: Name of the DLT pipeline.
            schedule: Cron schedule expression. Defaults to hourly.
            owner: Pipeline owner email address.

        Returns:
            Dagger pipeline.yaml configuration dict ready for YAML serialization.
        """
        config = self.get_pipeline_config(pipeline_name)

        return {
            "owner": owner,
            "description": f"DLT Pipeline - {pipeline_name}",
            "schedule": schedule,
            "start_date": "2024-01-01T00:00",
            "airflow_parameters": {
                "default_args": {
                    "retries": 2,
                    "retry_delay": 180,
                    "depends_on_past": False,
                },
                "dag_parameters": {
                    "catchup": False,
                    "max_active_runs": 1,
                    "tags": ["dlt", "databricks", pipeline_name],
                },
            },
            "alerts": [
                {
                    "type": "slack",
                    "channel": "#${ENV}-airflow-alerts",
                    "mentions": ["@dataeng-oncall"],
                }
            ],
        }

    def write_task_config(
        self, pipeline_name: str, output_path: Path, **kwargs: Any
    ) -> Path:
        """Write a task configuration to a YAML file.

        Args:
            pipeline_name: Name of the DLT pipeline.
            output_path: Directory to write the file to.
            **kwargs: Additional arguments passed to generate_task_config.

        Returns:
            Path to the written YAML file.
        """
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)

        task_config = self.generate_task_config(pipeline_name, **kwargs)
        file_path = output_path / f"{pipeline_name}_dlt.yaml"

        with open(file_path, "w") as f:
            # Add autogenerated marker
            task_config["autogenerated_by_dagger"] = f"dlt_task_generator:{pipeline_name}"
            yaml.dump(task_config, f, default_flow_style=False, sort_keys=False)

        _logger.info(f"Generated task config: {file_path}")
        return file_path

    def write_pipeline_config(
        self, pipeline_name: str, output_path: Path, **kwargs: Any
    ) -> Path:
        """Write a pipeline configuration to a YAML file.

        Args:
            pipeline_name: Name of the DLT pipeline.
            output_path: Directory to write the file to.
            **kwargs: Additional arguments passed to generate_pipeline_config.

        Returns:
            Path to the written YAML file.
        """
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)

        pipeline_config = self.generate_pipeline_config(pipeline_name, **kwargs)
        file_path = output_path / "pipeline.yaml"

        with open(file_path, "w") as f:
            yaml.dump(pipeline_config, f, default_flow_style=False, sort_keys=False)

        _logger.info(f"Generated pipeline config: {file_path}")
        return file_path

    def generate_all(self, output_base_path: Path) -> list[Path]:
        """Generate all DLT pipeline configurations.

        Creates pipeline.yaml and task configuration files for each loaded
        DLT pipeline in the repository.

        Args:
            output_base_path: Base directory for output (e.g., dags/dlt/).

        Returns:
            List of paths to all generated files.
        """
        output_base_path = Path(output_base_path)
        generated_files = []

        for pipeline_name in self.get_pipeline_names():
            pipeline_output_path = output_base_path / pipeline_name

            # Generate pipeline.yaml
            pipeline_file = self.write_pipeline_config(pipeline_name, pipeline_output_path)
            generated_files.append(pipeline_file)

            # Generate task config
            task_file = self.write_task_config(pipeline_name, pipeline_output_path)
            generated_files.append(task_file)

        return generated_files
