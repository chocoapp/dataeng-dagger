import json
import yaml
from abc import ABC, abstractmethod
from collections import OrderedDict
from os import path
from os.path import join
from typing import Tuple, List, Dict
import logging

# Task base configurations
ATHENA_TASK_BASE = {"type": "athena"}
DATABRICKS_TASK_BASE = {"type": "databricks"}
S3_TASK_BASE = {"type": "s3"}
_logger = logging.getLogger("root")


class DBTConfigParser(ABC):
    """Abstract base class for parsing dbt manifest.json files and generating task configurations."""

    def __init__(self, config_parameters: dict):
        self._dbt_project_dir = config_parameters.get("project_dir")
        self._profile_name = config_parameters.get("profile_name", "")
        self._target_name = config_parameters.get("target_name", "")
        self._dbt_profile_dir = config_parameters.get("profile_dir", None)
        self._manifest_data = self._load_file(
            self._get_manifest_path(), file_type="json"
        )
        profile_data = self._load_file(self._get_profile_path(), file_type="yaml")
        self._target_config = (
            profile_data[self._profile_name]["outputs"].get(self._target_name)
            if self._profile_name == "athena"
            else profile_data[self._profile_name]["outputs"]["data"]
        )  # if databricks, get the default catalog and schema from the data output
        self._default_schema = self._target_config.get("schema", "")
        self._nodes_in_manifest = self._manifest_data.get("nodes", {})
        self._sources_in_manifest = self._manifest_data.get("sources", {})

    @property
    def nodes_in_manifest(self):
        return self._nodes_in_manifest

    @property
    def sources_in_manifest(self):
        return self._sources_in_manifest

    @property
    def dbt_default_schema(self):
        return self._default_schema

    def _get_manifest_path(self) -> str:
        """
        Construct path for manifest.json file based on configuration parameters.
        """
        target_path = f"{self._profile_name}_target"
        return path.join(self._dbt_project_dir, target_path, "manifest.json")

    def _get_profile_path(self) -> str:
        """
        Construct path for profiles.yml file based on configuration parameters.
        """
        return path.join(self._dbt_profile_dir, "profiles.yml")

    @staticmethod
    def _load_file(file_path: str, file_type: str) -> dict:
        """Load a file (JSON or YAML) based on the specified type and return its contents."""
        try:
            with open(file_path, "r") as file:
                if file_type == "json":
                    return json.load(file)
                elif file_type == "yaml":
                    return yaml.safe_load(file)
        except FileNotFoundError:
            _logger.error(f"File not found: {file_path}")
            exit(1)

    def _get_athena_table_task(
        self, node: dict, follow_external_dependency: bool = False
    ) -> dict:
        """Generate an athena table task for a DBT node."""
        task = ATHENA_TASK_BASE.copy()
        if follow_external_dependency:
            task["follow_external_dependency"] = True

        task["schema"] = node.get("schema", self._default_schema)
        task["table"] = node.get("name", "")
        task["name"] = f"{task['schema']}__{task['table']}_athena"

        return task

    @abstractmethod
    def _get_table_task(
        self, node: dict, follow_external_dependency: bool = False
    ) -> dict:
        """Generate a table task for a DBT node for the specific dbt-adapter. Must be implemented by subclasses."""
        pass

    @abstractmethod
    def _get_model_data_location(
        self, node: dict, schema: str, model_name: str
    ) -> Tuple[str, str]:
        """Get the S3 path of the DBT model relative to the data bucket. Must be implemented by subclasses."""
        pass

    @abstractmethod
    def _get_s3_task(self, node: dict, is_output: bool = False) -> dict:
        """
        Generate an S3 task for a DBT node for the specific dbt-adapter. Must be implemented by subclasses.
        """
        pass

    @staticmethod
    def _get_dummy_task(node: dict, follow_external_dependency: bool = False) -> dict:
        """
        Generates a dummy dagger task
        Args:
            node: The extracted node from the manifest.json file

        Returns:
            dict: The dummy dagger task for the DBT node

        """
        task = {}
        task["name"] = node.get("name", "")
        task["type"] = "dummy"

        if follow_external_dependency:
            task["follow_external_dependency"] = True

        return task

    @abstractmethod
    def _generate_dagger_output(self, node: dict):
        """Generate the dagger output for a DBT node. Must be implemented by subclasses."""
        pass

    @abstractmethod
    def _is_node_preparation_model(self, node: dict):
        """Define whether it is a preparation model. Must be implemented by subclasses."""
        pass

    def _generate_dagger_tasks(self, node_name: str) -> List[Dict]:
        """
        Generates dagger tasks based on the type and materialization of the DBT model node.

        - If the node is a DBT source, an Athena table task is generated.
        - If the node is an ephemeral model, a dummy task is generated, and tasks for its dependent nodes are recursively generated.
        - If the node is a staging model (preparation model) and not materialized as a table, a table task is generated along with tasks for its dependent nodes.
        - For other nodes, a table task is generated. If the node is materialized as a table, an additional S3 task is also generated.

        Args:
            node_name: The name of the DBT model node

        Returns:
            List[Dict]: The respective dagger tasks for the DBT model node
        """
        dagger_tasks = []

        if node_name.startswith("source"):
            node = self._sources_in_manifest[node_name]
        else:
            node = self._nodes_in_manifest[node_name]

        resource_type = node.get("resource_type")
        materialized_type = node.get("config", {}).get("materialized")

        follow_external_dependency = True
        if resource_type == "seed" or (self._is_node_preparation_model(node) and materialized_type != "table"):
            follow_external_dependency = False

        if resource_type == "source":
            table_task = self._get_athena_table_task(
                node, follow_external_dependency=follow_external_dependency
            )
            dagger_tasks.append(table_task)

        elif materialized_type == "ephemeral":
            task = self._get_dummy_task(node)
            dagger_tasks.append(task)
            for node_name in node.get("depends_on", {}).get("nodes", []):
                dagger_tasks += self._generate_dagger_tasks(node_name)

        else:
            table_task = self._get_table_task(node, follow_external_dependency=follow_external_dependency)
            dagger_tasks.append(table_task)

            if materialized_type in ("table", "incremental"):
                dagger_tasks.append(self._get_s3_task(node))
            elif self._is_node_preparation_model(node):
                for dependent_node_name in node.get("depends_on", {}).get("nodes", []):
                    dagger_tasks.extend(
                        self._generate_dagger_tasks(dependent_node_name)
                    )

        return dagger_tasks

    def generate_dagger_io(self, model_name: str) -> Tuple[List[dict], List[dict]]:
        """
        Parse through all the parents of the DBT model and return the dagger inputs and outputs for the DBT model
        Args:
            model_name: The name of the DBT model

        Returns:
            Tuple[list, list]: The dagger inputs and outputs for the DBT model

        """
        inputs_list = []
        model_node = self._nodes_in_manifest[f"model.main.{model_name}"]
        parent_node_names = model_node.get("depends_on", {}).get("nodes", [])

        for parent_node_name in parent_node_names:
            dagger_input = self._generate_dagger_tasks(parent_node_name)
            inputs_list += dagger_input

        output_list = self._generate_dagger_output(model_node)

        unique_inputs = list(
            OrderedDict(
                (frozenset(item.items()), item) for item in inputs_list
            ).values()
        )

        return unique_inputs, output_list


class AthenaDBTConfigParser(DBTConfigParser):
    """Implementation for Athena configurations."""

    def __init__(self, default_config_parameters: dict):
        super().__init__(default_config_parameters)
        self._profile_name = "athena"
        self._default_data_bucket = default_config_parameters.get("data_bucket")
        self._default_data_dir = self._target_config.get(
            "s3_data_dir"
        ) or self._target_config.get("s3_staging_dir")

    def _is_node_preparation_model(self, node: dict):
        """Define whether it is a preparation model."""
        return node.get("name").startswith("stg_")

    def _get_table_task(
        self, node: dict, follow_external_dependency: bool = False
    ) -> dict:
        """
        Generates the dagger athena task for the DBT model node
        """
        return self._get_athena_table_task(node, follow_external_dependency)

    def _get_model_data_location(
        self, node: dict, schema: str, model_name: str
    ) -> Tuple[str, str]:
        """
        Gets the S3 path of the dbt model relative to the data bucket.
        """
        location = node.get("config", {}).get("external_location")
        if not location:
            location = join(self._default_data_dir, schema, model_name)

        split = location.split("//")[1].split("/")
        bucket_name, data_path = split[0], "/".join(split[1:])

        return bucket_name, data_path

    def _get_s3_task(self, node: dict, is_output: bool = False) -> dict:
        """
        Generates the dagger s3 task for the athena-dbt model node
        """
        task = S3_TASK_BASE.copy()

        schema = node.get("schema", self._default_schema)
        table = node.get("name", "")
        task["name"] = f"output_s3_path" if is_output else f"s3_{table}"
        task["bucket"] = self._default_data_bucket
        _, task["path"] = self._get_model_data_location(node, schema, table)

        return task

    def _generate_dagger_output(self, node: dict):
        """
        Generates the dagger output for the DBT model node with athena-dbt adapter. If the model is materialized as a view or ephemeral, then a dummy task is created.
        Otherwise, an athena and s3 task is created for the DBT model node.
        Args:
            node: The extracted node from the manifest.json file

        Returns:
            dict: The dagger output, which is a combination of an athena and s3 task for the DBT model node

        """
        materialized_type = node.get("config", {}).get("materialized")
        if materialized_type == "ephemeral":
            return [self._get_dummy_task(node)]
        else:
            output_tasks = [self._get_table_task(node)]
            if materialized_type in ("table", "incremental"):
                output_tasks.append(self._get_s3_task(node, is_output=True))
            return output_tasks


class DatabricksDBTConfigParser(DBTConfigParser):
    """Implementation for Databricks configurations."""

    def __init__(self, default_config_parameters: dict):
        super().__init__(default_config_parameters)
        self._profile_name = "databricks"
        self._default_catalog = self._target_config.get("catalog")
        self._create_external_athena_table = default_config_parameters.get(
            "create_external_athena_table", False
        )

    def _is_node_preparation_model(self, node: dict):
        """
        Define whether it is a preparation model.
        """
        return "preparation" in node.get("schema", "")

    def _get_table_task(
        self, node: dict, follow_external_dependency: bool = False
    ) -> dict:
        """
        Generates the dagger databricks task for the DBT model node
        """
        task = DATABRICKS_TASK_BASE.copy()
        if follow_external_dependency:
            task["follow_external_dependency"] = True

        task["catalog"] = node.get("database", self._default_catalog)
        task["schema"] = node.get("schema", self._default_schema)
        task["table"] = node.get("name", "")
        task[
            "name"
        ] = f"{task['catalog']}__{task['schema']}__{task['table']}_databricks"

        return task

    def _get_model_data_location(
        self, node: dict, schema: str, model_name: str
    ) -> Tuple[str, str]:
        """
        Gets the S3 path of the dbt model relative to the data bucket.
        """
        location_root = node.get("config", {}).get("location_root")
        location = join(location_root, model_name)
        split = location.split("//")[1].split("/")
        bucket_name, data_path = split[0], "/".join(split[1:])

        return bucket_name, data_path

    def _get_s3_task(self, node: dict, is_output: bool = False) -> dict:
        """
        Generates the dagger s3 task for the databricks-dbt model node
        """
        task = S3_TASK_BASE.copy()

        schema = node.get("schema", self._default_schema)
        table = node.get("name", "")
        task["name"] = f"output_s3_path" if is_output else f"s3_{table}"
        task["bucket"], task["path"] = self._get_model_data_location(
            node, schema, table
        )

        return task

    def _generate_dagger_output(self, node: dict):
        """
        Generates the dagger output for the DBT model node with the databricks-dbt adapter.
        If the model is materialized as a view or ephemeral, then a dummy task is created.
        Otherwise, and databricks and s3 task is created for the DBT model node.
        And if create_external_athena_table is True te an extra athena task is created.
        Args:
            node: The extracted node from the manifest.json file

        Returns:
            dict: The dagger output, which is a combination of an athena and s3 task for the DBT model node

        """
        materialized_type = node.get("config", {}).get("materialized")
        if materialized_type == "ephemeral":
            return [self._get_dummy_task(node)]
        else:
            output_tasks = [self._get_table_task(node)]
            if materialized_type in ("table", "incremental"):
                output_tasks.append(self._get_s3_task(node, is_output=True))
            if self._create_external_athena_table:
                output_tasks.append(self._get_athena_table_task(node))
            return output_tasks
