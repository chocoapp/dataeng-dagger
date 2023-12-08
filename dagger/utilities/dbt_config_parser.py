import json
from collections import OrderedDict
from os import path
from os.path import join
from typing import Tuple, List, Dict

import yaml

ATHENA_TASK_BASE = {"type": "athena"}
S3_TASK_BASE = {"type": "s3"}


class DBTConfigParser:
    """
    Module that parses the manifest.json file generated by dbt and generates the dagger inputs and outputs for the respective dbt model
    """

    def __init__(self, default_config_parameters: dict):
        self._dbt_profile = default_config_parameters.get("dbt_profile", "data")
        self._default_data_bucket = default_config_parameters["data_bucket"]
        self._dbt_project_dir = default_config_parameters.get("project_dir", None)
        dbt_manifest_path = path.join(self._dbt_project_dir, "target", "manifest.json")
        self._dbt_profile_dir = default_config_parameters.get("profile_dir", None)
        dbt_profile_path = path.join(self._dbt_profile_dir, "profiles.yml")

        with open(dbt_manifest_path, "r") as f:
            data = f.read()
        self._manifest_data = json.loads(data)
        profile_yaml = yaml.safe_load(open(dbt_profile_path, "r"))
        prod_dbt_profile = profile_yaml[self._dbt_project_dir.split("/")[-1]][
            "outputs"
        ][self._dbt_profile]
        self._default_data_dir = prod_dbt_profile.get(
            "s3_data_dir"
        ) or prod_dbt_profile.get("s3_staging_dir")
        self._default_schema = prod_dbt_profile.get("schema")

        self._nodes_in_manifest = self._manifest_data["nodes"]
        self._sources_in_manifest = self._manifest_data["sources"]

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

    def _get_athena_task(
        self, node: dict, follow_external_dependency: bool = False
    ) -> dict:
        """
        Generates the dagger athena task for the DBT model node
        Args:
            node: The extracted node from the manifest.json file
            follow_external_dependency: Whether to follow external airflow dependencies or not

        Returns:
            dict: The dagger athena task for the DBT model node

        """
        task = ATHENA_TASK_BASE.copy()
        if follow_external_dependency:
            task["follow_external_dependency"] = True

        task["schema"] = node.get("schema", self._default_schema)
        task["table"] = node.get("name", "")
        task["name"] = f"{task['schema']}__{task['table']}_athena"

        return task

    def _get_s3_task(self, node: dict) -> dict:
        """
        Generates the dagger s3 task for the DBT model node
        Args:
            node: The extracted node from the manifest.json file

        Returns:
            dict: The dagger s3 task for the DBT model node

        """
        task = S3_TASK_BASE.copy()

        schema = node.get("schema", self._default_schema)
        table = node.get("name", "")
        task["name"] = f"{schema}__{table}_s3"
        task["bucket"] = self._default_data_bucket
        task["path"] = self._get_model_data_location(node, schema, table)[1]

        return task

    def _generate_dagger_output(self, node: dict):
        """
        Generates the dagger output for the DBT model node. If the model is materialized as a view or ephemeral, then a dummy task is created.
        Otherwise, an athena and s3 task is created for the DBT model node.
        Args:
            node: The extracted node from the manifest.json file

        Returns:
            dict: The dagger output, which is a combination of an athena and s3 task for the DBT model node

        """
        if node.get("config", {}).get("materialized") in (
            "view",
            "ephemeral",
        ) or node.get("name").startswith("stg_"):
            return [self._get_dummy_task(node)]
        else:
            return [self._get_athena_task(node), self._get_s3_task(node)]

    def _generate_dagger_tasks(
        self,
        node_name: str,
    ) -> List[Dict]:
        """
        Generates the dagger task based on whether the DBT model node is a staging model or not.
        If the DBT model node represents a DBT seed or an ephemeral model, then a dagger dummy task is generated.
        If the DBT model node represents a staging model, then a dagger athena task is generated for each source of the DBT model. Apart from this, a dummy task is also generated for the staging model itself.
        If the DBT model node is not a staging model, then a dagger athena task and an s3 task is generated for the DBT model node itself.
        Args:
            node: The extracted node from the manifest.json file

        Returns:
            List[Dict]: The respective dagger tasks for the DBT model node

        """
        dagger_tasks = []

        if node_name.startswith("source"):
            node = self._sources_in_manifest[node_name]
        else:
            node = self._nodes_in_manifest[node_name]

        if node.get("resource_type") == "seed":
            task = self._get_dummy_task(node)
            dagger_tasks.append(task)
        elif node.get("resource_type") == "source":
            athena_task = self._get_athena_task(node, follow_external_dependency=True)
            dagger_tasks.append(athena_task)
        elif node.get("config", {}).get("materialized") == "ephemeral":
            task = self._get_dummy_task(node, follow_external_dependency=True)
            dagger_tasks.append(task)
        elif node.get("name").startswith("stg_"):
            dagger_tasks.append(
                self._get_dummy_task(node, follow_external_dependency=True)
            )
        else:
            athena_task = self._get_athena_task(node, follow_external_dependency=True)
            s3_task = self._get_s3_task(node)

            dagger_tasks.append(athena_task)
            dagger_tasks.append(s3_task)

        return dagger_tasks

    def _get_model_data_location(
        self, node: dict, schema: str, dbt_model_name: str
    ) -> Tuple[str, str]:
        """
        Gets the S3 path of the dbt model relative to the data bucket.
        If external location is not specified in the DBT model config, then the default data directory from the
        DBT profiles configuration is used.
        Args:
            node: The extracted node from the manifest.json file
            schema: The schema of the dbt model
            dbt_model_name: The name of the dbt model

        Returns:
            str: The relative S3 path of the dbt model relative to the data bucket

        """
        location = node.get("config", {}).get("external_location")
        if not location:
            location = join(self._default_data_dir, schema, dbt_model_name)

        split = location.split("//")[1].split("/")
        bucket_name, data_path = split[0], "/".join(split[1:])

        return bucket_name, data_path

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
