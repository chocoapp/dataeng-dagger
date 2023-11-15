from os import path
from os.path import join
from typing import Union, Tuple
import json
import yaml

ATHENA_IO_BASE = {"type": "athena"}
S3_IO_BASE = {"type": "s3"}


class DBTConfigParser:
    """
    Module that parses the manifest.json file generated by dbt and generates the dagger inputs and outputs for the respective dbt model
    """

    def __init__(self, default_config_parameters: dict):
        self._default_data_bucket = default_config_parameters["data_bucket"]
        self._dbt_project_dir = default_config_parameters.get("project_dir", None)
        dbt_manifest_path = path.join(self._dbt_project_dir, "target", "manifest.json")
        self._dbt_profile_dir = default_config_parameters.get("profile_dir", None)
        dbt_profile_path = path.join(self._dbt_profile_dir, "profiles.yml")

        with open(dbt_manifest_path, "r") as f:
            data = f.read()
        self._manifest_data = json.loads(data)
        profile_yaml = yaml.safe_load(open(dbt_profile_path, "r"))
        prod_dbt_profile = profile_yaml[self._dbt_project_dir]["outputs"]["data"]
        self._default_data_dir = prod_dbt_profile.get(
            "s3_data_dir"
        ) or prod_dbt_profile.get("s3_staging_dir")

    def generate_io(self, model_name: str) -> tuple[list[dict], list[dict]]:
        """
        Generates the dagger inputs and outputs for the respective dbt model
        Args:
            model_name: name of the dbt model

        Returns:
            tuple[list[dict], list[dict]]: dagger inputs and outputs for the respective dbt model

        """
        model_parents = self._get_dbt_model_parents(model_name)
        model_dagger_inputs = self.generate_dagger_inputs(model_parents)
        model_dagger_outputs = self.generate_dagger_outputs(
            model_parents["model_name"],
            model_parents["schema"],
            model_parents["relative_s3_path"],
        )

        return model_dagger_inputs, model_dagger_outputs

    def parse_dbt_staging_model(self, dbt_staging_model: str) -> Tuple[str, str]:
        """
        Parses the dbt staging model to get the core schema and table name
        Args:
            dbt_staging_model: name of the DBT staging model

        Returns:
            Tuple[str, str]: core schema and table name
        """
        _model_split, core_table = dbt_staging_model.split("__")
        core_schema = _model_split.split("_")[-1]

        return core_schema, core_table

    def generate_dagger_inputs(
        self, dbt_model_parents: dict
    ) -> Union[list[dict], None]:
        """
        Generates the dagger inputs for the respective dbt model. This means that all parents of the dbt model are added as dagger inputs.
        Staging models are added as Athena inputs and core models are added as Athena and S3 inputs.
        Intermediate models are not added as an input.
        Args:
            dbt_model_parents: All parents of the dbt model

        Returns:
            Union[list[dict], None]: dagger inputs for the respective dbt model. If there are no parents, returns None

        """
        dagger_inputs = []
        for parent in dbt_model_parents["inputs"]:
            model_name = parent["model_name"]
            athena_input = ATHENA_IO_BASE.copy()
            s3_input = S3_IO_BASE.copy()

            if model_name.startswith("stg_"):
                athena_input["name"] = model_name
                (
                    athena_input["schema"],
                    athena_input["table"],
                ) = self.parse_dbt_staging_model(model_name)

                dagger_inputs.append(athena_input)
            else:
                athena_input["name"] = athena_input["table"] = model_name
                athena_input["schema"] = parent["schema"]

                s3_input["name"] = model_name
                s3_input["bucket"] = self._default_data_bucket
                s3_input["path"] = parent["relative_s3_path"]

                dagger_inputs.append(athena_input)
                dagger_inputs.append(s3_input)

        return dagger_inputs or None

    def generate_dagger_outputs(
        self, model_name: str, schema: str, relative_s3_path: str
    ) -> list[dict]:
        """
        Generates the dagger outputs for the respective dbt model.
        This means that an Athena and S3 output is added for the dbt model.
        Args:
            model_name: The name of the dbt model
            schema: The schema of the dbt model
            relative_s3_path: The S3 path of the dbt model relative to the data bucket

        Returns:
            list[dict]: dagger S3 and Athena outputs for the respective dbt model

        """
        athena_input = ATHENA_IO_BASE.copy()
        s3_input = S3_IO_BASE.copy()

        athena_input["name"] = athena_input["table"] = s3_input["name"] = model_name
        athena_input["schema"] = schema

        s3_input["bucket"] = "cho${ENV}-data-lake"
        s3_input["relative_s3_path"] = relative_s3_path

        return [athena_input, s3_input]

    def _get_model_data_location(
        self, node: dict, schema: str, dbt_model_name: str
    ) -> str:
        """
        Gets the S3 path of the dbt model relative to the data bucket.
        If external location is not specified in the DBT model config, then the default data directory from the
        DBT profiles configuration is used.
        Args:
            node: The extracted node from the manifest.json file
            schema: The schema of the dbt model
            dbt_model_name: The name of the dbt model

        Returns:
            str: The S3 path of the dbt model relative to the data bucket

        """
        location = node.get("unrendered_config", {}).get("external_location")
        if not location:
            location = join(self._default_data_dir, schema, dbt_model_name)

        return location.split("data-lake/")[1]

    def _get_dbt_model_parents(self, model_name: str) -> dict:
        """
        Gets all parents of a single dbt model from the manifest.json file
        Args:
            model_name: The name of the DBT model

        Returns:
            dict: All parents of the dbt model along with the name, schema and S3 path of the dbt model itself

        """
        inputs_dict = {}
        inputs_list = []
        dbt_ref_to_model = f"model.{self._dbt_project_dir}.{model_name}"

        nodes = self._manifest_data["nodes"]
        model_info = nodes[f"model.main.{model_name}"]

        parents_as_full_selectors = model_info.get("depends_on", {}).get("nodes", [])
        inputs = [x.split(".")[-1] for x in parents_as_full_selectors]

        for index, node_name in enumerate(parents_as_full_selectors):
            if not (".int_" in node_name):
                dbt_parent_model_name = node_name.split(".")[-1]
                parent_model_node = nodes.get(node_name)
                parent_schema = parent_model_node.get("schema")

                model_data_location = self._get_model_data_location(
                    parent_model_node, parent_schema, dbt_parent_model_name
                )

                inputs_list.append(
                    {
                        "schema": parent_schema,
                        "model_name": inputs[index],
                        "relative_s3_path": model_data_location,
                    }
                )

        inputs_dict["model_name"] = model_name
        inputs_dict["node_name"] = dbt_ref_to_model
        inputs_dict["inputs"] = inputs_list
        inputs_dict["schema"] = model_info["schema"]
        inputs_dict["relative_s3_path"] = self._get_model_data_location(
            model_info, model_info["schema"], model_name
        )

        return inputs_dict
