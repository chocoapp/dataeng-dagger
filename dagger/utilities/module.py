import logging
from os import path
from mergedeep import merge
from dagger.utilities.dbt_config_parser import (
    AthenaDBTConfigParser,
    DatabricksDBTConfigParser,
)

import jinja2

import yaml

_logger = logging.getLogger("root")


class Module:
    def __init__(self, path_to_config, target_dir):
        self._directory = path.dirname(path_to_config)
        self._target_dir = target_dir or "./"
        self._path_to_config = path_to_config
        config = self.read_yaml(self.read_task_config(path_to_config))

        self._tasks = {}
        for task in config["tasks"]:
            self._tasks[task] = self.read_task_config(
                f"{path.join(self._directory, task)}.yaml"
            )

        self._branches_to_generate = config["branches_to_generate"]
        self._override_parameters = config.get("override_parameters", {})
        self._default_parameters = config.get("default_parameters", {})

    @staticmethod
    def read_yaml(yaml_str):
        try:
            yaml_obj = yaml.safe_load(yaml_str)
        except yaml.YAMLError as exc:
            _logger.error(f"Couldn't read config file {yaml_str}")
            exit(1)
        return yaml_obj

    def read_task_config(self, task):
        try:
            with open(task, "r") as myfile:
                content = myfile.read()
        except:
            _logger.error(f"Couldn't load task file: {task}")
            exit(1)
        return content

    @staticmethod
    def replace_template_parameters(_task_str, _template_parameters):
        environment = jinja2.Environment()
        template = environment.from_string(_task_str)
        rendered_task = template.render(_template_parameters)

        return (
            rendered_task
            # TODO Remove this hack and use Jinja escaping instead of special expression in template files
            .replace("__CBS__", "{")
            .replace("__CBE__", "}")
        )

    @staticmethod
    def dump_yaml(yaml_str, yaml_path):
        with open(yaml_path, "w") as stream:
            yaml.safe_dump(
                yaml_str, stream=stream, default_flow_style=False, sort_keys=False
            )

    def generate_task_configs(self):
        for branch_name, attrs in self._branches_to_generate.items():
            attrs = {} if attrs is None else attrs
            _logger.info(f"Generating tasks for branch {branch_name}")
            template_parameters = {}
            template_parameters.update(self._default_parameters or {})
            template_parameters.update(attrs)
            template_parameters['branch_name'] = branch_name

            dbt_manifest = None
            if "dbt" in self._tasks.keys():
                if template_parameters.get("profile_name") == "athena":
                    self._dbt_module = AthenaDBTConfigParser(template_parameters)
                if template_parameters.get("profile_name") == "databricks":
                    self._dbt_module = DatabricksDBTConfigParser(template_parameters)

                dbt_manifest = {}
                dbt_manifest['nodes'] = self._dbt_module.nodes_in_manifest
                dbt_manifest['sources'] = self._dbt_module.sources_in_manifest

                template_parameters["dbt_manifest"] = dbt_manifest
                template_parameters["dbt_default_schema"] = self._dbt_module.dbt_default_schema

            for task, task_yaml in self._tasks.items():
                task_name = f"{branch_name}_{task}"
                _logger.info(f"Generating task {task_name}")
                task_str = self.replace_template_parameters(
                    task_yaml, template_parameters
                )
                task_dict = yaml.safe_load(task_str)

                if task == "dbt":
                    inputs, outputs = self._dbt_module.generate_dagger_io(branch_name)
                    task_dict["inputs"] = inputs
                    task_dict["outputs"] = outputs
                    task_dict["task_parameters"]["select"] = branch_name

                task_dict["autogenerated_by_dagger"] = self._path_to_config
                override_parameters = self._override_parameters or {}
                merge(task_dict, override_parameters.get(branch_name, {}).get(task, {}))

                self.dump_yaml(
                    task_dict, f"{path.join(self._target_dir, task_name)}.yaml"
                )

    @staticmethod
    def module_config_template():
        return """
# The list of tasks that are used as a template for generating new tasks
tasks:
  - task1 # Name of the template task file in module directory (without yaml)
  - task2

## Optional: the list of default template parameters that going to be replaced in the task files for all branches
## E.g.: {template_parameter_name1} is going to be replaced with template_parameter_value1
## Default parameters can be overwritten in the branches_to_generate part
#
# default_parameters:
#     template_parameter_name1: template_parameter_value1
#     template_parameter_name2: template_parameter_value2

# The list of pipelines or branches of pipelines, going to be generated based on the above task list
# You can also define the branch specific parameters here which overrides the default parameters
# For each branch and for each task the <task-name>_<branch-name>
branches_to_generate:
  branch_name1: # branch_name1 uses only the default parameters
  branch_name2:
    template_parameter_name2: template_parameter_value2 # in branch_name2 the template_parameter_name2 is overwritten

## You can overwrite non-templated parameters of a task config for specific branches and tasks
#
#override_parameters:
#  branch_name1:
#    task1:
#      - "['task_parameters']['delete_target_dir'] = False"
        """
