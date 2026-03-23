import json

import click
import yaml

from dagger.utilities.module import Module
from dagger.utils import Printer


def parse_key_value(ctx, param, value):
    """Parse key=value pairs where value is a path to JSON or YAML file.

    Args:
        ctx: Click context.
        param: Click parameter.
        value: List of key=value pairs.

    Returns:
        Dictionary mapping variable names to parsed file contents.
    """
    if not value:
        return {}
    key_value_dict = {}
    for pair in value:
        try:
            key, val_file_path = pair.split('=', 1)
            with open(val_file_path, 'r') as f:
                if val_file_path.endswith(('.yaml', '.yml')):
                    val = yaml.safe_load(f)
                else:
                    val = json.load(f)
            key_value_dict[key] = val
        except ValueError:
            raise click.BadParameter(f"Key-value pair '{pair}' is not in the format key=value")
    return key_value_dict

@click.command()
@click.option("--config_file", "-c", help="Path to module config file")
@click.option("--target_dir", "-t", help="Path to directory to generate the task configs to")
@click.option("--jinja_parameters", "-j", callback=parse_key_value, multiple=True, default=None, help="Jinja parameters file in the format: <var_name>=<path to json/yaml file>")
def generate_tasks(config_file: str, target_dir: str, jinja_parameters: dict) -> None:
    """
    Generating tasks for a module based on config
    """

    module = Module(config_file, target_dir, jinja_parameters)
    module.generate_task_configs()

    Printer.print_success("Tasks are successfully generated")


@click.command()
def module_config() -> None:
    """
    Printing module config file
    """
    Printer.print_success(Module.module_config_template())
