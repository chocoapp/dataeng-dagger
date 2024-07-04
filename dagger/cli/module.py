import click
from dagger.utilities.module import Module
from dagger.utils import Printer
import json


def parse_key_value(ctx, param, value):
    #print('YYY', value)
    if not value:
        return {}
    key_value_dict = {}
    for pair in value:
        try:
            key, val_file_path = pair.split('=', 1)
            #print('YYY', key, val_file_path, pair)
            val = json.load(open(val_file_path))
            key_value_dict[key] = val
        except ValueError:
            raise click.BadParameter(f"Key-value pair '{pair}' is not in the format key=value")
    return key_value_dict

@click.command()
@click.option("--config_file", "-c", help="Path to module config file")
@click.option("--target_dir", "-t", help="Path to directory to generate the task configs to")
@click.option("--jinja_parameters", "-j", callback=parse_key_value, multiple=True, default=None, help="Path to jinja parameters json file in the format: <jinja_variable_name>=<path to json file>")
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
