import base64

from dagger.dag_creator.airflow.operator_creators.batch_creator import BatchCreator
import json


class DbtCreator(BatchCreator):
    ref_name = "dbt"

    def __init__(self, task, dag):
        super().__init__(task, dag)

        self._project_dir = task.project_dir
        self._profile_dir = task.profile_dir
        self._profile_name = task.profile_name
        self._target_name = task.target_name
        self._select = task.select
        self._dbt_command = task.dbt_command
        self._vars = task.vars
        # self._create_external_athena_table = task.create_external_athena_table

    def _generate_command(self):
        command = [self._task.executable_prefix, self._task.executable]
        command.append(f"--project_dir={self._project_dir}")
        command.append(f"--profiles_dir={self._profile_dir}")
        command.append(f"--profile_name={self._profile_name}")
        command.append(f"--target_name={self._target_name}")
        command.append(f"--dbt_command={self._dbt_command}")
        if self._select:
            command.append(f"--select={self._select}")
        if self._vars:
            dbt_vars = json.dumps(self._vars)
            command.append(f"--vars='{dbt_vars}'")
        # if self._create_external_athena_table:
        #     command.append(f"--create_external_athena_table={self._create_external_athena_table}")

        return command
