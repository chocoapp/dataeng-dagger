import base64

from dagger.dag_creator.airflow.operator_creators.batch_creator import BatchCreator
from dagger.dag_creator.airflow.operators.soda_batch import SodaBatchOperator
import json


class SodaCreator(BatchCreator):
    ref_name = "soda"

    def __init__(self, task, dag):
        super().__init__(task, dag)

        self._absolute_job_name = task.absolute_job_name
        self._project_dir = task.project_dir
        self._profiles_dir = task.profiles_dir
        self._profile_name = task.profile_name
        self._target_name = task.target_name
        self._table_name = task.table_name
        self._model_name = task.model_name
        self._output_s3_path = task.output_s3_path
        self._output_table = task.output_table
        self._vars = task.vars

    def _generate_command(self):
        command = BatchCreator._generate_command(self)

        command.append(f"--project_dir={self._project_dir}")
        command.append(f"--profiles_dir={self._profiles_dir}")
        command.append(f"--profile_name={self._profile_name}")
        command.append(f"--target_name={self._target_name}")
        command.append(f"--output_s3_path={self._output_s3_path}")
        command.append(f"--output_table={self._output_table}")

        if self._table_name:
            command.append(f"--table_name={self._table_name}")
        if self._model_name:
            command.append(f"--model_name={self._model_name}")
        if self._vars:
            command.append(f"--vars={self._vars}")
        return command

    def _create_operator(self, **kwargs):
        overrides = self._task.overrides
        overrides.update({"command": self._generate_command()})

        job_name = self._validate_job_name(self._task.job_name, self._task.absolute_job_name)
        batch_op = SodaBatchOperator(
            dag=self._dag,
            task_id=self._task.name,
            job_name=self._task.name,
            job_definition=job_name,
            region_name=self._task.region_name,
            job_queue=self._task.job_queue,
            container_overrides=overrides,
            awslogs_enabled=True,
            **kwargs,
        )
        return batch_op
