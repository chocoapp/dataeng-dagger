import base64

from dagger.dag_creator.airflow.operator_creators.batch_creator import BatchCreator
from dagger.dag_creator.airflow.operators.reverse_etl_batch import ReverseEtlBatchOperator
import json


class ReverseEtlCreator(BatchCreator):
    ref_name = "reverse_etl"

    def __init__(self, task, dag):
        super().__init__(task, dag)

        self._assume_role_arn = task.assume_role_arn
        self._num_threads = task.num_threads
        self._batch_size = task.batch_size
        self._absolute_job_name = task.absolute_job_name
        self._primary_id_column = task.primary_id_column
        self._secondary_id_column = task.secondary_id_column
        self._custom_id_column = task.custom_id_column
        self._model_name = task.model_name
        self._project_name = task.project_name
        self._is_deleted_column = task.is_deleted_column
        self._hash_column = task.hash_column
        self._updated_at_column = task.updated_at_column
        self._from_time = task.from_time
        self._days_to_live = task.days_to_live

    def _generate_command(self):
        command = BatchCreator._generate_command(self)

        command.append(f"--num_threads={self._num_threads}")
        command.append(f"--batch_size={self._batch_size}")
        command.append(f"--primary_id_column={self._primary_id_column}")
        command.append(f"--model_name={self._model_name}")
        command.append(f"--project_name={self._project_name}")

        if self._assume_role_arn:
            command.append(f"--assume_role_arn={self._assume_role_arn}")
        if self._secondary_id_column:
            command.append(f"--secondary_id_column={self._secondary_id_column}")
        if self._custom_id_column:
            command.append(f"--custom_id_column={self._custom_id_column}")
        if self._is_deleted_column:
            command.append(f"--is_deleted_column={self._is_deleted_column}")
        if self._hash_column:
            command.append(f"--hash_column={self._hash_column}")
        if self._updated_at_column:
            command.append(f"--updated_at_column={self._updated_at_column}")
        if self._from_time:
            command.append(f"--from_time={self._from_time}")
        if self._days_to_live:
            command.append(f"--days_to_live={self._days_to_live}")

        return command

    def _create_operator(self, **kwargs):
        overrides = self._task.overrides
        overrides.update({"command": self._generate_command()})

        job_name = self._validate_job_name(self._task.job_name, self._task.absolute_job_name)
        batch_op = ReverseEtlBatchOperator(
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
