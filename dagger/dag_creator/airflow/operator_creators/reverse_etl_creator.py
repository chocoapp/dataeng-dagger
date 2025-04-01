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
        self._is_deleted_column = task.is_deleted_column
        self._hash_column = task.hash_column
        self._updated_at_column = task.updated_at_column
        self._from_time = task.from_time
        self._days_to_live = task.days_to_live
        self._output_type = task.output_type
        self._region_name = task.region_name
        self._full_refresh = task.full_refresh
        self._target_case = task.target_case
        self._source_case = task.source_case
        self._column_mapping = task.column_mapping
        self._glue_registry_name = self.parse_attribute("glue_registry_name")
        self._glue_schema_name = self.parse_attribute("glue_schema_name")
        self._sort_key = self.parse_attribute("sort_key")
        self._custom_columns = self.parse_attribute("custom_columns")

    def _generate_command(self):
        command = BatchCreator._generate_command(self)

        command.append(f"--num_threads={self._num_threads}")
        command.append(f"--batch_size={self._batch_size}")
        command.append(f"--primary_id_column={self._primary_id_column}")
        command.append(f"--output_type={self._output_type}")
        command.append(f"--glue_registry_name={self._glue_registry_name}")

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
        if self._region_name:
            command.append(f"--region_name={self._region_name}")
        if self._full_refresh:
            command.append(f"--full_refresh={self._full_refresh}")
        if self._target_case:
            command.append(f"--target_case={self._target_case}")
        if self._source_case:
            command.append(f"--source_case={self._source_case}")
        if self._column_mapping:
            command.append(f"--column_mapping={self._column_mapping}")
        if self._glue_schema_name:
            command.append(f"--glue_schema_name={self._glue_schema_name}")
        if self._sort_key:
            command.append(f"--sort_key={self._sort_key}")
        if self._custom_columns:
            command.append(f"--custom_columns={json.dumps(self._custom_columns)}")


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
