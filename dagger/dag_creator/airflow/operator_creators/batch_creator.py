from pathlib import Path
from datetime import timedelta

from dagger.dag_creator.airflow.operator_creator import OperatorCreator
from dagger.dag_creator.airflow.operators.awsbatch_operator import AWSBatchOperator
from dagger import conf


class BatchCreator(OperatorCreator):
    ref_name = "batch"

    def __init__(self, task, dag):
        super().__init__(task, dag)

    @staticmethod
    def _validate_job_name(job_name, absolute_job_name):
        if not absolute_job_name and not job_name:
            raise Exception("Both job_name and absolute_job_name cannot be null")

        if absolute_job_name is not None:
            return absolute_job_name

        job_path = Path(conf.DAGS_DIR) / job_name.replace("-", "/")
        assert (
            job_path.is_dir()
        ), f"Job name `{job_name}`, points to a non-existing folder `{job_path}`"
        return job_name

    def _generate_command(self):
        command = [self._task.executable_prefix, self._task.executable]
        for param_name, param_value in self._template_parameters.items():
            command.append(
                "--{name}={value}".format(name=param_name, value=param_value)
            )

        return command

    def _create_operator(self, **kwargs):
        overrides = self._task.overrides
        overrides.update({"command": self._generate_command()})

        job_name = self._validate_job_name(self._task.job_name, self._task.absolute_job_name)
        batch_op = AWSBatchOperator(
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
