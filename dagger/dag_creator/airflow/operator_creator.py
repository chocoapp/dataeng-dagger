from abc import ABC, abstractmethod
from datetime import timedelta
from airflow.utils.task_group import TaskGroup

TIMEDELTA_PARAMETERS = ['execution_timeout']


class OperatorCreator(ABC):
    def __init__(self, task, dag):
        self._task = task
        self._dag = dag
        self._template_parameters = {}
        self._airflow_parameters = {}

    def _get_existing_task_group_or_create_new(self):
        group_id = self._task.task_group
        if self._dag.task_group:
            for group in self._dag.task_group.children.values():
                if isinstance(group, TaskGroup) and group.group_id == group_id:
                    return group

        return TaskGroup(group_id=group_id, dag=self._dag)

    @abstractmethod
    def _create_operator(self, kwargs):
        raise NotImplementedError

    def _update_template_with_ios(self, ios):
        for io in ios:
            self._template_parameters[io.name] = io.rendered_name

    def _fix_timedelta_parameters(self):
        for timedelta_parameter in TIMEDELTA_PARAMETERS:
            if self._airflow_parameters.get(timedelta_parameter) is not None:
                self._airflow_parameters[timedelta_parameter] =\
                    timedelta(seconds=self._airflow_parameters[timedelta_parameter])

    def _update_airflow_parameters(self):
        self._airflow_parameters.update(self._task.airflow_parameters)

        if self._task.pool:
            self._airflow_parameters["pool"] = self._task.pool

        if self._task.timeout_in_seconds:
            self._airflow_parameters["execution_timeout"] = self._task.timeout_in_seconds

        if self._task.task_group:
            self._airflow_parameters["task_group"] = self._get_existing_task_group_or_create_new()

        self._fix_timedelta_parameters()

    def create_operator(self):
        self._template_parameters.update(self._task.template_parameters)
        self._update_airflow_parameters()
        self._update_template_with_ios(self._task.inputs)
        self._update_template_with_ios(self._task.outputs)

        return self._create_operator(**self._airflow_parameters)
