from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DaggerBaseOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        pass

    def pre_execute(self, context):
        task_instance = context['task_instance']
        prev_ti = task_instance.get_previous_ti()
        if prev_ti:
            # Only enforce dependency if previous instance exists and failed
            if prev_ti.state not in ['success', 'skipped']:
                raise AirflowSkipException(
                    f"Previous task instance in state {prev_ti.state}"
                )
