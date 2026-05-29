"""Task configuration for declarative pipelines (DLT/Delta Live Tables)."""

from typing import Any, Optional

from dagger.pipeline.task import Task
from dagger.utilities.config_validator import Attribute


class DeclarativePipelineTask(Task):
    """Task configuration for triggering declarative pipelines via Databricks Jobs.

    This task type uses DatabricksRunNowOperator to trigger a Databricks Job
    that wraps the DLT pipeline. The job is identified by name and must be
    defined in the Databricks Asset Bundle.

    Attributes:
        ref_name: Reference name used by TaskFactory to instantiate this task type.
        job_name: Databricks Job name that triggers the DLT pipeline.
        databricks_conn_id: Airflow connection ID for Databricks.
        wait_for_completion: Whether to wait for job completion.
        poll_interval_seconds: Polling interval in seconds.
        timeout_seconds: Timeout in seconds.
        cancel_on_kill: Whether to cancel Databricks job if Airflow task is killed.
        deferrable: Whether the operator defers to the triggerer while waiting.

    Example YAML configuration:
        type: declarative_pipeline
        description: Run DLT pipeline users
        inputs:
          - type: s3
            name: input_order_service_public_users
            bucket: cho${ENV}-data-lake
            path: pg_changelogs/kafka/order-service/order_service.public.users
        outputs:
          - type: databricks
            catalog: changelogs
            schema: order_service_public
            table: pg_users
          - type: databricks
            catalog: core
            schema: order_service
            table: pg_users
        task_parameters:
          job_name: "[JOB] order-service-pipeline"
          databricks_conn_id: databricks_default
          wait_for_completion: true
          poll_interval_seconds: 30
          timeout_seconds: 3600
          deferrable: true
    """

    ref_name: str = "declarative_pipeline"

    @classmethod
    def init_attributes(cls, orig_cls: type) -> None:
        """Initialize configuration attributes for YAML parsing.

        Registers all task_parameters attributes that can be specified in the
        YAML configuration file. Called by the Task metaclass during class creation.

        Args:
            orig_cls: The original class being initialized (used for attribute registration).
        """
        cls.add_config_attributes(
            [
                Attribute(
                    attribute_name="job_name",
                    parent_fields=["task_parameters"],
                    comment="Databricks Job name that triggers the DLT pipeline",
                ),
                Attribute(
                    attribute_name="databricks_conn_id",
                    parent_fields=["task_parameters"],
                    required=False,
                    comment="Airflow connection ID for Databricks (default: databricks_default)",
                ),
                Attribute(
                    attribute_name="wait_for_completion",
                    parent_fields=["task_parameters"],
                    required=False,
                    validator=bool,
                    comment="Wait for job to complete (default: true)",
                ),
                Attribute(
                    attribute_name="poll_interval_seconds",
                    parent_fields=["task_parameters"],
                    required=False,
                    validator=int,
                    comment="Polling interval in seconds (default: 30)",
                ),
                Attribute(
                    attribute_name="timeout_seconds",
                    parent_fields=["task_parameters"],
                    required=False,
                    validator=int,
                    comment="Timeout in seconds (default: 3600)",
                ),
                Attribute(
                    attribute_name="cancel_on_kill",
                    parent_fields=["task_parameters"],
                    required=False,
                    validator=bool,
                    comment="Cancel Databricks job if Airflow task is killed (default: true)",
                ),
                Attribute(
                    attribute_name="deferrable",
                    parent_fields=["task_parameters"],
                    required=False,
                    validator=bool,
                    comment="Defer to the triggerer while waiting for the run (default: false)",
                ),
            ]
        )

    def __init__(
        self,
        name: str,
        pipeline_name: str,
        pipeline: Any,
        job_config: dict[str, Any],
    ) -> None:
        """Initialize a DeclarativePipelineTask instance.

        Args:
            name: The task name (used as task_id in Airflow).
            pipeline_name: Name of the Dagger pipeline this task belongs to.
            pipeline: The parent Pipeline object.
            job_config: Dictionary containing the task configuration from YAML.
        """
        super().__init__(name, pipeline_name, pipeline, job_config)

        self._job_name: str = self.parse_attribute("job_name")
        self._databricks_conn_id: str = (
            self.parse_attribute("databricks_conn_id") or "databricks_default"
        )
        wait_for_completion: Optional[bool] = self.parse_attribute("wait_for_completion")
        self._wait_for_completion: bool = (
            wait_for_completion if wait_for_completion is not None else True
        )
        self._poll_interval_seconds: int = (
            self.parse_attribute("poll_interval_seconds") or 30
        )
        self._timeout_seconds: int = self.parse_attribute("timeout_seconds") or 3600
        cancel_on_kill: Optional[bool] = self.parse_attribute("cancel_on_kill")
        self._cancel_on_kill: bool = (
            cancel_on_kill if cancel_on_kill is not None else True
        )
        self._deferrable: bool = self.parse_attribute("deferrable") or False

    @property
    def job_name(self) -> str:
        """Databricks Job name that triggers the DLT pipeline."""
        return self._job_name

    @property
    def databricks_conn_id(self) -> str:
        """Airflow connection ID for Databricks."""
        return self._databricks_conn_id

    @property
    def wait_for_completion(self) -> bool:
        """Whether to wait for job completion."""
        return self._wait_for_completion

    @property
    def poll_interval_seconds(self) -> int:
        """Polling interval in seconds."""
        return self._poll_interval_seconds

    @property
    def timeout_seconds(self) -> int:
        """Timeout in seconds."""
        return self._timeout_seconds

    @property
    def cancel_on_kill(self) -> bool:
        """Whether to cancel Databricks job if Airflow task is killed."""
        return self._cancel_on_kill

    @property
    def deferrable(self) -> bool:
        """Whether the operator defers to the triggerer while waiting for the run."""
        return self._deferrable
