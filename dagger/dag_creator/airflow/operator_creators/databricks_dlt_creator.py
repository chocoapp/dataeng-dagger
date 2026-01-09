"""Operator creator for Databricks DLT (Delta Live Tables) pipelines."""

import logging
from typing import Any

from airflow.models import BaseOperator, DAG

from dagger.dag_creator.airflow.operator_creator import OperatorCreator
from dagger.pipeline.tasks.databricks_dlt_task import DatabricksDLTTask

_logger = logging.getLogger(__name__)


def _cancel_databricks_run(context: dict[str, Any]) -> None:
    """Cancel a Databricks job run when task fails or is cleared.

    This callback retrieves the run_id from XCom and cancels the corresponding
    Databricks job run. Used as on_failure_callback to ensure jobs are cancelled
    when tasks are marked as failed.

    Args:
        context: Airflow context dictionary containing task instance and other metadata.
    """
    from airflow.providers.databricks.hooks.databricks import DatabricksHook

    ti = context.get("task_instance")
    if not ti:
        _logger.warning("No task instance in context, cannot cancel Databricks run")
        return

    # Get run_id from XCom (pushed by DatabricksRunNowOperator)
    run_id = ti.xcom_pull(task_ids=ti.task_id, key="run_id")
    if not run_id:
        _logger.warning(f"No run_id found in XCom for task {ti.task_id}")
        return

    # Get the databricks_conn_id from the operator
    databricks_conn_id = getattr(ti.task, "databricks_conn_id", "databricks_default")

    try:
        hook = DatabricksHook(databricks_conn_id=databricks_conn_id)
        hook.cancel_run(run_id)
        _logger.info(f"Cancelled Databricks run {run_id} for task {ti.task_id}")
    except Exception as e:
        _logger.error(f"Failed to cancel Databricks run {run_id}: {e}")


class DatabricksDLTCreator(OperatorCreator):
    """Creates operators for triggering Databricks DLT pipelines via Jobs.

    This creator uses DatabricksRunNowOperator to trigger a Databricks Job
    that wraps the DLT pipeline. The job is identified by name and must be
    defined in the Databricks Asset Bundle.

    Attributes:
        ref_name: Reference name used by OperatorFactory to match this creator
            with DatabricksDLTTask instances.
    """

    ref_name: str = "databricks_dlt"

    def __init__(self, task: DatabricksDLTTask, dag: DAG) -> None:
        """Initialize the DatabricksDLTCreator.

        Args:
            task: The DatabricksDLTTask containing pipeline configuration.
            dag: The Airflow DAG this operator will belong to.
        """
        super().__init__(task, dag)

    def _create_operator(self, **kwargs: Any) -> BaseOperator:
        """Create a DatabricksRunNowOperator for the DLT pipeline.

        Creates an Airflow operator that triggers an existing Databricks Job
        by name. The job must have a pipeline_task that references the DLT
        pipeline.

        Args:
            **kwargs: Additional keyword arguments passed to the operator.

        Returns:
            A configured DatabricksRunNowOperator instance.
        """
        # Import here to avoid import errors if databricks provider not installed
        from datetime import timedelta

        from airflow.providers.databricks.operators.databricks import (
            DatabricksRunNowOperator,
        )

        # Get task parameters
        job_name: str = self._task.job_name
        databricks_conn_id: str = getattr(
            self._task, "databricks_conn_id", "databricks_default"
        )
        wait_for_completion: bool = getattr(self._task, "wait_for_completion", True)
        poll_interval_seconds: int = getattr(self._task, "poll_interval_seconds", 30)
        timeout_seconds: int = getattr(self._task, "timeout_seconds", 3600)

        # DatabricksRunNowOperator triggers an existing Databricks Job by name
        # The job must have a pipeline_task that references the DLT pipeline
        # Note: timeout is handled via Airflow's execution_timeout, not a direct parameter
        # Note: on_kill() is already implemented in DatabricksRunNowOperator to cancel runs
        # We add on_failure_callback to also cancel when task is marked as failed
        operator: BaseOperator = DatabricksRunNowOperator(
            dag=self._dag,
            task_id=self._task.name,
            databricks_conn_id=databricks_conn_id,
            job_name=job_name,
            wait_for_termination=wait_for_completion,
            polling_period_seconds=poll_interval_seconds,
            execution_timeout=timedelta(seconds=timeout_seconds),
            do_xcom_push=True,  # Required to store run_id for cancellation callback
            on_failure_callback=_cancel_databricks_run,
            **kwargs,
        )

        return operator
