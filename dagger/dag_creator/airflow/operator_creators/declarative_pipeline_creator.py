"""Operator creator for declarative pipelines (DLT/Delta Live Tables)."""

import logging
from typing import Any, Callable, Optional

from airflow.models import BaseOperator, DAG

from dagger.dag_creator.airflow.operator_creator import OperatorCreator
from dagger.pipeline.tasks.declarative_pipeline_task import DeclarativePipelineTask

_logger = logging.getLogger(__name__)


def _cancel_databricks_run(context: dict[str, Any]) -> None:
    """Cancel a Databricks job run when task fails or is cleared.

    This callback retrieves the run_id from XCom and cancels the corresponding
    Databricks job run. Used as on_failure_callback to ensure jobs are cancelled
    when tasks are marked as failed.

    Args:
        context: Airflow context dictionary containing task instance and other metadata.
    """
    ti = context.get("task_instance")
    if not ti:
        _logger.warning("No task instance in context, cannot cancel Databricks run")
        return

    # Get run_id from XCom (pushed by DatabricksRunNowOperator)
    run_id = ti.xcom_pull(task_ids=ti.task_id, key="run_id")
    if not run_id:
        _logger.warning(f"No run_id found in XCom for task {ti.task_id}")
        return

    # Get the databricks_conn_id from the operator (set during operator creation)
    databricks_conn_id = ti.task.databricks_conn_id

    # Import here to avoid import errors if databricks provider not installed
    # and to only import when actually needed (after early returns)
    try:
        from airflow.providers.databricks.hooks.databricks import DatabricksHook

        hook = DatabricksHook(databricks_conn_id=databricks_conn_id)
        hook.cancel_run(run_id)
        _logger.info(f"Cancelled Databricks run {run_id} for task {ti.task_id}")
    except ImportError:
        _logger.error(
            "airflow-providers-databricks is not installed, cannot cancel run"
        )
    except Exception as e:
        _logger.error(f"Failed to cancel Databricks run {run_id}: {e}")


def _build_failure_callback(
    dag_callback: Optional[Callable[[dict[str, Any]], None]],
) -> Callable[[dict[str, Any]], None]:
    """Build a failure callback that cancels the Databricks run and invokes the DAG-level callback.

    When a DAG defines an ``on_failure_callback`` in its ``default_args`` (e.g. for
    Slack alerts), that callback is normally overridden by operator-level callbacks.
    This helper produces a single callback that always cancels the Databricks run
    **and** forwards to the DAG-level callback so that alerts still fire.

    Args:
        dag_callback: The DAG-level ``on_failure_callback`` (from ``default_args``),
            or ``None`` if the DAG has no failure callback configured.

    Returns:
        A callback suitable for ``on_failure_callback`` on the operator.
    """
    if dag_callback is None:
        return _cancel_databricks_run

    def _composite_failure_callback(context: dict[str, Any]) -> None:
        _cancel_databricks_run(context)
        dag_callback(context)

    return _composite_failure_callback


class DeclarativePipelineCreator(OperatorCreator):
    """Creates operators for triggering declarative pipelines via Databricks Jobs.

    This creator uses DatabricksRunNowOperator to trigger a Databricks Job
    that wraps the DLT pipeline. The job is identified by name and must be
    defined in the Databricks Asset Bundle.

    Attributes:
        ref_name: Reference name used by OperatorFactory to match this creator
            with DeclarativePipelineTask instances.
    """

    ref_name: str = "declarative_pipeline"

    def __init__(self, task: DeclarativePipelineTask, dag: DAG) -> None:
        """Initialize the DeclarativePipelineCreator.

        Args:
            task: The DeclarativePipelineTask containing pipeline configuration.
            dag: The Airflow DAG this operator will belong to.
        """
        super().__init__(task, dag)

    def _create_operator(self, **kwargs: Any) -> BaseOperator:
        """Create a DatabricksRunNowOperator for the declarative pipeline.

        Creates an Airflow operator that triggers an existing Databricks Job
        by name. The job must have a pipeline_task that references the DLT
        pipeline.

        Args:
            **kwargs: Additional keyword arguments passed to the operator.

        Returns:
            A configured DatabricksRunNowOperator instance.

        Raises:
            ValueError: If job_name is empty or not provided.
        """
        # Import here to avoid import errors if databricks provider not installed
        from datetime import timedelta

        from airflow.providers.databricks.operators.databricks import (
            DatabricksRunNowOperator,
        )

        # Get task parameters - defaults are handled in DeclarativePipelineTask
        job_name: str = self._task.job_name
        if not job_name:
            raise ValueError(
                f"job_name is required for DeclarativePipelineTask '{self._task.name}'"
            )
        databricks_conn_id: str = self._task.databricks_conn_id
        wait_for_completion: bool = self._task.wait_for_completion
        poll_interval_seconds: int = self._task.poll_interval_seconds
        timeout_seconds: int = self._task.timeout_seconds

        # Build the on_failure_callback: always cancel the Databricks run,
        # and also invoke the DAG-level callback (e.g. Slack alerts) if one exists.
        on_failure: Callable[[dict[str, Any]], None] = _build_failure_callback(
            self._dag.default_args.get("on_failure_callback"),
        )

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
            on_failure_callback=on_failure,
            **kwargs,
        )

        return operator
