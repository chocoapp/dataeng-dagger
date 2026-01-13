"""Unit tests for DatabricksDLTCreator."""

import sys
import unittest
from datetime import timedelta
from unittest.mock import MagicMock, patch

from dagger.dag_creator.airflow.operator_creators.databricks_dlt_creator import (
    DatabricksDLTCreator,
    _cancel_databricks_run,
)


class TestDatabricksDLTCreator(unittest.TestCase):
    """Test cases for DatabricksDLTCreator."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_task = MagicMock()
        self.mock_task.name = "test_dlt_task"
        self.mock_task.job_name = "test-dlt-job"
        self.mock_task.databricks_conn_id = "databricks_default"
        self.mock_task.wait_for_completion = True
        self.mock_task.poll_interval_seconds = 30
        self.mock_task.timeout_seconds = 3600
        self.mock_task.cancel_on_kill = True

        self.mock_dag = MagicMock()

        # Set up mock for DatabricksRunNowOperator
        self.mock_operator = MagicMock()
        self.mock_operator_class = MagicMock(return_value=self.mock_operator)
        self.mock_databricks_module = MagicMock()
        self.mock_databricks_module.DatabricksRunNowOperator = self.mock_operator_class

    def test_ref_name(self) -> None:
        """Test that ref_name is correctly set."""
        self.assertEqual(DatabricksDLTCreator.ref_name, "databricks_dlt")

    @patch.dict(
        sys.modules,
        {"airflow.providers.databricks.operators.databricks": MagicMock()},
    )
    def test_create_operator(self) -> None:
        """Test operator creation returns an operator instance."""
        mock_operator = MagicMock()
        mock_operator_class = MagicMock(return_value=mock_operator)
        sys.modules[
            "airflow.providers.databricks.operators.databricks"
        ].DatabricksRunNowOperator = mock_operator_class

        creator = DatabricksDLTCreator(self.mock_task, self.mock_dag)
        operator = creator._create_operator()

        mock_operator_class.assert_called_once()
        self.assertEqual(operator, mock_operator)

    @patch.dict(
        sys.modules,
        {"airflow.providers.databricks.operators.databricks": MagicMock()},
    )
    def test_create_operator_maps_task_properties(self) -> None:
        """Test that task properties are correctly mapped to operator."""
        mock_operator_class = MagicMock()
        sys.modules[
            "airflow.providers.databricks.operators.databricks"
        ].DatabricksRunNowOperator = mock_operator_class

        creator = DatabricksDLTCreator(self.mock_task, self.mock_dag)
        creator._create_operator()

        call_kwargs = mock_operator_class.call_args[1]

        self.assertEqual(call_kwargs["dag"], self.mock_dag)
        self.assertEqual(call_kwargs["task_id"], "test_dlt_task")
        self.assertEqual(call_kwargs["databricks_conn_id"], "databricks_default")
        self.assertEqual(call_kwargs["job_name"], "test-dlt-job")
        self.assertEqual(call_kwargs["wait_for_termination"], True)
        self.assertEqual(call_kwargs["polling_period_seconds"], 30)
        self.assertEqual(call_kwargs["execution_timeout"], timedelta(seconds=3600))
        self.assertTrue(call_kwargs["do_xcom_push"])
        self.assertEqual(call_kwargs["on_failure_callback"], _cancel_databricks_run)

    @patch.dict(
        sys.modules,
        {"airflow.providers.databricks.operators.databricks": MagicMock()},
    )
    def test_create_operator_with_custom_values(self) -> None:
        """Test operator creation with non-default values."""
        self.mock_task.databricks_conn_id = "custom_conn"
        self.mock_task.wait_for_completion = False
        self.mock_task.poll_interval_seconds = 60
        self.mock_task.timeout_seconds = 7200

        mock_operator_class = MagicMock()
        sys.modules[
            "airflow.providers.databricks.operators.databricks"
        ].DatabricksRunNowOperator = mock_operator_class

        creator = DatabricksDLTCreator(self.mock_task, self.mock_dag)
        creator._create_operator()

        call_kwargs = mock_operator_class.call_args[1]

        self.assertEqual(call_kwargs["databricks_conn_id"], "custom_conn")
        self.assertEqual(call_kwargs["wait_for_termination"], False)
        self.assertEqual(call_kwargs["polling_period_seconds"], 60)
        self.assertEqual(call_kwargs["execution_timeout"], timedelta(seconds=7200))

    @patch.dict(
        sys.modules,
        {"airflow.providers.databricks.operators.databricks": MagicMock()},
    )
    def test_create_operator_empty_job_name_raises_error(self) -> None:
        """Test that empty job_name raises ValueError."""
        self.mock_task.job_name = ""

        creator = DatabricksDLTCreator(self.mock_task, self.mock_dag)

        with self.assertRaises(ValueError) as context:
            creator._create_operator()

        self.assertIn("job_name is required", str(context.exception))
        self.assertIn("test_dlt_task", str(context.exception))

    @patch.dict(
        sys.modules,
        {"airflow.providers.databricks.operators.databricks": MagicMock()},
    )
    def test_create_operator_none_job_name_raises_error(self) -> None:
        """Test that None job_name raises ValueError."""
        self.mock_task.job_name = None

        creator = DatabricksDLTCreator(self.mock_task, self.mock_dag)

        with self.assertRaises(ValueError) as context:
            creator._create_operator()

        self.assertIn("job_name is required", str(context.exception))

    @patch.dict(
        sys.modules,
        {"airflow.providers.databricks.operators.databricks": MagicMock()},
    )
    def test_create_operator_passes_kwargs(self) -> None:
        """Test that additional kwargs are passed to operator."""
        mock_operator_class = MagicMock()
        sys.modules[
            "airflow.providers.databricks.operators.databricks"
        ].DatabricksRunNowOperator = mock_operator_class

        creator = DatabricksDLTCreator(self.mock_task, self.mock_dag)
        creator._create_operator(retries=3, retry_delay=60)

        call_kwargs = mock_operator_class.call_args[1]

        self.assertEqual(call_kwargs["retries"], 3)
        self.assertEqual(call_kwargs["retry_delay"], 60)


class TestCancelDatabricksRun(unittest.TestCase):
    """Test cases for _cancel_databricks_run callback."""

    def test_cancel_run_no_task_instance(self) -> None:
        """Test callback handles missing task instance gracefully."""
        context: dict = {}

        # Should not raise, just log warning
        _cancel_databricks_run(context)

    def test_cancel_run_no_run_id(self) -> None:
        """Test callback handles missing run_id gracefully."""
        mock_ti = MagicMock()
        mock_ti.task_id = "test_task"
        mock_ti.xcom_pull.return_value = None

        context = {"task_instance": mock_ti}

        # Should not raise, just log warning
        _cancel_databricks_run(context)

        mock_ti.xcom_pull.assert_called_once_with(task_ids="test_task", key="run_id")

    @patch.dict(
        sys.modules,
        {"airflow.providers.databricks.hooks.databricks": MagicMock()},
    )
    def test_cancel_run_success(self) -> None:
        """Test successful cancellation of Databricks run."""
        mock_hook = MagicMock()
        mock_hook_class = MagicMock(return_value=mock_hook)
        sys.modules[
            "airflow.providers.databricks.hooks.databricks"
        ].DatabricksHook = mock_hook_class

        mock_ti = MagicMock()
        mock_ti.task_id = "test_task"
        mock_ti.xcom_pull.return_value = "run_12345"
        mock_ti.task.databricks_conn_id = "databricks_default"

        context = {"task_instance": mock_ti}

        _cancel_databricks_run(context)

        mock_hook_class.assert_called_once_with(databricks_conn_id="databricks_default")
        mock_hook.cancel_run.assert_called_once_with("run_12345")

    @patch.dict(
        sys.modules,
        {"airflow.providers.databricks.hooks.databricks": MagicMock()},
    )
    def test_cancel_run_handles_exception(self) -> None:
        """Test callback handles cancellation errors gracefully."""
        mock_hook = MagicMock()
        mock_hook.cancel_run.side_effect = Exception("API Error")
        mock_hook_class = MagicMock(return_value=mock_hook)
        sys.modules[
            "airflow.providers.databricks.hooks.databricks"
        ].DatabricksHook = mock_hook_class

        mock_ti = MagicMock()
        mock_ti.task_id = "test_task"
        mock_ti.xcom_pull.return_value = "run_12345"
        mock_ti.task.databricks_conn_id = "databricks_default"

        context = {"task_instance": mock_ti}

        # Should not raise, just log error
        _cancel_databricks_run(context)

        mock_hook.cancel_run.assert_called_once_with("run_12345")

    @patch.dict(
        sys.modules,
        {"airflow.providers.databricks.hooks.databricks": MagicMock()},
    )
    def test_cancel_run_with_custom_conn_id(self) -> None:
        """Test cancellation uses correct connection ID."""
        mock_hook = MagicMock()
        mock_hook_class = MagicMock(return_value=mock_hook)
        sys.modules[
            "airflow.providers.databricks.hooks.databricks"
        ].DatabricksHook = mock_hook_class

        mock_ti = MagicMock()
        mock_ti.task_id = "test_task"
        mock_ti.xcom_pull.return_value = "run_67890"
        mock_ti.task.databricks_conn_id = "custom_databricks_conn"

        context = {"task_instance": mock_ti}

        _cancel_databricks_run(context)

        mock_hook_class.assert_called_once_with(
            databricks_conn_id="custom_databricks_conn"
        )

    @patch.dict(
        sys.modules,
        {"airflow.providers.databricks.hooks.databricks": None},
    )
    def test_cancel_run_handles_import_error(self) -> None:
        """Test callback handles missing databricks provider gracefully."""
        mock_ti = MagicMock()
        mock_ti.task_id = "test_task"
        mock_ti.xcom_pull.return_value = "run_12345"
        mock_ti.task.databricks_conn_id = "databricks_default"

        context = {"task_instance": mock_ti}

        # Should not raise, just log error
        _cancel_databricks_run(context)


if __name__ == "__main__":
    unittest.main()
