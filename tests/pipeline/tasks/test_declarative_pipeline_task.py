"""Unit tests for DeclarativePipelineTask."""

import unittest
from unittest.mock import MagicMock

import yaml

from dagger.pipeline.tasks.declarative_pipeline_task import DeclarativePipelineTask


class TestDeclarativePipelineTask(unittest.TestCase):
    """Test cases for DeclarativePipelineTask."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        with open(
            "tests/fixtures/pipeline/tasks/declarative_pipeline_task.yaml", "r"
        ) as stream:
            self.config = yaml.safe_load(stream)

        # Create a mock pipeline object
        self.mock_pipeline = MagicMock()
        self.mock_pipeline.directory = "tests/fixtures/pipeline/tasks"

        self.task = DeclarativePipelineTask(
            name="test_dlt_task",
            pipeline_name="test_pipeline",
            pipeline=self.mock_pipeline,
            job_config=self.config,
        )

    def test_ref_name(self) -> None:
        """Test that ref_name is correctly set."""
        self.assertEqual(DeclarativePipelineTask.ref_name, "declarative_pipeline")

    def test_job_name(self) -> None:
        """Test job_name property."""
        self.assertEqual(self.task.job_name, "test-dlt-job")

    def test_databricks_conn_id(self) -> None:
        """Test databricks_conn_id property."""
        self.assertEqual(self.task.databricks_conn_id, "databricks_test")

    def test_wait_for_completion(self) -> None:
        """Test wait_for_completion property."""
        self.assertTrue(self.task.wait_for_completion)

    def test_poll_interval_seconds(self) -> None:
        """Test poll_interval_seconds property."""
        self.assertEqual(self.task.poll_interval_seconds, 60)

    def test_timeout_seconds(self) -> None:
        """Test timeout_seconds property."""
        self.assertEqual(self.task.timeout_seconds, 7200)

    def test_cancel_on_kill(self) -> None:
        """Test cancel_on_kill property."""
        self.assertTrue(self.task.cancel_on_kill)

    def test_deferrable(self) -> None:
        """Test deferrable property."""
        self.assertTrue(self.task.deferrable)

    def test_task_name(self) -> None:
        """Test that task name is correctly set."""
        self.assertEqual(self.task.name, "test_dlt_task")

    def test_pipeline_name(self) -> None:
        """Test that pipeline_name is correctly set."""
        self.assertEqual(self.task.pipeline_name, "test_pipeline")


class TestDeclarativePipelineTaskDefaults(unittest.TestCase):
    """Test cases for DeclarativePipelineTask default values."""

    def setUp(self) -> None:
        """Set up test fixtures with minimal config."""
        self.config = {
            "type": "declarative_pipeline",
            "description": "Test DLT task with defaults",
            "inputs": [],
            "outputs": [],
            "airflow_task_parameters": None,
            "template_parameters": None,
            "task_parameters": {
                "job_name": "minimal-dlt-job",
            },
        }

        self.mock_pipeline = MagicMock()
        self.mock_pipeline.directory = "tests/fixtures/pipeline/tasks"

        self.task = DeclarativePipelineTask(
            name="minimal_dlt_task",
            pipeline_name="test_pipeline",
            pipeline=self.mock_pipeline,
            job_config=self.config,
        )

    def test_default_databricks_conn_id(self) -> None:
        """Test default databricks_conn_id value."""
        self.assertEqual(self.task.databricks_conn_id, "databricks_default")

    def test_default_wait_for_completion(self) -> None:
        """Test default wait_for_completion value."""
        self.assertTrue(self.task.wait_for_completion)

    def test_default_poll_interval_seconds(self) -> None:
        """Test default poll_interval_seconds value."""
        self.assertEqual(self.task.poll_interval_seconds, 30)

    def test_default_timeout_seconds(self) -> None:
        """Test default timeout_seconds value."""
        self.assertEqual(self.task.timeout_seconds, 3600)

    def test_default_cancel_on_kill(self) -> None:
        """Test default cancel_on_kill value."""
        self.assertTrue(self.task.cancel_on_kill)

    def test_default_deferrable(self) -> None:
        """Test default deferrable value."""
        self.assertFalse(self.task.deferrable)


class TestDeclarativePipelineTaskBooleanHandling(unittest.TestCase):
    """Test cases for boolean parameter handling edge cases."""

    def test_wait_for_completion_false(self) -> None:
        """Test that wait_for_completion=false is correctly handled."""
        config = {
            "type": "declarative_pipeline",
            "description": "Test",
            "inputs": [],
            "outputs": [],
            "airflow_task_parameters": None,
            "template_parameters": None,
            "task_parameters": {
                "job_name": "test-job",
                "wait_for_completion": False,
            },
        }

        mock_pipeline = MagicMock()
        mock_pipeline.directory = "tests/fixtures/pipeline/tasks"

        task = DeclarativePipelineTask(
            name="test_task",
            pipeline_name="test_pipeline",
            pipeline=mock_pipeline,
            job_config=config,
        )

        self.assertFalse(task.wait_for_completion)

    def test_cancel_on_kill_false(self) -> None:
        """Test that cancel_on_kill=false is correctly handled."""
        config = {
            "type": "declarative_pipeline",
            "description": "Test",
            "inputs": [],
            "outputs": [],
            "airflow_task_parameters": None,
            "template_parameters": None,
            "task_parameters": {
                "job_name": "test-job",
                "cancel_on_kill": False,
            },
        }

        mock_pipeline = MagicMock()
        mock_pipeline.directory = "tests/fixtures/pipeline/tasks"

        task = DeclarativePipelineTask(
            name="test_task",
            pipeline_name="test_pipeline",
            pipeline=mock_pipeline,
            job_config=config,
        )

        self.assertFalse(task.cancel_on_kill)


if __name__ == "__main__":
    unittest.main()
