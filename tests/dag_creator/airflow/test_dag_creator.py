import unittest
from datetime import datetime

from dagger import conf
from dagger.config_finder.config_finder import ConfigFinder
from dagger.config_finder.config_processor import ConfigProcessor
from dagger.dag_creator.airflow.dag_creator import DagCreator
from dagger.graph.task_graph import TaskGraph

from airflow.utils.dot_renderer import render_dag


class TestDagCreator(unittest.TestCase):
    @staticmethod
    def _fetch_task_graph():
        cf = ConfigFinder(conf.DAGS_DIR)
        cp = ConfigProcessor(cf)

        pipelines = cp.process_pipeline_configs()

        task_graph = TaskGraph()
        for pipeline in pipelines:
            task_graph.add_pipeline(pipeline)

        return task_graph
#
    @staticmethod
    def _read_file(filename):
        with open(filename, "r") as stream:
            return stream.read()

    def setUp(self) -> None:
        self.maxDiff = None

        self.task_graph = self._fetch_task_graph()

        self.dot_test_batch_graph_without_dataset =\
            self._read_file("tests/fixtures/dag_creator/airflow/dag_test_batch_without_dataset.dot")

        self.dot_test_batch_graph_with_dataset =\
            self._read_file("tests/fixtures/dag_creator/airflow/dag_test_batch_with_dataset.dot")

        self.dot_test_spark_deactivate = \
            self._read_file("tests/fixtures/dag_creator/airflow/dag_test_spark_deactivate.dot")

        self.dot_test_external_sensor =\
            self._read_file("tests/fixtures/dag_creator/airflow/dag_test_external_sensor.dot")

    def test_dag_creator_without_dataset(self):
        dag_creator = DagCreator(self.task_graph._graph)
        dags = dag_creator.traverse_graph()

        self.assertEqual(len(dags), 3)
        test_batch_dag = dags['test_batch']

        dot = render_dag(test_batch_dag)
        self.assertEqual(dot.source, self.dot_test_batch_graph_without_dataset)

    def test_dag_creator_with_dataset(self):
        dag_creator = DagCreator(self.task_graph._graph, with_data_nodes=True)
        dags = dag_creator.traverse_graph()

        self.assertEqual(len(dags), 3)

        test_batch_dag = dags['test_batch']
        dot = render_dag(test_batch_dag)
        self.assertEqual(dot.source, self.dot_test_batch_graph_with_dataset)

    def test_dag_creator_external_sensor(self):
        dag_creator = DagCreator(self.task_graph._graph)
        dags = dag_creator.traverse_graph()

        self.assertEqual(len(dags), 3)
        test_external_sensor_dag = dags['test_external_sensor']

        dot = render_dag(test_external_sensor_dag)
        self.assertEqual(dot.source, self.dot_test_external_sensor)

    def test_get_execution_delta_fn(self):
        execution_date = datetime(2021, 12, 28, 18, 30)
        test_cases = [
            # (from_dag_schedule, to_dag_schedule, expected_result)
            # Airflow 3 CronTriggerTimetable: the target is the most recent upstream
            # tick at or before the downstream run's logical_date (= the upstream run's
            # own logical_date).
            ("0 * * * *", "30 * * * *", datetime(2021, 12, 28, 18, 0)),  # both hourly with different minutes
            ("30 * * * *", "30 * * * *", datetime(2021, 12, 28, 18, 30)),  # both hourly with same minutes
            ("0 * * * *", "30 18 * * *", datetime(2021, 12, 28, 18, 0)),  # from hourly to daily with different minutes
            ("30 * * * *", "30 18 * * *", datetime(2021, 12, 28, 18, 30)),  # from hourly to daily with same minutes
            ("0 19 * * *", "30 * * * *", datetime(2021, 12, 27, 19, 0)),  # from daily to hourly with different minutes
            ("0 20 * * *", "30 * * * *", datetime(2021, 12, 27, 20, 0)),  # from daily to hourly with different minutes
            ("30 19 * * *", "30 * * * *", datetime(2021, 12, 27, 19, 30)),  # from daily to hourly with same minutes
        ]

        for test_case in test_cases:
            from_dag_schedule, to_dag_schedule, expected_result = test_case

            execution_delta_fn = DagCreator._get_execution_date_fn(from_dag_schedule, to_dag_schedule)
            self.assertEqual(execution_delta_fn(execution_date), expected_result)

    def test_get_execution_delta_fn_daily_waiting_on_hourly_airflow3(self):
        # Regression for the Airflow 3 logical_date bug: a daily DAG (00:45) waiting on
        # an hourly upstream must target the hourly run whose logical_date is the most
        # recent tick at or before the daily run's logical_date. Pre-fix this resolved to
        # a future/nonexistent hourly tick, so the deferred sensor polled forever.
        execution_delta_fn = DagCreator._get_execution_date_fn("0 * * * *", "45 0 * * *")
        self.assertEqual(
            execution_delta_fn(datetime(2026, 5, 27, 0, 45)),
            datetime(2026, 5, 27, 0, 0),
        )

    def test_disable_task(self):
        dag_creator = DagCreator(self.task_graph._graph, with_data_nodes=True)
        dags = dag_creator.traverse_graph()

        self.assertEqual(len(dags), 3)
        test_spark_dag = dags['test_spark']

        dot = render_dag(test_spark_dag)
        self.assertEqual(dot.source, self.dot_test_spark_deactivate)

    def test_override_params(self):
        dag_creator = DagCreator(self.task_graph._graph, with_data_nodes=True)
        dags = dag_creator.traverse_graph()

        test_spark_dag = dags['test_spark']

        self.assertEqual(test_spark_dag.schedule_interval, "30 1 * * *")
