import unittest
from dagger.pipeline.io_factory import databricks_io

import yaml


class DbIOTest(unittest.TestCase):
    def setUp(self) -> None:
        with open('tests/fixtures/pipeline/ios/databricks_io.yaml', "r") as stream:
            config = yaml.safe_load(stream)

        self.db_io = databricks_io.DatabricksIO(config, "/")

    def test_properties(self):
        self.assertEqual(self.db_io.alias(), "databricks://test_catalog/test_schema/test_table")
        self.assertEqual(self.db_io.rendered_name, "test_catalog.test_schema.test_table")
        self.assertEqual(self.db_io.airflow_name, "databricks-test_catalog-test_schema-test_table")
