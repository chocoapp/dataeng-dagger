import unittest
from dagger.pipeline.ios.dynamo_io import DynamoIO
import yaml


class DynamoIOTest(unittest.TestCase):
    def setUp(self) -> None:
        with open("tests/fixtures/pipeline/ios/dynamo_io.yaml", "r") as stream:
            config = yaml.safe_load(stream)

        self.dynamo_io = DynamoIO(config, "/")

    def test_properties(self):
        self.assertEqual(self.dynamo_io.alias(), "dynamo://eu_west_1/schema.table_name")
        self.assertEqual(self.dynamo_io.rendered_name, "schema.table_name")
        self.assertEqual(self.dynamo_io.airflow_name,"dynamo-eu_west_1-schema.table_name")

if __name__ == "__main__":
    unittest.main()
