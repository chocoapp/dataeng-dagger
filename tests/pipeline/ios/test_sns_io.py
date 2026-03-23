import unittest
from dagger.pipeline.ios.sns_io import SnsIO
import yaml


class SnsIOTest(unittest.TestCase):
    def setUp(self) -> None:
        with open("tests/fixtures/pipeline/ios/sns_io.yaml", "r") as stream:
            config = yaml.safe_load(stream)

        self.sns_io = SnsIO(config, "/")

    def test_properties(self):
        self.assertEqual(self.sns_io.alias(), f"sns://eu_west_1/topic_name")
        self.assertEqual(self.sns_io.rendered_name, "topic_name")
        self.assertEqual(self.sns_io.airflow_name, "sns-eu_west_1-topic_name")

if __name__ == "__main__":
    unittest.main()
