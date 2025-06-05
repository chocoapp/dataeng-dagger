import unittest
from dagger.pipeline.io_factory import s3_io

import yaml


class DbIOTest(unittest.TestCase):
    def setUp(self) -> None:
        with open('tests/fixtures/pipeline/ios/s3_io.yaml', "r") as stream:
            self.config = yaml.safe_load(stream)

    def test_properties(self):
        db_io = s3_io.S3IO(self.config, "/")

        self.assertEqual(db_io.alias(), "s3://eu_west_1/test_bucket/test_path")
        self.assertEqual(db_io.rendered_name, "s3://test_bucket/test_path")
        self.assertEqual(db_io.airflow_name, "s3-eu_west_1-test_bucket-test_path")

    def test_with_protocol(self):
        self.config['s3_protocol'] = 's3a'
        db_io = s3_io.S3IO(self.config, "/")

        self.assertEqual(db_io.alias(), "s3://eu_west_1/test_bucket/test_path")
        self.assertEqual(db_io.rendered_name, "s3a://test_bucket/test_path")
        self.assertEqual(db_io.airflow_name, "s3-eu_west_1-test_bucket-test_path")

    def test_with_region_name(self):
        self.config['region_name'] = 'us-west-2'
        db_io = s3_io.S3IO(self.config, "/")

        self.assertEqual(db_io.alias(), "s3://us-west-2/test_bucket/test_path")
        self.assertEqual(db_io.rendered_name, "s3://test_bucket/test_path")
        self.assertEqual(db_io.airflow_name, "s3-us-west-2-test_bucket-test_path")
        self.assertEqual(db_io.region_name, "us-west-2")
