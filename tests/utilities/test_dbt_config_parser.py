import logging
import unittest
from unittest import skip
from unittest.mock import patch, MagicMock

from dagger.utilities.dbt_config_parser import DBTConfigParser
from dagger.utilities.module import Module
from tests.fixtures.modules.dbt_config_parser_fixtures import (
    EXPECTED_DAGGER_OUTPUTS,
    EXPECTED_DAGGER_INPUTS,
    DBT_MANIFEST_FILE_FIXTURE,
    DBT_PROFILE_FIXTURE,
    EXPECTED_STAGING_NODE,
    EXPECTED_STAGING_NODE_MULTIPLE_DEPENDENCIES,
    EXPECTED_SEED_NODE,
    EXPECTED_MODEL_MULTIPLE_DEPENDENCIES,
    EXPECTED_EPHEMERAL_NODE,
    EXPECTED_DBT_STAGING_MODEL_DAGGER_OUTPUTS,
    EXPECTED_DBT_STAGING_MODEL_DAGGER_INPUTS,
)

_logger = logging.getLogger("root")

DEFAULT_CONFIG_PARAMS = {
    "data_bucket": "bucket1-data-lake",
    "project_dir": "main",
    "profile_dir": ".dbt",
    "dbt_profile": "data",
}
MODEL_NAME = "model1"


class TestDBTConfigParser(unittest.TestCase):
    @patch("builtins.open", new_callable=MagicMock, read_data=DBT_MANIFEST_FILE_FIXTURE)
    @patch("json.loads", return_value=DBT_MANIFEST_FILE_FIXTURE)
    @patch("yaml.safe_load", return_value=DBT_PROFILE_FIXTURE)
    def setUp(self, mock_open, mock_json_load, mock_safe_load):
        self._dbt_config_parser = DBTConfigParser(DEFAULT_CONFIG_PARAMS)
        self._sample_dbt_node = DBT_MANIFEST_FILE_FIXTURE["nodes"]["model.main.model1"]

    @skip("Run only locally")
    def test_generate_task_configs(self):
        module = Module(
            path_to_config="./tests/fixtures/modules/dbt_test_config.yaml",
            target_dir="./tests/fixtures/modules/",
        )

        module.generate_task_configs()

    def test_generate_dagger_inputs(self):
        test_inputs = [
            (
                "model.main.stg_core_schema1__table1",
                EXPECTED_STAGING_NODE,
            ),
            (
                "model.main.stg_core_schema2__table2",
                EXPECTED_STAGING_NODE_MULTIPLE_DEPENDENCIES,
            ),
            (
                "seed.main.seed_buyer_country_overwrite",
                EXPECTED_SEED_NODE,
            ),
            (
                "model.main.int_model3",
                EXPECTED_EPHEMERAL_NODE,
            ),
        ]
        for mock_input, expected_output in test_inputs:
            result = self._dbt_config_parser._generate_dagger_tasks(mock_input)
            self.assertListEqual(result, expected_output)

    def test_generate_io_inputs(self):
        fixtures = [
            ("model1", EXPECTED_DAGGER_INPUTS),
            (
                "model3",
                EXPECTED_MODEL_MULTIPLE_DEPENDENCIES,
            ),
            ("stg_core_schema2__table2", EXPECTED_DBT_STAGING_MODEL_DAGGER_INPUTS),
        ]
        for mock_input, expected_output in fixtures:
            result, _ = self._dbt_config_parser.generate_dagger_io(mock_input)

            self.assertListEqual(result, expected_output)

    def test_generate_io_outputs(self):
        fixtures = [
            ("model1", EXPECTED_DAGGER_OUTPUTS),
            ("stg_core_schema2__table2", EXPECTED_DBT_STAGING_MODEL_DAGGER_OUTPUTS),
        ]
        for mock_input, expected_output in fixtures:
            _, result = self._dbt_config_parser.generate_dagger_io(mock_input)

            self.assertListEqual(result, expected_output)
