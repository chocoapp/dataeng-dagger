import inspect
import shutil
import unittest
from pathlib import Path
from unittest.mock import patch
import os
import importlib.util

import jinja2

from dagger.utilities.module import Module
from dagger import conf

TESTS_ROOT = Path(__file__).parent.parent

class TestLoadPlugins(unittest.TestCase):

    def setUp(self):
        self._jinja_environment = jinja2.Environment()
        self._template = self._jinja_environment.from_string("inputs: {{ SampleFolderPlugin.get_inputs() }}")

    @patch("dagger.conf.PLUGIN_DIRS", new=[])
    @patch("os.walk")
    def test_load_plugins_no_plugin_dir(self, mock_os_walk):
        # Simulate os.walk returning no Python files
        mock_os_walk.return_value = [("/fake/plugin/dir", [], [])]

        result_environment = Module.load_plugins_to_jinja_environment(self._jinja_environment)

        self.assertNotIn("SampleFolderPlugin", result_environment.globals)

    @patch("dagger.conf.PLUGIN_DIRS", new=[str(TESTS_ROOT.joinpath("fixtures/plugins"))])
    def test_load_plugins(self):
        result_environment = Module.load_plugins_to_jinja_environment(self._jinja_environment)

        self.assertIn("SampleFolderPlugin", result_environment.globals.keys())

    @patch("dagger.conf.PLUGIN_DIRS", new=[str(TESTS_ROOT.joinpath("fixtures/plugins"))])
    def test_load_plugins_render_jinja(self):
        result_environment = Module.load_plugins_to_jinja_environment(self._jinja_environment)

        rendered_task = self._template.render()
        expected_task = "inputs: [{'name': 'sample_folder_plugin_task', 'type': 'dummy'}]"

        self.assertEqual(rendered_task, expected_task)

if __name__ == '__main__':
    unittest.main()