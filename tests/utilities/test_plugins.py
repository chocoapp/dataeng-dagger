import inspect
import shutil
import unittest
from pathlib import Path
from unittest.mock import patch
import os
import importlib.util

import jinja2

from dagger.utilities.module import Module  # Adjust this import according to your actual module structure
from dagger import conf

TESTS_ROOT = Path(__file__).parent.parent

class TestLoadPlugins(unittest.TestCase):

    def setUp(self):
        self._jinja_environment = jinja2.Environment()
        loaded_classes = Module.load_plugins()
        for class_name, class_obj in loaded_classes.items():
            self._jinja_environment.globals[class_name] = class_obj

        self._template = self._jinja_environment.from_string("inputs: {{ SampleFolderPlugin.get_inputs() }}")

    @patch("dagger.conf.PLUGIN_DIRS", new=[])
    @patch("os.walk")
    def test_load_plugins_no_plugin_dir(self, mock_os_walk):
        # Simulate os.walk returning no Python files
        mock_os_walk.return_value = [("/fake/plugin/dir", [], [])]

        result = Module.load_plugins()

        # Expecting an empty dictionary since no plugins were found
        self.assertEqual(result, {})

    @patch("dagger.conf.PLUGIN_DIRS", new=[str(TESTS_ROOT.joinpath("fixtures/plugins"))])
    def test_load_plugins(self):

        result = Module.load_plugins()
        for name, plugin_class in result.items():
            result[name] = str(plugin_class)

        expected_classes = {"SampleFolderPlugin": "<class 'sample_folder_plugin.SampleFolderPlugin'>"}

        self.assertEqual(result, expected_classes)

    @patch("dagger.conf.PLUGIN_DIRS", new=[str(TESTS_ROOT.joinpath("fixtures/plugins"))])
    def test_load_plugins_in_jinja(self):
        result = Module.load_plugins()
        for class_name, class_obj in result.items():
            self._jinja_environment.globals[class_name] = class_obj

        rendered_task = self._template.render()
        expected_task = "inputs: [{'name': 'sample_folder_plugin_task', 'type': 'dummy'}]"

        self.assertEqual(rendered_task, expected_task)

if __name__ == '__main__':
    unittest.main()