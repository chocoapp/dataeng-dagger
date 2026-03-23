from dagger.pipeline.tasks.batch_task import BatchTask
from dagger.utilities.config_validator import Attribute
from dagger import conf

class SodaTask(BatchTask):
    ref_name = "soda"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes(
            [
                Attribute(
                    attribute_name="executable_prefix",
                    required=False,
                    parent_fields=["task_parameters"],
                    comment="E.g.: python",
                ),
                Attribute(
                    attribute_name="executable",
                    required=False,
                    parent_fields=["task_parameters"],
                    comment="E.g.: my_code.py",
                ),
                Attribute(
                    attribute_name="table_name",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="Full table name in the format 'database.schema.table' By default it is"
                            " set to the name of the input <schema>.<table>",
                ),
                Attribute(
                    attribute_name="output_s3_path",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="S3 location to upload the scan results",

                ),
                Attribute(
                    attribute_name="output_table",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="Athena table that will contain the scan results.",
                ),
                Attribute(
                    attribute_name="is_critical_test",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="True if test run is critical test. Defaults to False",

                ),
                Attribute(
                    attribute_name="vars",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="Variables needed to run soda scan",
                )

            ]
        )

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)

        self._executable = self.executable or conf.SODA_DEFAULT_EXECUTABLE
        self._executable_prefix = self.executable_prefix or conf.SODA_DEFAULT_EXECUTABLE_PREFIX
        self._absolute_job_name = self._absolute_job_name or conf.SODA_DEFAULT_JOB_NAME
        self._output_table = self.parse_attribute("output_table") or conf.SODA_DEFAULT_OUTPUT_TABLE
        self._output_s3_path = self.parse_attribute("output_s3_path") or conf.SODA_DEFAULT_OUTPUT_S3_PATH
        self._table_name = self.parse_attribute("table_name")
        self._is_critical_test = self.parse_attribute("is_critical_test")
        self._vars = self.parse_attribute("vars")




    @property
    def output_table(self):
        return self._output_table

    @property
    def output_s3_path(self):
        return self._output_s3_path

    @property
    def table_name(self):
        return self._table_name
    @property
    def is_critical_test(self):
        return self._is_critical_test

    @property
    def vars(self):
        return self._vars


