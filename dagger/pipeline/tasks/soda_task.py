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
                    attribute_name="project_dir",
                    parent_fields=["task_parameters"],
                    required = False,
                    validator=str,
                    comment="Directory containing the dbt_project.yml file",
                ),
                Attribute(
                    attribute_name="profiles_dir",
                    parent_fields=["task_parameters"],
                    required=False,
                    comment="Directory containing the profiles.yml file",
                ),
                Attribute(
                    attribute_name="profile_name",
                    parent_fields=["task_parameters"],
                    required=False,
                    comment="Profile name to load from the profiles.yml file.",
                ),
                Attribute(
                    attribute_name="target_name",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=True,
                    comment="Target to load for the given profile. By default use 'ENV' environment variable.",
                ),
                Attribute(
                    attribute_name="table_name",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="Full table name in the format 'database.schema.table'",
                ),
                Attribute(
                    attribute_name="model_name",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="Name of dbt model to be scanned by soda",
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
        self._project_dir = self.parse_attribute("project_dir") or conf.SODA_DEFAULT_PROJECT_DIR
        self._profiles_dir = self.parse_attribute("profiles_dir") or conf.SODA_DEFAULT_PROFILES_DIR
        self._profile_name = self.parse_attribute("profile_name") or conf.SODA_DEFAULT_PROFILE_NAME
        self._output_table = self.parse_attribute("output_table") or conf.SODA_DEFAULT_OUTPUT_TABLE
        self._output_s3_path = self.parse_attribute("output_s3_path") or conf.SODA_DEFAULT_OUTPUT_S3_PATH
        self._target_name = self.parse_attribute("target_name")

        self._table_name = self.parse_attribute("table_name")
        self._model_name = self.parse_attribute("model_name")
        self._vars = self.parse_attribute("vars")


        if self._table_name and self._model_name:
            raise ValueError(f"SodaTask: {self._name} table_name and model_name are mutually exclusive")



    @property
    def project_dir(self):
        return self._project_dir

    @property
    def profiles_dir(self):
        return self._profiles_dir

    @property
    def profile_name(self):
        return self._profile_name

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
    def model_name(self):
        return self._model_name

    @property
    def vars(self):
        return self._vars

    @property
    def target_name(self):
        return self._target_name

