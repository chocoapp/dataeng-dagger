from dagger.pipeline.tasks.batch_task import BatchTask
from dagger.utilities.config_validator import Attribute

class ReverseEtlTask(BatchTask):
    ref_name = "reverse_etl"

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
                    attribute_name="assume_role_arn",
                    parent_fields=["task_parameters"],
                    required = False,
                    validator=str,
                    comment="The ARN of the role to assume before running the job",
                ),
                Attribute(
                    attribute_name="num_threads",
                    parent_fields=["task_parameters"],
                    required=False,
                    validator=int,
                    comment="The number of threads to use for the job",
                ),
                Attribute(
                    attribute_name="batch_size",
                    parent_fields=["task_parameters"],
                    required=False,
                    validator=int,
                    comment="The number of rows to fetch in each batch",
                ),
                Attribute(
                    attribute_name="primary_id_column",
                    parent_fields=["task_parameters"],
                    validator=str,
                    comment="The primary key column to use for the job",
                ),
                Attribute(
                    attribute_name="secondary_id_column",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="The secondary key column to use for the job",
                ),
                Attribute(
                    attribute_name="custom_id_column",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="The custom key column to use for the job",
                ),
                Attribute(
                    attribute_name="model_name",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="The name of the model. This is going to be a column on the target table. By default it is"
                            " set to the name of the input <schema>.<table>",
                ),
                Attribute(
                    attribute_name="project_name",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="The name of the project. This is going to be a column on the target table. By default it is"
                            " set to feature_store",
                ),
                Attribute(
                    attribute_name="is_deleted_column",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="The column that has the boolean flag to indicate if the row is deleted",
                ),
                Attribute(
                    attribute_name="hash_column",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="The column that has the the hash value of the row to be used to get the diff since "
                            "the last export. If provided, the from_time is required. It's mutually exclusive with "
                            "updated_at_column",
                ),
                Attribute(
                    attribute_name="updated_at_column",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="The column that has the last updated timestamp of the row to be used to get the diff "
                            "since the last export. If provided, the from_time is required. It's mutually exclusive "
                            "with hash_column",
                ),
                Attribute(
                    attribute_name="from_time",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="Timestamp in YYYY-mm-ddTHH:MM format. It is used for incremental loads."
                            "It's required when hash_column or updated_at_column is provided",
                ),
                Attribute(
                    attribute_name="days_to_live",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="The number of days to keep the data in the table. If provided, the time_to_live attribute "
                            "will be set in dynamodb",
                ),

            ]
        )

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)

        self.executable = self.executable or "reverse_etl.py"
        self.executable_prefix = self.executable_prefix or "python"

        self._assume_role_arn = self.parse_attribute("assume_role_arn")
        self._num_threads = self.parse_attribute("num_threads") or 4
        self._batch_size = self.parse_attribute("batch_size") or 10000
        self._absolute_job_name = self._absolute_job_name or "common_batch_jobs/reverse_etl"
        self._primary_id_column = self.parse_attribute("primary_id_column")
        self._secondary_id_column = self.parse_attribute("secondary_id_column")
        self._custom_id_column = self.parse_attribute("custom_id_column")
        self._model_name = self.parse_attribute("model_name")
        self._project_name = self.parse_attribute("project_name") or "feature_store"
        self._is_deleted_column = self.parse_attribute("is_deleted_column")
        self._hash_column = self.parse_attribute("hash_column")
        self._updated_at_column = self.parse_attribute("updated_at_column")
        self._from_time = self.parse_attribute("from_time")
        self._days_to_live = self.parse_attribute("days_to_live")

        if self._hash_column and self._updated_at_column:
            raise ValueError("hash_column and updated_at_column are mutually exclusive")

        if self._hash_column or self._updated_at_column:
            if not self._from_time:
                raise ValueError("from_time is required when hash_column or updated_at_column is provided")

    @property
    def assume_role_arn(self):
        return self._assume_role_arn

    @property
    def num_threads(self):
        return self._num_threads

    @@property
    def batch_size(self):
        return self._batch_size

    @property
    def primary_id_column(self):
        return self._primary_id_column

    @property
    def secondary_id_column(self):
        return self._secondary_id_column

    @property
    def custom_id_column(self):
        return self._custom_id_column

    @property
    def model_name(self):
        return self._model_name

    @property
    def project_name(self):
        return self._project_name

    @property
    def is_deleted_column(self):
        return self._is_deleted_column

    @property
    def hash_column(self):
        return self._hash_column

    @property
    def updated_at_column(self):
        return self._updated_at_column

    @property
    def from_time(self):
        return self._from_time

    @property
    def days_to_live(self):
        return self._days_to_live
