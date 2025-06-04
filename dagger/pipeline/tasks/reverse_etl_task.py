from dagger.pipeline.tasks.batch_task import BatchTask
from dagger.utilities.config_validator import Attribute
from dagger import conf

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
                    comment="The number of threads to use for the job",
                ),
                Attribute(
                    attribute_name="batch_size",
                    parent_fields=["task_parameters"],
                    required=False,
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
                Attribute(
                    attribute_name="full_refresh",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="If set to True, the job will perform a full refresh instead of an incremental one",
                ),
                Attribute(
                    attribute_name="target_case",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="Target column case for DynamoDB. 'snake' leaves columns in snake_case; 'camel' converts to camelCase.",
                ),
                Attribute(
                    attribute_name="source_case",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="Source dataset column case. Specify the case of the incoming dataset."
                ),
                Attribute(
                    attribute_name="column_mapping",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment='Optional JSON string for column mappings. Example: \'{"id": "chat_id"}\'',
                ),
                Attribute(
                    attribute_name="glue_registry_name",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=True,
                    comment='AWS Glue Registry name',
                ),
                Attribute(
                    attribute_name="glue_schema_name",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment='AWS Glue Schema name. output_name will be used if not provided',
                ),
                Attribute(
                    attribute_name="sort_key",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment='Optional JSON string for sort key composition using #.join(). Example: \'{"sort_key": ["project", "model_name", "secondary_id", "custom_id"]}\'',
                ),
                Attribute(
                    attribute_name="custom_columns",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment='Optional JSON string for additional custom columns from static values. Example: \'{"custom_project": "ProjectXYZ", "model_name": "ModelABC"}\''
                ),
                Attribute(
                    attribute_name="input_table_columns_to_include",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment='Optional comma-separated list of columns to include in the job. Example: \'column1,column2,column3\', if not provided, all columns of input table will be included',
                ),
                Attribute(
                    attribute_name="input_table_columns_to_exclude",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment='Optional comma-separated list of columns to exclude from the job. Example: \'column1,column2,column3\', if not provided, all columns of input table will be included',
                ),
                Attribute(
                    attribute_name="file_format",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="File format for S3 output: 'json' or 'parquet' (required when output_type is 's3')",
                ),
                Attribute(
                    attribute_name="file_prefix",
                    parent_fields=["task_parameters"],
                    validator=str,
                    required=False,
                    comment="File prefix for S3 output files",
                )
            ]
        )

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)

        self._executable = self.executable or conf.REVERSE_ETL_DEFAULT_EXECUTABLE
        self._executable_prefix = self.executable_prefix or conf.REVERSE_ETL_DEFAULT_EXECUTABLE_PREFIX

        self._assume_role_arn = self.parse_attribute("assume_role_arn")
        self._num_threads = self.parse_attribute("num_threads")
        self._batch_size = self.parse_attribute("batch_size")
        self._absolute_job_name = self._absolute_job_name or conf.REVERSE_ETL_DEFAULT_JOB_NAME
        self._primary_id_column = self.parse_attribute("primary_id_column")
        self._secondary_id_column = self.parse_attribute("secondary_id_column")
        self._custom_id_column = self.parse_attribute("custom_id_column")
        self._is_deleted_column = self.parse_attribute("is_deleted_column")
        self._hash_column = self.parse_attribute("hash_column")
        self._updated_at_column = self.parse_attribute("updated_at_column")
        self._from_time = self.parse_attribute("from_time")
        self._days_to_live = self.parse_attribute("days_to_live")
        self._full_refresh = self.parse_attribute("full_refresh")
        self._target_case = self.parse_attribute("target_case")
        self._source_case = self.parse_attribute("source_case")
        self._column_mapping = self.parse_attribute("column_mapping")
        self._glue_registry_name = self.parse_attribute("glue_registry_name")
        self._glue_schema_name = self.parse_attribute("glue_schema_name")
        self._sort_key = self.parse_attribute("sort_key")
        self._custom_columns = self.parse_attribute("custom_columns")
        self._input_table_columns_to_include = self.parse_attribute("input_table_columns_to_include")
        self._input_table_columns_to_exclude = self.parse_attribute("input_table_columns_to_exclude")
        self._file_format = self.parse_attribute("file_format")
        self._file_prefix = self.parse_attribute("file_prefix")

        if self._hash_column and self._updated_at_column:
            raise ValueError(f"ReverseETLTask: {self._name} hash_column and updated_at_column are mutually exclusive")

        if self._input_table_columns_to_include and self._input_table_columns_to_exclude:
            raise ValueError(f"ReverseETLTask: {self._name} _input_table_columns_to_include and _input_table_columns_to_exclude are mutually exclusive")

        if self._hash_column or self._updated_at_column:
            if not self._from_time:
                raise ValueError(f"ReverseETLTask: {self._name} from_time is required when hash_column or updated_at_column is provided")

        # Making sure the input table name is set as it is expected in the reverse etl job
        input_index = self._get_io_index(self._inputs)
        print('XXX', self._inputs, input_index)
        if input_index is None:
            raise ValueError(f"ReverseEtlTask: {self._name} must have an input")
        self._inputs[input_index].name = "input_table_name"

        # Making sure the output name is set as it is expected in the reverse etl job
        output_index = self._get_io_index(self._outputs)
        if output_index is None:
            raise ValueError(f"ReverseEtlTask: {self._name} must have an output")
        self._outputs[output_index].name = "output_name"

        # Extracting the output type from the output definition
        self._output_type = self._outputs[output_index].ref_name

        # Extracting the outputs region name from the output definition
        self._region_name = self._outputs[output_index].region_name


    def _get_io_index(self, ios):
        if len([io for io in ios if io.ref_name != "dummy"]) > 1:
            raise ValueError(f"ReverseEtlTask: {self._name} can only have one input or output")

        for i, io in enumerate(ios):
            if io.ref_name != "dummy":
                return i
        return None


    @property
    def assume_role_arn(self):
        return self._assume_role_arn

    @property
    def num_threads(self):
        return self._num_threads

    @property
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

    @property
    def output_type(self):
        return self._output_type

    @property
    def region_name(self):
        return self._region_name

    @property
    def full_refresh(self):
        return self._full_refresh

    @property
    def target_case(self):
        return self._target_case

    @property
    def source_case(self):
        return self._source_case

    @property
    def column_mapping(self):
        return self._column_mapping

    @property
    def glue_registry_name(self):
        return self._glue_registry_name

    @property
    def glue_schema_name(self):
        return self._glue_schema_name

    @property
    def sort_key(self):
        return self._sort_key

    @property
    def custom_columns(self):
        return self._custom_columns

    @property
    def input_table_columns_to_include(self):
        return self._input_table_columns_to_include

    @property
    def input_table_columns_to_exclude(self):
        return self._input_table_columns_to_exclude

    @property
    def file_format(self):
        return self._file_format

    @property
    def file_prefix(self):
        return self._file_prefix
