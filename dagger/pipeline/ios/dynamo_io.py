from dagger.pipeline.io import IO
from dagger.utilities.config_validator import Attribute


class DynamoIO(IO):
    ref_name = "dynamo"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes(
            [
                Attribute(
                    attribute_name="region_name",
                    required=False,
                    comment="Only needed for cross region dynamo tables"
                ),
                Attribute(
                    attribute_name="table",
                    comment="The name of the dynamo table"
                ),
            ]
        )

    def __init__(self, io_config, config_location):
        super().__init__(io_config, config_location)

        self._region_name = self.parse_attribute("region_name")
        self._table = self.parse_attribute("table")

    def alias(self):
        return f"dynamo://{self._region_name or ''}/{self._table}"

    @property
    def rendered_name(self):
        return self._table

    @property
    def airflow_name(self):
        return f"dynamo-{'-'.join([name_part for name_part in [self._region_name, self._table] if name_part])}"

    @property
    def region_name(self):
        return self._region_name

    @property
    def table(self):
        return self._table
