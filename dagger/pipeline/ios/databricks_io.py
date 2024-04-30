from dagger.pipeline.io import IO
from dagger.utilities.config_validator import Attribute


class DatabricksIO(IO):
    ref_name = "databricks"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes(
            [
                Attribute(attribute_name="catalog"),
                Attribute(attribute_name="schema"),
                Attribute(attribute_name="table"),
            ]
        )

    def __init__(self, io_config, config_location):
        super().__init__(io_config, config_location)

        self._catalog = self.parse_attribute("catalog")
        self._schema = self.parse_attribute("schema")
        self._table = self.parse_attribute("table")

    def alias(self):
        return f"databricks://{self._catalog}/{self._schema}/{self._table}"

    @property
    def rendered_name(self):
        return f"{self._catalog}.{self._schema}.{self._table}"

    @property
    def airflow_name(self):
        return f"databricks-{self._catalog}-{self._schema}-{self._table}"

    @property
    def catalog(self):
        return self._catalog

    @property
    def schema(self):
        return self._schema

    @property
    def table(self):
        return self._table
