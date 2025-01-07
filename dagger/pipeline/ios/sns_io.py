from dagger.pipeline.io import IO
from dagger.utilities.config_validator import Attribute


class SnsIO(IO):
    ref_name = "sns"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes(
            [
                Attribute(
                    attribute_name="account_id",
                    required=False,
                    comment="Only needed for cross account dynamo tables"
                ),
                Attribute(
                    attribute_name="region_name",
                    required=False,
                    comment="Only needed for cross region dynamo tables"
                ),
                Attribute(
                    attribute_name="sns_topic",
                    comment="The name of the sns topic"
                ),
            ]
        )

    def __init__(self, io_config, config_location):
        super().__init__(io_config, config_location)

        self._region_name = self.parse_attribute("region_name")
        self._sns_topic = self.parse_attribute("sns_topic")

    def alias(self):
        return f"sns://{self._region_name or ''}/{self._sns_topic}"

    @property
    def rendered_name(self):
        return self._sns_topic

    @property
    def airflow_name(self):
        return f"sns-{'-'.join([name_part for name_part in [self._region_name, self._sns_topic] if name_part])}"

    @property
    def region_name(self):
        return self._region_name

    @property
    def sns_topic(self):
        return self._sns_topic