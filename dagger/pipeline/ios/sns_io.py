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
                    attribute_name="region",
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

        self._account_id = self.parse_attribute("account_id")
        self._region = self.parse_attribute("region")
        self._sns_topic = self.parse_attribute("sns_topic")

    def alias(self):
        return f"dynamo://{self._account_id or ''}/{self._region or ''}/{self._sns_topic}"

    @property
    def rendered_name(self):
        if not self._account_id and not self._region:
            return self._sns_topic
        else:
            return ":".join([self._account_id or '', self._region or '', self._sns_topic])

    @property
    def airflow_name(self):
        return f"dynamo-{'-'.join([name_part for name_part in [self._account_id, self._region, self._sns_topic] if name_part])}"

    @property
    def account_id(self):
        return self._account_id

    @property
    def region(self):
        return self._region

    @property
    def sns_topic(self):
        return self._sns_topic