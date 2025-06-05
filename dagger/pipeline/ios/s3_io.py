from os.path import join, normpath

from dagger.pipeline.io import IO
from dagger.utilities.config_validator import Attribute


class S3IO(IO):
    ref_name = "s3"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes(
            [
                Attribute(
                    attribute_name="region_name",
                    required=False,
                    comment="Only needed for cross region S3 buckets"
                ),
                Attribute(
                    attribute_name="s3_protocol",
                    required=False,
                    comment="S3 protocol: s3a/s3/s3n",
                ),
                Attribute(attribute_name="bucket"),
                Attribute(attribute_name="path"),
            ]
        )

    def __init__(self, io_config, config_location):
        super().__init__(io_config, config_location)

        self._region_name = self.parse_attribute("region_name")
        self._s3_protocol = self.parse_attribute("s3_protocol") or "s3"
        self._bucket = normpath(self.parse_attribute("bucket"))
        self._path = normpath(self.parse_attribute("path"))

    def alias(self):
        return f"s3://{self._region_name or ''}/{join(self._bucket, self._path)}"

    @property
    def rendered_name(self):
        return f"{self._s3_protocol}://{join(self._bucket, self._path)}"

    @property
    def airflow_name(self):
        return f"s3-{'-'.join([name_part for name_part in [self._region_name, join(self._bucket, self._path).replace('/', '-')] if name_part])}"

    @property
    def bucket(self):
        return self._bucket

    @property
    def path(self):
        return self._path

    @property
    def region_name(self):
        return self._region_name
