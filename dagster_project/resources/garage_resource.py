import json
from contextlib import contextmanager
from typing import Iterator

import boto3
from dagster import ConfigurableResource, EnvVar


class GarageResource(ConfigurableResource):
    """S3-compatible connection to Garage, reads credentials from config."""

    endpoint_url: str = EnvVar("GARAGE_ENDPOINT_URL")
    access_key_id: str = EnvVar("GARAGE_ACCESS_KEY_ID")
    secret_access_key: str = EnvVar("GARAGE_SECRET_ACCESS_KEY")
    region: str = EnvVar("GARAGE_REGION")
    bucket_raw: str = EnvVar("GARAGE_BUCKET_RAW")

    @contextmanager
    def _client(self) -> Iterator["boto3.client"]:
        client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name=self.region,
        )
        try:
            yield client
        finally:
            client.close()

    def put_json(self, bucket: str, key: str, payload: object) -> None:
        body = json.dumps(payload).encode("utf-8")
        with self._client() as client:
            client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")

    def get_json(self, bucket: str, key: str) -> object:
        with self._client() as client:
            response = client.get_object(Bucket=bucket, Key=key)
            return json.loads(response["Body"].read())
