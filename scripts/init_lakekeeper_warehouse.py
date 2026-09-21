#!/usr/bin/env python
"""One-time cluster bootstrap after `docker compose up -d`:

1. Assigns a layout to the (fresh, single-node) Garage cluster - without
   this, Garage refuses all reads/writes.
2. Imports the GARAGE_ACCESS_KEY_ID/SECRET from .env as a real Garage access
   key (a fresh Garage cluster starts with zero keys - they are not created
   just by putting values in .env).
3. Creates the raw/staging/datamart buckets, grants that key read/write on
   each, and configures CORS so the Lakekeeper web UI's browser-side table
   preview can read them directly.
4. Creates the initial Iceberg warehouse in Lakekeeper, backed by the
   staging bucket.

Uses the Garage Admin API via `requests` (already a project dependency)
instead of the AWS CLI or `garage` CLI, so it runs the same way on
PowerShell, bash, or any shell with Python.
"""
import os
import sys
from pathlib import Path

import boto3
import requests


def _load_dotenv(path: Path) -> None:
    """Minimal KEY=VALUE .env loader (no extra dependency needed for this
    one-off script). Existing environment variables take precedence."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"Error: {name} is required (did you load .env into this shell?)", file=sys.stderr)
        sys.exit(1)
    return value


def _garage_admin_request(method: str, path: str, **kwargs) -> requests.Response:
    admin_url = _required_env("GARAGE_ADMIN_URL")
    admin_token = _required_env("GARAGE_ADMIN_TOKEN")
    response = requests.request(
        method,
        f"{admin_url}{path}",
        headers={"Authorization": f"Bearer {admin_token}"},
        timeout=30,
        **kwargs,
    )
    return response


def bootstrap_garage_layout() -> None:
    """Assigns storage capacity to the single Garage node. A fresh Garage
    cluster has no layout and rejects all S3 operations until one is set."""
    status = _garage_admin_request("GET", "/v1/status").json()
    node_id = status["node"]
    current_role = next(
        (n.get("role") for n in status.get("nodes", []) if n.get("id") == node_id), None
    )

    if current_role is not None:
        print(f"Garage layout already assigned to node {node_id}")
        return

    print(f"Assigning Garage layout to node {node_id}...")
    layout = _garage_admin_request("GET", "/v1/layout").json()
    _garage_admin_request(
        "POST",
        "/v1/layout",
        json=[{"id": node_id, "zone": "garage", "capacity": 1_000_000_000_000, "tags": []}],
    ).raise_for_status()
    apply_response = _garage_admin_request(
        "POST", "/v1/layout/apply", json={"version": layout["version"] + 1}
    )
    apply_response.raise_for_status()
    print("  done")


def bootstrap_garage_key() -> None:
    """Registers the GARAGE_ACCESS_KEY_ID/SECRET from .env as a real Garage
    access key (imported keys are not auto-created from config). Garage
    requires access key IDs in its own format: `GK` + 24 hex chars."""
    access_key_id = _required_env("GARAGE_ACCESS_KEY_ID")
    secret_access_key = _required_env("GARAGE_SECRET_ACCESS_KEY")

    print(f"Importing Garage access key {access_key_id}...")
    response = _garage_admin_request(
        "POST",
        "/v1/key/import",
        json={
            "accessKeyId": access_key_id,
            "secretAccessKey": secret_access_key,
            "name": "dagster-pipeline",
        },
    )
    if response.ok:
        print("  done")
    else:
        print(f"  {response.status_code}: {response.text} (may already be imported)")


def _pipeline_buckets() -> list[str]:
    return [
        _required_env("GARAGE_BUCKET_RAW"),
        _required_env("GARAGE_BUCKET_STAGING"),
        _required_env("GARAGE_BUCKET_DATAMART"),
    ]


def create_garage_buckets() -> None:
    access_key_id = _required_env("GARAGE_ACCESS_KEY_ID")
    buckets = _pipeline_buckets()

    print("Creating Garage buckets and granting access...")
    for bucket in buckets:
        create_response = _garage_admin_request("POST", "/v1/bucket", json={"globalAlias": bucket})
        if not create_response.ok:
            print(f"  {bucket}: {create_response.status_code}: {create_response.text}")
            continue

        bucket_id = create_response.json()["id"]
        allow_response = _garage_admin_request(
            "POST",
            "/v1/bucket/allow",
            json={
                "bucketId": bucket_id,
                "accessKeyId": access_key_id,
                "permissions": {"read": True, "write": True, "owner": True},
            },
        )
        allow_response.raise_for_status()
        print(f"  {bucket}: created and access granted")


def configure_bucket_cors() -> None:
    """Allows the Lakekeeper web UI's browser-side table preview (DuckDB-WASM,
    reading Iceberg files directly from Garage) to pass the browser's CORS
    check. Garage only supports this via the S3 API, not its admin API."""
    endpoint_url = _required_env("GARAGE_ENDPOINT_URL")
    access_key_id = _required_env("GARAGE_ACCESS_KEY_ID")
    secret_access_key = _required_env("GARAGE_SECRET_ACCESS_KEY")
    region = os.environ.get("GARAGE_REGION", "garage")
    buckets = _pipeline_buckets()

    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=region,
    )

    print("Configuring CORS on Garage buckets...")
    for bucket in buckets:
        client.put_bucket_cors(
            Bucket=bucket,
            CORSConfiguration={
                "CORSRules": [
                    {
                        "AllowedOrigins": ["*"],
                        "AllowedMethods": ["GET", "HEAD"],
                        "AllowedHeaders": ["*"],
                        "ExposeHeaders": ["ETag"],
                        "MaxAgeSeconds": 3600,
                    }
                ]
            },
        )
        print(f"  {bucket}: done")


def bootstrap_lakekeeper() -> None:
    """A fresh Lakekeeper instance has no project and refuses to create a
    warehouse until it is bootstrapped (this also sets the initial admin)."""
    lakekeeper_uri = _required_env("LAKEKEEPER_URI")
    management_base = lakekeeper_uri.removesuffix("/catalog")

    print("Bootstrapping Lakekeeper...")
    response = requests.post(
        f"{management_base}/management/v1/bootstrap",
        json={"accept-terms-of-use": True},
        timeout=30,
    )
    if response.ok:
        print("  done")
    else:
        print(f"  {response.status_code}: {response.text} (may already be bootstrapped)")


def create_lakekeeper_warehouse() -> None:
    lakekeeper_uri = _required_env("LAKEKEEPER_URI")
    warehouse_name = _required_env("LAKEKEEPER_WAREHOUSE")
    # Lakekeeper runs inside the Docker network, so it must reach Garage via
    # the container-to-container hostname, not the host-facing localhost URL.
    endpoint_url = _required_env("GARAGE_INTERNAL_ENDPOINT_URL")
    access_key_id = _required_env("GARAGE_ACCESS_KEY_ID")
    secret_access_key = _required_env("GARAGE_SECRET_ACCESS_KEY")
    bucket_staging = _required_env("GARAGE_BUCKET_STAGING")
    region = os.environ.get("GARAGE_REGION", "garage")

    management_base = lakekeeper_uri.removesuffix("/catalog")
    payload = {
        "warehouse-name": warehouse_name,
        "storage-profile": {
            "type": "s3",
            "flavor": "s3-compat",
            "bucket": bucket_staging,
            "endpoint": endpoint_url,
            "region": region,
            "path-style-access": True,
            "sts-enabled": False,
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": access_key_id,
            "aws-secret-access-key": secret_access_key,
        },
    }

    print(f"Creating Lakekeeper warehouse '{warehouse_name}'...")
    response = requests.post(f"{management_base}/management/v1/warehouse", json=payload, timeout=30)
    if response.ok:
        print("  done")
    else:
        print(f"  {response.status_code}: {response.text}")


if __name__ == "__main__":
    _load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    bootstrap_garage_layout()
    bootstrap_garage_key()
    create_garage_buckets()
    configure_bucket_cors()
    bootstrap_lakekeeper()
    create_lakekeeper_warehouse()
    print('Verify with: docker compose exec trino trino --execute "SHOW CATALOGS"')
