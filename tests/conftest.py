"""Shared pytest fixtures for metaflow-gha tests."""

import os

import boto3
import pytest
from moto import mock_aws

BUCKET = "test-bucket"

# Env vars that point to real S3-compatible endpoints (e.g. Cloudflare R2)
# and must be cleared so moto can intercept boto3 calls during tests.
_S3_ENDPOINT_VARS = ("AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL", "METAFLOW_S3_ENDPOINT_URL")


@pytest.fixture
def s3(monkeypatch):
    for var in _S3_ENDPOINT_VARS:
        monkeypatch.delenv(var, raising=False)
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client
