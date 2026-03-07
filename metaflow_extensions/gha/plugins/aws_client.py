from __future__ import annotations

import os

import boto3


def make_s3_client():
    """
    Create an S3 client honoring S3-compatible endpoint env vars.

    This supports backends like Cloudflare R2 where endpoint_url is required.
    """
    endpoint_url = os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get(
        "METAFLOW_S3_ENDPOINT_URL"
    )
    if endpoint_url:
        return boto3.client("s3", endpoint_url=endpoint_url)
    return boto3.client("s3")
