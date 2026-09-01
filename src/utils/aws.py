"""
Single factory for every AWS client in the portfolio.

Swapping the emulator (MiniStack -> moto -> real AWS) is a one-line change:
set AWS_ENDPOINT_URL, or unset it entirely to hit real AWS.
"""

import os

import boto3

_DEFAULT_ENDPOINT = "http://localhost:4566"
_DEFAULT_REGION = "us-east-1"


def client(service_name: str):
    endpoint_url = os.environ.get("AWS_ENDPOINT_URL", _DEFAULT_ENDPOINT) or None
    return boto3.client(
        service_name,
        endpoint_url=endpoint_url,
        region_name=os.environ.get("AWS_REGION", _DEFAULT_REGION),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    )


def resource(service_name: str):
    endpoint_url = os.environ.get("AWS_ENDPOINT_URL", _DEFAULT_ENDPOINT) or None
    return boto3.resource(
        service_name,
        endpoint_url=endpoint_url,
        region_name=os.environ.get("AWS_REGION", _DEFAULT_REGION),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    )
