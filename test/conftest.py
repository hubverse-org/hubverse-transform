"""Shared pytest fixtures."""

import pytest
from pyarrow import fs


@pytest.fixture()
def s3_bucket_name() -> str:  # type: ignore
    """
    PyArrow will not create FileSystem objects for S3 buckets that don't exist. The test suite isn't writing anything to S3,
    so all we need is the name of an actual S3 bucket to pass to ModelOutputHandler. We could handle this via mocks, but those
    attempts made the test suite highly unreadable, and setting up LocalStack or another AWS testing mechanism is a big hammer
    for this purpose, so here's a fixture that will return a bucket name usable by the test suite.

    Ultimately, this difficulty probably warrants a closer look at the code structure, because it shouldn't be this hard to test!
    """

    bucket_list = ["hubverse-assets", "test-bucket", "bucket123"]
    for bucket in bucket_list:
        try:
            fs.FileSystem.from_uri(f"s3://{bucket}")
            return bucket
        except OSError:
            continue
        else:
            raise ValueError("No valid S3 bucket found for testing ModelOutputHandler")
