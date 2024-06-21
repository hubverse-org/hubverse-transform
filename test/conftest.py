import csv
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from pyarrow import csv as pyarrow_csv
from pyarrow import fs
from pyarrow import parquet as pq


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


@pytest.fixture()
def model_output_table() -> pa.Table:
    """
    Simple model-output representation to test functions with a PyArrow table input.
    """
    return pa.table(
        {
            "location": ["earth", "vulcan", "seti alpha"],
            "value": [11.11, 22.22, 33.33],
        }
    )


@pytest.fixture()
def model_output_data() -> list[dict[str, Any]]:
    """
    Fixture that returns a list of model-output data representing multiple output types.
    This fixture is used as input for other fixtures that generate temporary .csv and .parquet files for testing.
    """

    model_output_fieldnames = [
        "reference_date",
        "location",
        "horizon",
        "target",
        "output_type",
        "output_type_id",
        "value",
    ]
    model_output_list = [
        ["2420-01-01", "US", "1 light year", "hospitalizations", "quantile", 0.5, 62],
        ["2420-01-01", "US", "1 light year", "hospitalizations", "quantile", 0.75, 50.1],
        ["2420-01-01", "03", 3, "hospitalizations", "mean", None, 33],
        ["1999-12-31", "US", "last month", "hospitalizations", "pmf", "large_increase", 2.597827508665773e-9],
    ]

    model_output_dict_list = [
        {field: value for field, value in zip(model_output_fieldnames, row)} for row in model_output_list
    ]

    return model_output_dict_list


@pytest.fixture()
def test_csv_file(tmpdir, model_output_data) -> str:
    """
    Write a temporary csv file and return the URI for use in tests.
    """
    test_file_dir = Path(tmpdir.mkdir("raw_csv"))
    test_file_name = "2420-01-01-janeswayaddition-voyager1.csv"
    test_file_path = str(test_file_dir.joinpath(test_file_name))

    fieldnames = model_output_data[0].keys()

    with open(test_file_path, "w", newline="") as test_csv_file:
        writer = csv.DictWriter(test_csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in model_output_data:
            writer.writerow(row)

    return str(test_file_path)


@pytest.fixture()
def test_parquet_file(tmpdir, test_csv_file) -> str:
    """
    Write a temporary parquet file and return the URI for use in tests.
    """
    test_file_dir = Path(tmpdir.mkdir("raw_parquet"))
    test_file_name = "2420-01-01-janeswayaddition-voyager1.parquet"
    test_file_path = str(test_file_dir.joinpath(test_file_name))
    local = fs.LocalFileSystem()

    # read our test csv so we can write it back out as a parquet file
    model_output_table = pyarrow_csv.read_csv(test_csv_file)
    with local.open_output_stream(test_file_path) as test_parquet_file:
        pq.write_table(model_output_table, test_parquet_file)

    return str(test_file_path)
