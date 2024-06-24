"""Unit tests for the ModelOutputHandler class."""

import csv
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from cloudpathlib import AnyPath
from hubverse_transform.model_output import ModelOutputHandler
from pyarrow import csv as pyarrow_csv
from pyarrow import fs
from pyarrow import parquet as pq

# the mocker fixture used throughout is provided by pytest-mock
# see conftest.py for definition of other fixtures (e.g., s3_bucket_name)


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


def test_new_instance():
    hub_path = AnyPath("mock:bucket123")
    mo_path = AnyPath("raw/prefix1/prefix2/2420-01-01-team_one-model.csv")
    output_path = AnyPath("mock:bucket123/prefix1/prefix2")
    mo = ModelOutputHandler(hub_path, mo_path, output_path)
    assert mo.input_file == "bucket123/raw/prefix1/prefix2/2420-01-01-team_one-model.csv"
    assert mo.output_path == "bucket123/prefix1/prefix2"
    assert mo.file_name == "2420-01-01-team_one-model"
    assert mo.file_type == ".csv"
    assert mo.round_id == "2420-01-01"
    assert mo.model_id == "team_one-model"


@pytest.mark.parametrize(
    "mo_path_str, expected_round_id, expected_model_id",
    [
        ("raw/prefix/2420-01-01-team-model.csv", "2420-01-01", "team-model"),
        ("raw/prefix/2420-01-01-----team-model.parquet", "2420-01-01", "team-model"),
        ("raw/prefix/2420-01-01____teammodelallonestring.csv", "2420-01-01", "teammodelallonestring"),
        ("raw/prefix/2420-01-01____look-at-all-the-hyphens-.csv", "2420-01-01", "look-at-all-the-hyphens-"),
    ],
)
def test_parse_file(tmpdir, s3_bucket_name, mo_path_str, expected_round_id, expected_model_id):
    """Test getting round_id and model_id from file name."""
    mo_path = AnyPath(mo_path_str)
    output_path = AnyPath(tmpdir)

    # test parsing with S3 path and local path
    hub_path_list = [AnyPath(f"s3://{s3_bucket_name}"), AnyPath(tmpdir)]
    for hub_path in hub_path_list:
        mo = ModelOutputHandler(hub_path, mo_path, output_path)
        assert mo.round_id == expected_round_id
        assert mo.model_id == expected_model_id


@pytest.mark.parametrize(
    "mo_path_str, expected_input_file, expected_output_path, expected_file_name, expected_model_id",
    [
        (
            "raw/prefix1/prefix 2/2420-01-01-team-model name with spaces.csv",
            "raw/prefix1/prefix 2/2420-01-01-team-model name with spaces.csv",
            "prefix1/prefix 2",
            "2420-01-01-team-model name with spaces",
            "team-model name with spaces",
        ),
        (
            "raw/prefix1/~prefix 2/2420-01-01-team-model.name.pqt",
            "raw/prefix1/~prefix 2/2420-01-01-team-model.name.pqt",
            "prefix1/~prefix 2",
            "2420-01-01-team-model.name",
            "team-model.name",
        ),
        (
            "raw/raw/prefix 1/prefix2/2420-01-01-spáces at end .csv",
            "raw/raw/prefix 1/prefix2/2420-01-01-spáces at end.csv",
            "raw/prefix 1/prefix2",
            "2420-01-01-spáces at end",
            "spáces at end",
        ),
        (
            "raw/prefix 1/prefix 🐍/2420-01-01 look ma no hyphens.csv",
            "raw/prefix 1/prefix 🐍/2420-01-01 look ma no hyphens.csv",
            "prefix 1/prefix 🐍",
            "2420-01-01 look ma no hyphens",
            "look ma no hyphens",
        ),
    ],
)
def test_from_s3_special_characters(
    s3_bucket_name, mo_path_str, expected_input_file, expected_output_path, expected_file_name, expected_model_id
):
    """Test special characters when instantiating ModelOutputHandler via from_s3"""
    # ensure spaces and other characters in directory, filename, s3 key, etc. are handled correctly
    mo = ModelOutputHandler.from_s3(s3_bucket_name, mo_path_str)
    assert mo.input_file == f"{s3_bucket_name}/{expected_input_file}"
    assert mo.output_path == f"{s3_bucket_name}/{expected_output_path}"
    assert mo.file_name == expected_file_name
    assert mo.model_id == expected_model_id


@pytest.mark.parametrize(
    "s3_key, expected_input_uri, expected_output_uri",
    [
        (
            "raw/prefix1/prefix2/2420-01-01-team-model.csv",
            "s3://hubverse-test/raw/prefix1/prefix2/2420-01-01-team-model.csv",
            "s3://hubverse-test/prefix1/prefix2",
        ),
        (
            "raw/model-output/prefix1/prefix2/2420-01-01-team-model.parquet",
            "s3://hubverse-test/raw/model-output/prefix1/prefix2/2420-01-01-team-model.parquet",
            "s3://hubverse-test/model-output/prefix1/prefix2",
        ),
        (
            "raw/prefix1/prefix2/prefix3/prefix4/2420-01-01-team-model.csv",
            "s3://hubverse-test/raw/prefix1/prefix2/prefix3/prefix4/2420-01-01-team-model.csv",
            "s3://hubverse-test/prefix1/prefix2/prefix3/prefix4",
        ),
        ("raw/2420-01-01-team-model.csv", "s3://hubverse-test/raw/2420-01-01-team-model.csv", "s3://hubverse-test/."),
    ],
)
def test_from_s3(mocker, s3_key, expected_input_uri, expected_output_uri):
    s3_expected_input_path = AnyPath(expected_input_uri)
    hub_path = s3_expected_input_path.parents[-1]
    mo_path = AnyPath(s3_expected_input_path.key)

    mocker.patch("hubverse_transform.model_output.ModelOutputHandler.__init__", return_value=None)
    mo = ModelOutputHandler.from_s3("hubverse-test", s3_key)
    mo.__init__.assert_called_once_with(hub_path, mo_path, AnyPath(expected_output_uri))


def test_from_s3_alternate_origin_prefix(mocker):
    mocker.patch("hubverse_transform.model_output.ModelOutputHandler.__init__", return_value=None)

    mo = ModelOutputHandler.from_s3(
        "hubverse-test",
        "different-raw-prefix/prefix1/prefix2/2420-01-01-team-model.snappy.parquet",
        origin_prefix="different-raw-prefix",
    )
    mo.__init__.assert_called_once_with(
        AnyPath("s3://hubverse-test"),
        AnyPath("different-raw-prefix/prefix1/prefix2/2420-01-01-team-model.snappy.parquet"),
        AnyPath("s3://hubverse-test/prefix1/prefix2"),
    )


def test_from_s3_missing_prefix():
    with pytest.raises(ValueError):
        ModelOutputHandler.from_s3("hubverse-test", "prefix1/2420-01-01-team_name-model.csv")


@pytest.mark.parametrize(
    "file_uri, expected_error",
    [
        ("raw/prefix1/prefix2/2420-01-01.csv", ValueError),
        ("raw/prefix1/prefix2/round_id-team-model.csv", ValueError),
        ("raw/prefix1/prefix2/01-02-2440-team-model-name.csv", ValueError),
    ],
)
def test_parse_s3_key_invalid_format(tmpdir, file_uri, expected_error):
    # ensure ValueError is raised for invalid model-output file name format
    with pytest.raises(expected_error):
        hub_path = AnyPath(tmpdir)
        mo_path = AnyPath(file_uri)
        ModelOutputHandler(hub_path, mo_path, hub_path)


def test_add_columns(tmpdir, model_output_table):
    hub_path = AnyPath(tmpdir)
    mo_path = AnyPath("raw/prefix1/prefix2/2420-01-01-team-model.csv")
    mo = ModelOutputHandler(hub_path, mo_path, hub_path)

    result = mo.add_columns(model_output_table)

    # transformed data should have 2 new columns: round_id and model_id
    assert result.num_columns == 4
    assert set(["round_id", "model_id"]).issubset(result.column_names)


def test_added_column_values(tmpdir, model_output_table):
    hub_path = AnyPath(tmpdir)
    mo_path = AnyPath("raw/prefix1/prefix2/2420-01-01-janewaysaddiction-voyager1.csv")

    mo = ModelOutputHandler(hub_path, mo_path, hub_path)

    result = mo.add_columns(model_output_table)

    # round_id and model_id columns should each contain a single value
    # that matches round and model as derived from the file name
    assert len(result.column("round_id").unique()) == 1
    assert result.column("round_id").unique()[0].as_py() == "2420-01-01"

    assert len(result.column("model_id").unique()) == 1
    result.column("model_id").unique()[0].as_py() == "janewaysaddiction-voyager1"


def test_read_file_csv(tmpdir, test_csv_file, model_output_table):
    hub_path = AnyPath(tmpdir)
    mo_path = AnyPath(test_csv_file)
    mo = ModelOutputHandler(hub_path, mo_path, hub_path)
    pyarrow_table = mo.read_file()
    assert len(pyarrow_table) == 4

    # empty output_type_id should transform to null
    output_type_id_col = pyarrow_table.column("output_type_id")
    assert str(output_type_id_col[0]) == "0.5"
    assert pa.compute.is_null(output_type_id_col[2]).as_py() is True
    assert str(output_type_id_col[3]) == "large_increase"


def test_read_file_parquet(tmpdir, test_parquet_file, model_output_table):
    hub_path = AnyPath(tmpdir)
    mo_path = AnyPath(test_parquet_file)
    mo = ModelOutputHandler(hub_path, mo_path, hub_path)
    pyarrow_table = mo.read_file()
    assert len(pyarrow_table) == 4

    # output_type_id should retain the value from the .csv file, even when the value is empty or "NA"
    output_type_id_col = pyarrow_table.column("output_type_id")
    assert str(output_type_id_col[0]) == "0.5"
    assert str(output_type_id_col[2]) == ""
    assert str(output_type_id_col[3]) == "large_increase"


def test_write_parquet(tmpdir, model_output_table):
    hub_path = AnyPath(tmpdir)
    mo_path = AnyPath("raw/prefix1/prefix2/2420-01-01-team-model.csv")
    output_path = AnyPath(tmpdir.mkdir("model-output"))

    mo = ModelOutputHandler(hub_path, mo_path, output_path)

    expected_output_file_path = str(Path(*[output_path, f"{mo.file_name}.parquet"]))
    actual_output_file_path = mo.write_parquet(model_output_table)

    assert actual_output_file_path == expected_output_file_path


def test_transform_model_output_path(test_csv_file, tmpdir):
    hub_path = AnyPath(tmpdir)
    mo_path = AnyPath(test_csv_file)
    output_path = AnyPath(str(tmpdir.mkdir("model-output")))

    mo = ModelOutputHandler(hub_path, mo_path, output_path)
    output_uri = mo.transform_model_output()

    input_path = Path(test_csv_file)
    output_path = Path(output_uri)

    assert output_path.suffix == ".parquet"
    assert "raw" not in output_path.parts
    assert input_path.stem in output_path.stem


@pytest.mark.parametrize(
    "file_uri",
    [
        ("raw/prefix1/prefix2/"),
        ("raw/prefix1/prefix2/round_id-team-model.txt"),
        ("photo.jpg"),
        ("raw/prefix1/prefix2/01-02-2440-team-model-name"),
    ],
)
def test_invalid_file_warning(tmpdir, file_uri):
    # ensure ValueError is raised for invalid model-output file name format

    hub_path = AnyPath(tmpdir)
    mo_path = AnyPath(file_uri)
    with pytest.raises(UserWarning):
        ModelOutputHandler(hub_path, mo_path, hub_path)
