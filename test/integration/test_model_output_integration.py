import pathlib

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from pyarrow import parquet

from hubverse_transform.model_output import ModelOutputHandler


@pytest.fixture()
def test_file_path() -> pathlib.Path:
    """
    Return path to the integration test files.
    """
    test_file_path = pathlib.Path(__file__).parent.joinpath("data")
    return test_file_path


@pytest.fixture()
def expected_model_output_schema() -> pa.Schema:
    """
    Return the expected Arrow schema for parquet files written by the ModelOutputHandler.
    """
    expected_schema = pa.schema(
        [
            pa.field("reference_date", pa.date32()),
            pa.field("target", pa.string()),
            pa.field("horizon", pa.int64()),
            pa.field("target_end_date", pa.date32()),
            pa.field("location", pa.string()),
            pa.field("output_type", pa.string()),
            pa.field("output_type_id", pa.string()),
            pa.field("value", pa.float64()),
            pa.field("round_id", pa.string()),
            pa.field("model_id", pa.string()),
        ]
    )
    return expected_schema


def test_missing_model_output_id_numeric(tmpdir, test_file_path):
    """Test behavior of model_output_id columns when there are a mix of numeric and missing output_type_ids."""
    output_path = pathlib.Path(tmpdir.mkdir("model-output"))
    mo_path = test_file_path.joinpath("2024-07-07-teamabc-output_type_ids_numeric.csv")
    mo = ModelOutputHandler(pathlib.Path(tmpdir), mo_path, output_path)
    output_uri = mo.add_model_output()

    # read the output parquet file
    transformed_output = parquet.read_table(output_uri)

    # missing data values (e.g., NA) in a numeric column should be normalized to null
    expr = pc.field("output_type_id").is_null()
    null_output_type_rows = transformed_output.filter(expr)
    assert len(null_output_type_rows) == 2


def test_missing_model_output_id_mixture(tmpdir, test_file_path):
    """Test behavior of model_output_id columns when there are a mix of numeric, string, and missing output_type_ids."""
    output_path = pathlib.Path(tmpdir.mkdir("model-output"))
    mo_path = test_file_path.joinpath("2024-07-07-teamabc-output_type_ids_mixed.csv")
    mo = ModelOutputHandler(pathlib.Path(tmpdir), mo_path, output_path)
    output_uri = mo.add_model_output()

    # read the output parquet file
    transformed_output = parquet.read_table(output_uri)

    # missing data values (e.g., NA) in a string column should be transformed to null
    expr = pc.field("output_type_id").is_null()
    null_output_type_rows = transformed_output.filter(expr)
    assert len(null_output_type_rows) == 8


def test_model_output_csv_schema(tmpdir, test_file_path, expected_model_output_schema):
    """Test the parquet schema on files written by ModelOutputHandler."""
    mo_path = "2024-05-04-teamabc-locations_numeric.csv"
    mo_full_path = test_file_path.joinpath(mo_path)
    output_path = pathlib.Path(tmpdir.mkdir("model-output"))
    mo = ModelOutputHandler(pathlib.Path(tmpdir), mo_full_path, output_path)
    output_uri = mo.add_model_output()

    actual_schema = pq.read_metadata(output_uri).schema.to_arrow_schema()
    assert expected_model_output_schema.equals(actual_schema)

    # read the output parquet file
    transformed_output = parquet.read_table(output_uri)
    assert len(transformed_output) == 23

    # location is transformed to string
    assert set(transformed_output["location"].to_pylist()) == {"02"}

    # output_type_id transformed to string
    assert transformed_output["output_type_id"].to_pylist()[0] == "0.01"


def test_model_output_parquet_schema(tmpdir, test_file_path, expected_model_output_schema):
    """Test the parquet schema on files written by ModelOutputHandler."""
    mo_path = "2024-05-04-teamabc-locations_numeric.parquet"
    mo_full_path = test_file_path.joinpath(mo_path)
    output_path = pathlib.Path(tmpdir.mkdir("model-output"))
    mo = ModelOutputHandler(pathlib.Path(tmpdir), mo_full_path, output_path)
    output_uri = mo.add_model_output()

    # check schema of the original model_output parquet files so we can verify that
    # model_output_id and location are correctly transformed to string
    original_schema = pq.read_metadata(mo_full_path).schema.to_arrow_schema()
    pa.types.is_float64(original_schema.field("output_type_id").type)
    pa.types.is_int64(original_schema.field("location").type)

    actual_schema = pq.read_metadata(output_uri).schema.to_arrow_schema()
    assert expected_model_output_schema.equals(actual_schema)

    # read the output parquet file
    transformed_output = parquet.read_table(output_uri)
    assert len(transformed_output) == 23

    # location is transformed to string (no leading zeroes because original parquet column is int)
    assert set(transformed_output["location"].to_pylist()) == {"2"}

    # output_type_id transformed to string (leading zeroes retained because original parquet column is float)
    assert transformed_output["output_type_id"].to_pylist()[0] == "0.01"


@pytest.mark.parametrize(
    "file_ext",
    [
        ("parquet"),
        ("csv"),
    ],
)
def test_delete_model_output(tmp_path, test_file_path, file_ext):
    file_name = "2024-05-04-teamabc-locations_numeric"
    hub_path = tmp_path / "test_hub"
    mo_path = pathlib.Path("raw") / "model-output" / "teamabc" / f"{file_name}.{file_ext}"
    output_path = hub_path / "model-output" / "test_model"
    output_file = output_path / f"{file_name}.parquet"
    output_path.mkdir(parents=True)
    output_file.touch()

    extra_output_file = output_path / "2024-05-11-teamabc-locations_numeric.parquet"
    extra_output_file.touch()

    mo = ModelOutputHandler(hub_path, mo_path, output_path)
    assert len(list(output_path.iterdir())) == 2
    mo.delete_model_output()
    assert len(list(output_path.iterdir())) == 1

    # test missing model output file
    with pytest.raises(UserWarning):
        mo.delete_model_output()
    assert len(list(output_path.iterdir())) == 1
