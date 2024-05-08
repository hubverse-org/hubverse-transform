import pathlib

import pyarrow.compute as pc
import pytest
from hubverse_transform.model_output import ModelOutputHandler
from pyarrow import parquet


@pytest.fixture()
def test_file_path() -> pathlib.Path:
    """
    Return path to the integration test files.
    """
    test_file_path = pathlib.Path(__file__).parent.joinpath("data")
    return test_file_path


def test_missing_model_output_id_numeric(tmpdir, test_file_path):
    """Test behavior of model_output_id columns when there are a mix of numeric and missing output_type_ids."""
    output_dir = str(tmpdir.mkdir("model-output"))
    file_path = str(test_file_path.joinpath("2024-07-07-teamabc-output_type_ids_numeric.csv"))
    mo = ModelOutputHandler(file_path, output_dir)
    output_uri = mo.transform_model_output()

    # read the output parquet file
    transformed_output = parquet.read_table(output_uri)

    # missing data values (e.g., NA) in a numeric column should be normalized to null
    expr = pc.field("output_type_id").is_null()
    null_output_type_rows = transformed_output.filter(expr)
    assert len(null_output_type_rows) == 2


def test_missing_model_output_id_mixture(tmpdir, test_file_path):
    """Test behavior of model_output_id columns when there are a mix of numeric, string, and missing output_type_ids."""
    output_dir = str(tmpdir.mkdir("model-output"))
    file_path = str(test_file_path.joinpath("2024-07-07-teamabc-output_type_ids_mixed.csv"))
    mo = ModelOutputHandler(file_path, output_dir)
    output_uri = mo.transform_model_output()

    # read the output parquet file
    transformed_output = parquet.read_table(output_uri)

    # missing data values (e.g., NA) in a string column should be transformed to null
    expr = pc.field("output_type_id").is_null()
    null_output_type_rows = transformed_output.filter(expr)
    assert len(null_output_type_rows) == 8
