from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hubverse_transform.model_output import ModelOutputHandler
from pyarrow import csv, fs


@pytest.fixture()
def model_output_table() -> pa.Table:
    return pa.table(
        {
            'location': ['earth', 'vulcan', 'seti alpha'],
            'value': [11.11, 22.22, 33.33],
        }
    )


@pytest.fixture()
def test_csv_file(tmpdir, model_output_table) -> str:
    """Write a temporary csv file and return the URI for use in tests."""
    test_file_dir = Path(tmpdir.mkdir('raw'))
    test_file_name = '2420-01-01-janeswayaddition-voyager1.csv'
    test_file_path = str(test_file_dir.joinpath(test_file_name))
    local = fs.LocalFileSystem()

    with local.open_output_stream(test_file_path) as test_csv_file:
        csv.write_csv(model_output_table, test_csv_file)

    return str(test_file_path)


@pytest.fixture()
def test_parquet_file(tmpdir, model_output_table) -> str:
    """Write a temporary csv file and return the URI for use in tests."""
    test_file_dir = Path(tmpdir.mkdir('raw'))
    test_file_name = '2420-01-01-janeswayaddition-voyager1.parquet'
    test_file_path = str(test_file_dir.joinpath(test_file_name))
    local = fs.LocalFileSystem()

    with local.open_output_stream(test_file_path) as test_parquet_file:
        pq.write_table(model_output_table, test_parquet_file)

    return str(test_file_path)


def test_new_instance():
    input_uri = 'mock:bucket123/raw/prefix1/prefix2/2420-01-01-team_one-model.csv'
    output_uri = 'mock:bucket123/prefix1/prefix2'
    mo = ModelOutputHandler(input_uri, output_uri)
    assert mo.input_file == 'bucket123/raw/prefix1/prefix2/2420-01-01-team_one-model.csv'
    assert mo.output_path == 'bucket123/prefix1/prefix2'
    assert mo.file_name == '2420-01-01-team_one-model'
    assert mo.file_type == '.csv'
    assert mo.round_id == '2420-01-01'
    assert mo.team == 'team_one'
    assert mo.model == 'model'


@pytest.mark.parametrize(
    's3_key, expected_input_uri, expected_output_uri',
    [
        (
            'raw/prefix1/prefix2/2420-01-01-team-model.csv',
            's3://bucket123/raw/prefix1/prefix2/2420-01-01-team-model.csv',
            's3://bucket123/prefix1/prefix2',
        ),
        (
            'raw/model-output/prefix1/prefix2/2420-01-01-team-model.parquet',
            's3://bucket123/raw/model-output/prefix1/prefix2/2420-01-01-team-model.parquet',
            's3://bucket123/model-output/prefix1/prefix2',
        ),
        (
            'raw/prefix1/prefix2/prefix3/prefix4/2420-01-01-team-model.csv',
            's3://bucket123/raw/prefix1/prefix2/prefix3/prefix4/2420-01-01-team-model.csv',
            's3://bucket123/prefix1/prefix2/prefix3/prefix4',
        ),
        ('raw/2420-01-01-team-model.csv', 's3://bucket123/raw/2420-01-01-team-model.csv', 's3://bucket123/.'),
    ],
)
def test_from_s3(mocker, s3_key, expected_input_uri, expected_output_uri):
    mocker.patch('hubverse_transform.model_output.ModelOutputHandler.__init__', return_value=None)
    mo = ModelOutputHandler.from_s3('bucket123', s3_key)
    mo.__init__.assert_called_once_with(expected_input_uri, expected_output_uri)


def test_from_s3_alternate_origin_prefix(mocker):
    mocker.patch('hubverse_transform.model_output.ModelOutputHandler.__init__', return_value=None)
    mo = ModelOutputHandler.from_s3(
        'bucket123',
        'different-raw-prefix/prefix1/prefix2/2420-01-01-team-model.parquet',
        origin_prefix='different-raw-prefix',
    )
    mo.__init__.assert_called_once_with(
        's3://bucket123/different-raw-prefix/prefix1/prefix2/2420-01-01-team-model.parquet',
        's3://bucket123/prefix1/prefix2',
    )


def test_from_s3_missing_prefix():
    with pytest.raises(ValueError):
        ModelOutputHandler.from_s3('test-bucket', 'prefix1/2420-01-01-team_name-model.csv')


@pytest.mark.parametrize(
    'file_uri, expected_error',
    [
        ('mock:raw/prefix1/prefix2/2420-01-01-team-name-voyager1.csv', ValueError),
        ('mock:raw/prefix1/prefix2/round_id-team-model.csv', ValueError),
        ('mock:raw/prefix1/prefix2/2420-01-01-team-model-name.csv', ValueError),
    ],
)
def test_parse_s3_key_invalid_format(file_uri, expected_error):
    # ensure ValueError is raised for invalid model-output file name format
    with pytest.raises(ValueError):
        ModelOutputHandler(file_uri, 'mock:fake-output-uri')


def test_parse_input_file_invalid_type():
    input_uri = 'mock:raw/prefix1/prefix2/2000-01-01-team1-model1.jpg'

    with pytest.raises(NotImplementedError):
        ModelOutputHandler(input_uri, 'mock:fake-output-uri')


def test_add_columns(model_output_table):
    file_uri = 'mock:raw/prefix1/prefix2/2420-01-01-team-model.csv'
    mo = ModelOutputHandler(file_uri, 'mock:fake-output-uri')

    result = mo.add_columns(model_output_table)

    # transformed data should have 3 new columns: round_id, team, and model
    assert result.num_columns == 5
    assert set(['round_id', 'team_abbr', 'model_abbr']).issubset(result.column_names)


def test_added_column_values(model_output_table):
    file_uri = 'mock:raw/prefix1/prefix2/2420-01-01-janewaysaddiction-voyager1.csv'
    mo = ModelOutputHandler(file_uri, 'mock:fake-output-uri')

    result = mo.add_columns(model_output_table)

    # round_id, team, and model columns should each contain a single value
    # that matches round, team, and model as derived from the file name
    assert len(result.column('round_id').unique()) == 1
    result.column('team_abbr').unique()[0].as_py() == '2420-01-01'

    assert len(result.column('team_abbr').unique()) == 1
    result.column('team_abbr').unique()[0].as_py() == 'janewaysaddiction'

    assert len(result.column('model_abbr').unique()) == 1
    result.column('team_abbr').unique()[0].as_py() == 'voyager1'


def test_read_file_csv(test_csv_file, model_output_table):
    mo = ModelOutputHandler(test_csv_file, 'mock:fake-output-uri')
    assert mo.read_file() == model_output_table


def test_read_file_parquet(test_parquet_file, model_output_table):
    mo = ModelOutputHandler(test_parquet_file, 'mock:fake-output-uri')
    assert mo.read_file() == model_output_table


def test_write_parquet(tmpdir, model_output_table):
    output_dir = str(tmpdir.mkdir('model-output'))

    mo = ModelOutputHandler('mock:raw/prefix1/prefix2/2420-01-01-team-model.csv', output_dir)

    expected_output_file_path = str(Path(*[output_dir, f'{mo.file_name}.parquet']))
    actual_output_file_path = mo.write_parquet(model_output_table)

    assert actual_output_file_path == expected_output_file_path


def test_transform_model_output(test_csv_file, tmpdir):
    output_dir = str(tmpdir.mkdir('model-output'))
    mo = ModelOutputHandler(test_csv_file, output_dir)
    output_uri = mo.transform_model_output()

    input_path = Path(test_csv_file)
    output_path = Path(output_uri)

    assert output_path.suffix == '.parquet'
    assert 'raw' not in output_path.parts
    assert input_path.stem in output_path.stem
