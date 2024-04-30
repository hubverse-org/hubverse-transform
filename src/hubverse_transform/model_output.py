import logging
import pathlib
import re

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv, fs

# Log to stdout
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s -  %(levelname)s - %(name)s - %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class ModelOutputHandler:
    def __init__(self, input_uri: str, output_uri: str):
        input_filesystem = fs.FileSystem.from_uri(input_uri)
        self.fs_input = input_filesystem[0]
        self.input_file = input_filesystem[1]

        output_filesystem = fs.FileSystem.from_uri(output_uri)
        self.fs_output = output_filesystem[0]
        self.output_path = output_filesystem[1]

        # get file name and type from input file
        path = pathlib.Path(self.input_file)
        self.file_name = path.stem
        self.file_type = path.suffix

        # TODO: Add other input file types as needed
        if self.file_type not in [".csv", ".parquet"]:
            raise NotImplementedError(f"Unsupported file type: {path.suffix}")

        # Parse model-output file name into individual parts
        # (round_id, team, model)
        file_parts = self.parse_file(self.file_name)
        self.round_id = file_parts["round_id"]
        self.team = file_parts["team"]
        self.model = file_parts["model"]

    def __repr__(self):
        return f"ModelOutputHandler('{self.fs_input.type_name}', '{self.input_file}', '{self.output_path}')"

    def __str__(self):
        return f"Handle model-output data transforms for {self.input_file}."

    @classmethod
    def from_s3(cls, bucket_name: str, s3_key: str, origin_prefix: str = "raw") -> "ModelOutputHandler":
        """Instantiate ModelOutputHandler for file on AWS S3."""

        # ModelOutputHandler is designed to operate on original versions of model-output
        # data (i.e., as submitted my modelers). This check ensures that the file being
        # transformed has originated from wherever a hub keeps these "raw" (un-altered)
        # model-outputs.
        path = pathlib.Path(s3_key)
        if path.parts[0] != origin_prefix:
            raise ValueError(f"Model output path {s3_key} does not begin with {origin_prefix}.")

        s3_input_uri = f"s3://{bucket_name}/{s3_key}"

        # Destination path = origin path w/o the origin prefix
        destination_path = str(path.relative_to(origin_prefix).parent)
        s3_output_uri = f"s3://{bucket_name}/{destination_path}"

        return cls(s3_input_uri, s3_output_uri)

    def parse_file(cls, file_name: str) -> dict:
        """Parse model-output file name into individual parts."""

        # TODO: verify assumptions about format of model-output filenames
        # Code below assumes [round_id likely yyyy-mm-dd]-[team]-[model] AND
        # that there are no hyphens in team or model names
        # https://github.com/orgs/Infectious-Disease-Modeling-Hubs/discussions/10
        file_name_split = file_name.rsplit("-", 2)

        if (
            file_name.count("-") > 4
            or len(file_name_split) != 3
            or not re.match(r"^\d{4}-\d{2}-\d{2}$", file_name_split[0])
        ):
            raise ValueError(f"File name {file_name} not in expected model-output format: yyyy-mm-dd-team-model.")

        file_parts = {}
        file_parts["round_id"] = file_name_split[0]
        file_parts["team"] = file_name_split[1]
        file_parts["model"] = file_name_split[2]

        # TODO: why so many logs?
        logger.info(f"Parsed model-output filename: {file_parts}")
        return file_parts

    def read_file(self) -> pa.table:
        """Read model-output file into PyArrow table."""

        logger.info(f"Reading file: {self.input_file}")

        if self.file_type == ".csv":
            model_output_file = self.fs_input.open_input_stream(self.input_file)
            # normalize incoming missing data values to null, regardless of data type
            options = csv.ConvertOptions(
                null_values=["na", "NA", "", " ", "null", "Null", "NaN", "nan"], strings_can_be_null=True
            )
            model_output_table = csv.read_csv(model_output_file, convert_options=options)
        else:
            # parquet requires random access reading (because metadata),
            # so we use open_input_file instead of open_intput_stream
            model_output_file = self.fs_input.open_input_file(self.input_file)
            model_output_table = pq.read_table(model_output_file)

        return model_output_table

    def add_columns(self, model_output_table: pa.table) -> pa.table:
        """Add model-output metadata columns to PyArrow table."""

        num_rows = model_output_table.num_rows
        logger.info(f"Adding columns to table with {num_rows} rows")

        # Create a dictionary of the existing columns
        existing_columns = {name: model_output_table[name] for name in model_output_table.column_names}

        # Create arrays that we'll use to append columns to the table
        new_columns = {
            "round_id": pa.array([self.round_id for i in range(0, num_rows)]),
            "team_abbr": pa.array([self.team for i in range(0, num_rows)]),
            "model_abbr": pa.array([self.model for i in range(0, num_rows)]),
        }

        # Merge the new columns with the existing columns
        all_columns = existing_columns | new_columns
        updated_model_output_table = pa.Table.from_pydict(all_columns)

        return updated_model_output_table

    def write_parquet(self, updated_model_output_table: pa.table) -> str:
        """Write transformed model-output table to parquet file."""

        transformed_file_path = f"{self.output_path}/{self.file_name}.parquet"

        with self.fs_output.open_output_stream(transformed_file_path) as parquet_file:
            pq.write_table(updated_model_output_table, parquet_file)

        logger.info(f"Finished writing parquet file: {transformed_file_path}")

        return transformed_file_path

    def transform_model_output(self) -> str:
        """Transform model-output data and write to parquet file."""

        model_output_table = self.read_file()
        updated_model_output_table = self.add_columns(model_output_table)
        transformed_file_path = self.write_parquet(updated_model_output_table)

        return transformed_file_path
