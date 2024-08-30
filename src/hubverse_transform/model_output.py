# mypy: disable-error-code="operator,attr-defined"

import logging
import os
import re
from urllib.parse import quote, unquote

import pyarrow as pa
import pyarrow.parquet as pq
from cloudpathlib import AnyPath, S3Path
from pyarrow import csv, fs

# Log to stdout
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s -  %(levelname)s - %(name)s - %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.WARNING)


class ModelOutputHandler:
    """
    Transforms a Hubverse model-output file to a standard format.

    Attributes
    ----------
    input_uri: str
        URI of the incoming model-output file to be transformed.
    file_name : str
        Name of the incoming model-output file to be transformed.
    file_type : str
        Type of file to be transformed
        (.parquet, .pqt, .csv currently supported).
    fs: str
        The filesystem type of the incoming model-output file.
    output_uri: str
        URI of the transformed model-output file.
    round_id : str
        Name of the round_id associated with the model-output file.
    model_id : int
        Name of the model_id associated with the model-output file.
    """

    def __init__(self, hub_path: os.PathLike, mo_path: os.PathLike, output_path: os.PathLike):
        """
        Parameters
        ----------
        hub_path : os.PathLike
            The location of a Hubverse hub
            (e.g., S3 bucket name, local filepath).
        mo_path : os.PathLike
            The location of a single model-output file, excluding
            the hub_path (e.g., S3 key)
        output_path : os.PathLike
            Where the transformed model-output file will be saved.
        """

        # create sanitized input/output URIs
        sanitized_input_path = AnyPath(self._sanitize_uri(hub_path / mo_path))
        self.input_uri = str(sanitized_input_path)
        self.file_name = sanitized_input_path.stem
        self.file_type = sanitized_input_path.suffix
        sanitized_output_path = AnyPath(self._sanitize_uri(output_path / f"{self.file_name}.parquet"))
        self.output_uri = str(sanitized_output_path)

        # create pyarrow filesystem objects to read and write files
        input_filesystem = fs.FileSystem.from_uri(self.input_uri)
        self._fs_input = input_filesystem[0]
        self._input_file = input_filesystem[1]
        output_filesystem = fs.FileSystem.from_uri(self.output_uri)
        self._fs_output = output_filesystem[0]
        self._output_file = output_filesystem[1]

        # parse model-output file name into individual parts
        file_parts = self._parse_file(self.file_name)
        self.round_id = file_parts["round_id"]
        self.model_id = unquote(file_parts["model_id"])

        # filesystem must be supoprted
        if (file_system := self._fs_input.type_name) not in ["local", "s3"]:
            raise ValueError(f"Unsupported filesystem: {file_system}")
        else:
            self.fs = file_system

        # file must be a supported type
        if self.file_type not in [".csv", ".parquet", ".pqt"]:
            msg = f"Input file type {self.file_type} is not supported"
            self._raise_invalid_file_warning(self.input_uri, msg)

        # handle case when object creation is triggered without a file
        # (e.g., if someone manually creates a folder in an S3 bucket)
        if not mo_path.suffix:
            msg = "Input file has no extension"
            self._raise_invalid_file_warning(self.input_uri, msg)

    def __repr__(self):
        return f"ModelOutputHandler('{self.input_uri}')"

    def __str__(self):
        return f"Handle model-output data transforms for {self._input_file}."

    @classmethod
    def from_s3(cls, bucket_name: str, s3_key: str, origin_prefix: str = "raw") -> "ModelOutputHandler":
        """
        Factory method to create ModelOutputHandler for S3-based files.

        Use this method to instantiate a ModelOutputHandler object for
        model-output files store in an S3 bucket (for example, when
        transitions are invoked via an AWS lambda function).

        Parameters
        ----------
        bucket_name : str
            The S3 bucket that contains the model-output file.
        s3_key : str
            The S3 object key of the model-output file.
        origin_prefix : str, default="raw"
            The S3 prefix used to store a hub's original,
            pre-transformed data. Must be the first part of the s3_key.

        Returns
        -------
        ModelOutputHandler
            A new instance of ModelOutputHandler.

        Raises
        ------
        ValueError
            If the s3_key does not begin with the origin_prefix.

        Examples
        --------
        mo_handler = ModelOutputHandler.from_s3(
            "my-bucket",
            "original_files/2022-01-01_model_output.csv",
            "original_files"
            )
        """

        # ModelOutputHandler is designed to operate on original versions of model-output
        # data (i.e., as submitted my modelers). This check ensures that the file being
        # transformed has originated from wherever a hub keeps these "raw" (un-altered)
        # model-outputs.
        s3_mo_path = AnyPath(s3_key)
        if s3_mo_path.parts[0] != origin_prefix:
            raise ValueError(f"Model output path {s3_key} does not begin with {origin_prefix}.")

        s3_bucket_path = S3Path(f"s3://{bucket_name}")

        # Destination path = origin path w/o the origin prefix
        destination_path = str(s3_mo_path.relative_to(origin_prefix).parent)
        s3_output_path = S3Path(f"s3://{bucket_name}/{destination_path}")

        return cls(s3_bucket_path, s3_mo_path, s3_output_path)  # type: ignore

    def _raise_invalid_file_warning(self, path: str, msg: str) -> None:
        """Raise a warning if the class was instantiated with an invalid file."""

        logger.warning(
            {
                "message": msg,
                "file": path,
            }
        )
        raise UserWarning(msg)

    def _sanitize_uri(self, path: os.PathLike, safe=":/") -> str:
        """Sanitize URIs for use with pyarrow's filesystem."""

        # remove spaces at the end of a filename (e.g., my-model-output .csv) and
        # also at the beginning and end of the path string
        clean_path = AnyPath(str(path).replace(path.stem, path.stem.strip()))  # type: ignore
        clean_string = str(clean_path).strip()

        # encode the cleaned path (for example, any remaining spaces) so we can safely use it
        clean_uri = quote(str(clean_string), safe=safe)

        return clean_uri

    def _parse_file(cls, file_name: str) -> dict:
        """Parse model-output file name into individual parts."""

        # In practice, Hubverse hubs are formatting round_id as dates in YYYY-MM-DD format.
        # There's an open discussion about whether or not we want to support round_ids in other
        # formats, but until there's a final decision, this code will assume that each model-output
        # file begins with a YYYY-MM-DD round_id.
        # https://github.com/hubverse-org/hubValidations/discussions/13

        round_id_match = re.match(r"^\d{4}-\d{2}-\d{2}", file_name)
        if not round_id_match:
            raise ValueError(f"Unable to get YYYY-MM-DD round_id from file name {file_name}.")
        round_id = round_id_match.group(0)

        # model_id is anything that comes after round_id in the filename
        model_id_split = re.split(rf"{round_id}[-_]*", file_name)
        if not model_id_split or len(model_id_split) <= 1 or not model_id_split[-1]:
            raise ValueError(f"Unable to get model_id from file name {file_name}.")
        model_id = model_id_split[-1].strip()

        file_parts = {}
        file_parts["round_id"] = round_id
        file_parts["model_id"] = model_id

        logger.info(f"Parsed model-output filename: {file_parts}")
        return file_parts

    def read_file(self) -> pa.table:
        """Read model-output file into PyArrow table."""

        logger.info(f"Reading file: {self.input_uri}")

        if self.file_type == ".csv":
            model_output_file = self._fs_input.open_input_stream(self._input_file)
            # normalize incoming missing data values to null, regardless of data type
            options = csv.ConvertOptions(
                null_values=["na", "NA", "", " ", "null", "Null", "NaN", "nan"],
                strings_can_be_null=True,
                # temp fix: force location and output_type_id columns to string
                column_types={"location": pa.string(), "output_type_id": pa.string()},
            )
            model_output_table = csv.read_csv(model_output_file, convert_options=options)
        else:
            # temp fix: force location and output_type_id columns to string
            schema_new = pq.read_schema(self._input_file)
            for field_name in ["location", "output_type_id"]:
                field_idx = schema_new.get_field_index(field_name)
                if field_idx >= 0:
                    schema_new = schema_new.set(field_idx, pa.field(field_name, pa.string()))
            model_output_file = self._fs_input.open_input_file(self._input_file)
            model_output_table = pq.read_table(model_output_file, schema=schema_new)

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
            "model_id": pa.array([self.model_id for i in range(0, num_rows)]),
        }

        # Merge the new columns with the existing columns
        all_columns = existing_columns | new_columns
        updated_model_output_table = pa.Table.from_pydict(all_columns)

        return updated_model_output_table

    def write_parquet(self, updated_model_output_table: pa.table) -> str:
        """Write transformed model-output table to parquet file."""

        with self._fs_output.open_output_stream(self._output_file) as parquet_file:
            pq.write_table(updated_model_output_table, parquet_file)

        logger.info(f"Finished writing parquet file: {self.output_uri}")

        return self.output_uri

    def transform_model_output(self) -> str:
        """Transform model-output data and write to parquet file."""

        model_output_table = self.read_file()
        updated_model_output_table = self.add_columns(model_output_table)
        transformed_file_path = self.write_parquet(updated_model_output_table)

        return transformed_file_path
