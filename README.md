# Hubverse Transform
A package to perform data transformations on hubverse model-output files.

The package contains a `ModelOutputHandler` class that reads, transforms, and writes a single Hubverse-compliant model-output file.

Currently, its primary purpose is for use as an AWS Lambda function that transforms model-output files uploaded to hub S3 bucket.

## Usage

To install this package:

```bash
pip install git+https://github.com/Infectious-Disease-Modeling-Hubs/hubverse-transform.git
```

Sample usage:

```python
from hubverse_transform.model_output import ModelOutputHandler

# to use with a local model-output file

mo = ModelOutputHandler(
    '~/code/hubverse-cloud/model-output/UMass-flusion/2023-10-14-UMass-flusion.csv',
    '/.'

)
# read the original model-output file into an Arrow table
original_file = mo.read_file()

# add new columns to the original model_output data
transformed_data = mo.add_columns(original_file)

# write transformed data to parquet
# TODO: fix this up for local filesystem (it's currently designed for S3 writes)
# mo.write(transformed_data)
```

Sample output of the original and transformed data:
```
In [31]: original_file.take([0,1])
Out[31]:
pyarrow.Table
reference_date: date32[day]
location: string
horizon: int64
target: string
target_end_date: date32[day]
output_type: string
output_type_id: double
value: double
----
reference_date: [[2023-10-14,2023-10-14]]
location: [["01","01"]]
horizon: [[0,0]]
target: [["wk inc flu hosp","wk inc flu hosp"]]
target_end_date: [[2023-10-14,2023-10-14]]
output_type: [["quantile","quantile"]]
output_type_id: [[0.01,0.025]]
value: [[0,1.5810684371620558]]

In [36]: transformed_data.take([0,1])
Out[36]:
pyarrow.Table
reference_date: date32[day]
location: string
horizon: int64
target: string
target_end_date: date32[day]
output_type: string
output_type_id: double
value: double
round_id: string
model_id: string
----
reference_date: [[2023-10-14,2023-10-14]]
location: [["01","01"]]
horizon: [[0,0]]
target: [["wk inc flu hosp","wk inc flu hosp"]]
target_end_date: [[2023-10-14,2023-10-14]]
output_type: [["quantile","quantile"]]
output_type_id: [[0.01,0.025]]
value: [[0,1.5810684371620558]]
round_id: [["2023-10-14","2023-10-14"]]
model_id: [["UMass-flusion","UMass-flusion"]]
...
```

## Dev setup

If you'd like to contribute, this section has the setup instructions.

**Prerequisites**

The setup instructions below use [PDM](https://pdm-project.org/) to install Python, manage a Python virtual environment, and manage dependencies. However, PDM is only absolutely necessary for managing dependencies (because the lockfile is in PDM format), so other tools for Python installs and environments will work as well.

To install PDM: https://pdm-project.org/en/latest/#installation

**Setup**

Follow the directions below to set this project up on your local machine.

1. Clone this repository and change into the project directory.
2. Make sure you have a version of Python installed that meets the `requires-python` constraint in [pyproject.toml](pyproject.toml).

    **Note:** if you don't have Python installed, PDM can install it for you: `pdm python install 3.12.2`
3. Install the project dependencies (this will also create a virtual environment):

    ```bash
    pdm install
    ```
4. Verify that everything is working by running the test suite:

    ```bash
    pdm run pytest
    ```

To sync project dependencies after pulling upstream code changes:

```bash
pdm sync
```

## Adding Dependencies

This project uses PDM to manage dependencies and add them to a cross-platform lockfile.

To add a new dependency:

```bash
pdm add [package-name]
```

To add a new dev dependency:

```bash
pdm add --dev [package-name]
```

The `pdm add` command will install the package, add it to [`pyproject.toml`](pyproject.toml), and update [`pdm.lock`](pdm.lock).

Refer to [PDM's documentation](https://pdm-project.org/latest/usage/dependency/) for complete information about adding dependencies.


## Creating and deploying the AWS Lambda package

**Temporary: next step is to deploy updates to the lambda package via GitHub Actions**

To package the hubverse_transform code for deployment to the `hubverse-transform-model-output` AWS Lambda function:

1. Make sure you have the AWS CLI installed
2. Make sure PDM is installed (see the dev setup instructions above)
3. Make sure you have AWS credentials that allow writes to the `hubverse-assets` S3 bucket
4. From the root of this project, run the deploy script:
```bash
source deploy_lambda.sh
```