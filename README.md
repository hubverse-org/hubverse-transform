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

1. Python 3.12

    **Note:** There are several options for installing Python on your machine:
    - [download from python.org](https://www.python.org/downloads/)
    - [pyenv](https://github.com/pyenv/pyenv?tab=readme-ov-file#getting-pyenv) - handy tool for installing an managing multiple versions of Python on MacOS and Linux
    - [pyenv-win](https://github.com/pyenv-win/pyenv-win) - a fork of pyenv for Windows


2. A way to manage Python virtual environments

    There are many tools for managing Python virtual environments. The setup instructions below use `venv` which comes with Python, but if you prefer another virtual environment management tool, feel free to use it.

**Setup**

Follow the directions below to set this project up on your local machine.

1. Make sure that your current Python interpreter meets the `requires-python` constraint in [pyproject.toml](pyproject.toml).
2. Clone this repository and change into the project directory (`hubverse-transform`):
3. Create a Python virtual environment for the project:
    ```bash
    python -m venv .venv
    ```
4. Activate the virtual environment:
    ```bash
    # MacOs/Linux
    source .venv/bin/activate

    # Windows
    .venv\Scripts\activate
    ```
5. Install the project dependencies (including installing the project as an editable package):
    ```bash
    pip install -e . && pip install -r requirements/requirements-dev.txt
    ```
6. Verify that everything is working by running the test suite:

    ```bash
    pytest
    ```

## Adding Dependencies

Because we want a robust lockfile to use for reproducible builds, adding dependencies to the project is a multi-step process. Here we use `uv` to resolve and install the project's dependencies. However, `pip-tools` will also work (`uv` is a drop-in replacement for `pip-tools` and is much faster).

Prerequisites:
- ['uv'](https://github.com/astral-sh/uv?tab=readme-ov-file#getting-started)

1. Add the new dependency to [`pyproject.toml`](pyproject.toml) (don't be too prescriptive about versions):
    - Dependencies required for `hubverse_transform` to run should be added to the `dependencies` section.
    - Dependencies needed for development (for example, running tests or linting) should be added to the `dev` section of `project.optional-dependencies`.

2. Generate updated requirements files:

    ```bash
    uv pip compile pyproject.toml -o requirements/requirements.txt && uv pip compile pyproject.toml --extra dev -o requirements/requirements-dev.txt
    ```

3. Update project dependencies:

    **Note:** This package was originally developed on MacOS. If you have trouble installing the dependencies. `uv pip sync` has a [`--python-platform` flag](https://github.com/astral-sh/uv?tab=readme-ov-file#multi-platform-resolution) that can be used to specify the platform.

    ```bash
    # note: requirements-dev.txt contains the base requirements AND the dev requirements
    #
    # using pip
    pip install -r requirements/requirements-dev.txt
    #
    # alternately, you can use uv to install the dependencies: it is faster and has a
    # a handy sync option that will cleanup unused dependencieså
    uv pip sync requirements/requirements-dev.txt
    ```

## Creating and deploying the AWS Lambda package

**Temporary: next step is to deploy updates to the lambda package via GitHub Actions**

To package the hubverse_transform code for deployment to the `hubverse-transform-model-output` AWS Lambda function:

1. Make sure you have the AWS CLI installed
2. Make sure you have AWS credentials that allow writes to the `hubverse-assets` S3 bucket
3. From the root of this project, run the deploy script:
```bash
source deploy_lambda.sh
```