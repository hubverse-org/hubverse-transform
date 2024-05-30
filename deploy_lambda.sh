#!/bin/bash

# allow pip installs outside of a virtual environment
export PIP_REQUIRE_VIRTUALENV=false

build_dir="build"

# create build directory if it doesn't exist, and remove any prior
# artifacts
echo "Removing old build artifacts"
rm -rf $build_dir
mkdir -p $build_dir/hubverse_transform

# install the hubverse_transform dependencies into the build directory
echo "Installing dependencies into the build directory"
pip install \
--platform manylinux2014_x86_64 \
--target=$build_dir \
--python-version 3.12 \
--only-binary=:all: --upgrade \
-r requirements/requirements.txt

# copy the hubverse_transform package into the build directory so it
# will be included in the lambda deployment package
cp -r src/hubverse_transform/ $build_dir/hubverse_transform

# create the zip file that will be deployed to AWS Lambda
# https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-package.html#gettingstarted-package-zip
echo "Creating AWS Lambda deployment package"

# step 1: zip the files in the build directory (from the above pip install, but exclude the stuff we don't need)
echo "Zipping project dependencies"
cd $build_dir
py_exclude=("*.pyc" "*.ipynb" "*__pycache__*" "*ipynb_checkpoints*" "requirements.txt" "*.egg-info")
zip -r hubverse-transform-model-output.zip . -x "${py_exclude[@]}"

# step 2: add the lambda handler the .zip package
echo "Adding lambda handler to the deployment .zip package"
cd ..
zip -j $build_dir/hubverse-transform-model-output.zip faas/lambda_function.py

# for reference: the S3 bucket in the comment below is where our IaC (i.e., hubverse-infrastructure repo)
# creates the placeholder lambda function for model-output-transforms
# s3://hubverse-assets/lambda/hubverse-transform-model-output.zip
echo "Uploading deployment package to S3 and performing aws lambda update-function-code"
aws s3 cp build/hubverse-transform-model-output.zip s3://hubverse-assets/lambda/
aws lambda update-function-code \
  --function-name arn:aws:lambda:us-east-1:767397675902:function:hubverse-transform-model-output \
  --s3-bucket hubverse-assets \
  --s3-key lambda/hubverse-transform-model-output.zip > $build_dir/lambda_update.log

echo "Lambda function updated (see $build_dir/lambda_update.log for details)"
