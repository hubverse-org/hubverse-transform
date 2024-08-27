"""
Update metadata for all files in the raw/model-output/ directory of a specified AWS S3 bucket.
This script can be used for force re-triggering the lambda that transforms hubverse model-output files.
"""
import argparse
from datetime import datetime, timezone

import boto3
from botocore import exceptions as boto_exceptions


def main():
    parser = argparse.ArgumentParser(
        description="Re-trigger lambda that transforms hubverse model-output files for AWS S3 storage"
    )

    parser.add_argument(
        "s3_bucket",
        metavar="Hubverse S3 bucket",
        type=str,
        help="""
                        A Hubverse S3 bucket name. Metadata of the files in the raw/model-output/
                        directory of this bucket will be updated to trigger the transform lambda.
                        """,
    )

    args = parser.parse_args()
    s3_bucket = args.s3_bucket
    print(f"Updating metadata for all files in {s3_bucket}/raw/model-output/\n")

    update_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    updated_file_count = 0

    try:
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=s3_bucket, Prefix="raw/model-output/")

        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                print(f"Processing {key}")

                s3_resource = boto3.resource("s3")
                s3_object = s3_resource.Object(s3_bucket, key)
                s3_object.metadata.update({"manual-update": update_date})
                s3_object.copy_from(
                    CopySource={"Bucket": s3_bucket, "Key": key},
                    Metadata=s3_object.metadata,
                    MetadataDirective="REPLACE",
                )
                updated_file_count += 1

    except boto_exceptions.NoCredentialsError:
        print("No AWS credentials found. Please configure your AWS credentials.")
    except boto_exceptions.ClientError as e:
        print("Boto client error - ", e)
    except Exception as e:
        print("Error - ", e)

    print(f"Updated metadata for {updated_file_count} files in {s3_bucket}/raw/model-output/")


if __name__ == "__main__":
    main()
