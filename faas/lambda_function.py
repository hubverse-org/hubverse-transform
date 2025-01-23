"""
This the handler invoked by the Lambda function that runs whenever there's a change to a hub's uploaded model-output files.
For the Hubverse AWS account, the baseline "hubverse-transform-model-output" Lambda is defined via our IaC (Infrastructure as Code)
repository: hubverse-transform-model-output.

This handler, along with the actual transformation module (hubverse_transform), live outside of the Hubverse's IaC repository:

- To avoid tightly coupling AWS infrastructure to the more general hubverse_transform module that can be used for hubs hosted elsewhere
- To allow faster iteration and testing of the hubverse_transform module without needing to update the IaC repo or redeploy AWS resources
"""
import json
import logging
import urllib.parse

from hubverse_transform.model_output import ModelOutputHandler

logger = logging.getLogger()
logger.setLevel("INFO")


def lambda_handler(event, context):
    logger.info("Received event: " + json.dumps(event, indent=2))

    # info from the S3 event
    event_source = event["Records"][0]["eventSource"]
    event_name = event["Records"][0]["eventName"]
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(event["Records"][0]["s3"]["object"]["key"], encoding="utf-8")

    try:
        mo = ModelOutputHandler.from_s3(bucket, key)
        if "objectcreated" in event_name.lower():
            logger.info(f"Adding file: {bucket}/{key}")
            mo.add_model_output()
        elif "objectremoved" in event_name.lower():
            logger.info(f"Deleting file: {bucket}/{key}")
            mo.delete_model_output()
        else:
            logger.info({
                "msg": "Event type not supported, skipping",
                "event_source": event_source,
                "event": event_name,
                "file": f"{bucket}/{key}"
            })
            return
    except UserWarning:
        pass
    except Exception as e:
        logger.exception({
            "msg": "Error handling file",
            "event": event_name,
            "event_source": event_source,
            "file": f"{bucket}/{key}",
            "error": str(e)
        })
