import json
import boto3
import random
import string
import os
import logging

logger = logging.getLogger()
logger.setLevel("INFO")

transcribe = boto3.client("transcribe")

def lambda_handler(event, context):
    """
    Receives an event from S3 and starts an Amazon Transcribe job with the 
    event object as the job media.
    """

    logger.info("# EVENT")
    logger.info(event)

    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    if key == "source/":
        logger.info("Source folder, skipping")
        return {
            "statusCode": 200,
            "body": "Source folder, skipping"
        }

    job_id_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))

    job_name = "summarizer-" + job_id_suffix
    # discard the filename, as we are only interested in
    # the extension.
    _, extension = os.path.splitext(key)
    if not extension:
        # extension is a blank, which means
        # we received a key of "sample/foo"
        return {"statusCode": 400, "body": f"invalid file name {key}"}
    media_format = extension[1:] # strip the leading .
    job_uri = f"s3://{bucket}/{key}"

    try:
        # Adjust the settings as per your requirements
        response = transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={"MediaFileUri": job_uri},
            MediaFormat=media_format,
            IdentifyMultipleLanguages=True,
            Settings={
                "ShowSpeakerLabels": True,
                "MaxSpeakerLabels": 10,
                "ChannelIdentification": False,
                "ShowAlternatives": False,
            },
            OutputBucketName=os.environ["OUTPUT_BUCKET"],
            OutputKey=f"transcription/{job_name}.json"
        )

        logger.info(f"Response: {response}")
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            logger.error(f"Transcribe job creation failed: {job_name}")
            return {
                "statusCode": response["ResponseMetadata"]["HTTPStatusCode"],
                "body": job_name
            }

        logger.info(f"Transcribe job created: {job_name}")
        return {
            "statusCode": 200,
            "body": job_name
        }

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return {
            "statusCode": 500,
            "body": str(e)
        }

if __name__ == "__main__":

    # Sample event and env vars
    os.environ['OUTPUT_BUCKET'] = "my_bucket"
    event = {
        "Records": [{
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "awsRegion": "us-east-1",
            "eventTime": "2024-04-16T14: 10: 19.042Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {},
            "requestParameters": {},
            "responseElements": {},
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "...",
                "bucket": {
                    "name": "<bucket-name>",
                    "ownerIdentity": {
                        "principalId": "A1..."
                    },
                    "arn": "<bucket-arn>"
                },
                "object": {
                    "key": "source/<audio-file>",
                    "size": 42000,
                    "eTag": "...",
                    "sequencer": "..."
                }
            }
        }]
    }

    response = lambda_handler(event, None)
    print(response)
