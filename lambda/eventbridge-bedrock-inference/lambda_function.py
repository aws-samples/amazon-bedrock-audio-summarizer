import boto3
import json
import sys
import tempfile
import os
import logging

from botocore.exceptions import ClientError

transcribe = boto3.client("transcribe")
s3_client = boto3.client("s3")
bedrock_client = boto3.client("bedrock-runtime")

logger = logging.getLogger()
logger.setLevel("INFO")

def convert_to_txt_file(json_file):
    """
    Convert the JSON output of Amazon Transcribe to plaintext format, and write it to a file.

    Args:
        json_file (str): Path to the JSON file with the default Transcribe output.

    Returns:
        str: The converted transcript
        str: The path to the output text file or None
    """
    try:
        with open(json_file, "r") as f:
            data = json.load(f)
    except json.decoder.JSONDecodeError:
        
        logger.error("File is not a valid JSON file")
        return None

    # Save the converted output to a tempoary file
    temp_file = tempfile.NamedTemporaryFile(dir="/tmp", suffix=".txt", delete=False)
    output_path = temp_file.name

    current_speaker = None
    current_text = ""
    output = []

    with open(output_path, "w", encoding="utf-8") as output_file:
        for item in data["results"].get("items", []):
            if item["type"] == "pronunciation":
                content = item["alternatives"][0]["content"]
                speaker_label = item["speaker_label"]

                if speaker_label != current_speaker:
                    if current_text:
                        output_file.write(f"{current_speaker}: {current_text.strip()}\n")
                        output.append(f"{current_speaker}: {current_text.strip()}\n")
                    current_speaker = speaker_label
                    current_text = content
                else:
                    current_text += " " + content
            elif item["type"] == "punctuation":
                current_text += item["alternatives"][0]["content"]

        if current_text:
            output_file.write(f"{speaker_label}: {current_text.strip()}\n")
            output.append(f"{speaker_label}: {current_text.strip()}\n")

    return "".join(output), output_path

def download_file(url):
    """Downloads a text file from the url and saves it to a temporary file.

    Args:
        url: The url of the text file to download.

    Returns:
        The path to the downloaded temporary file, or None if download fails.
    """
    try:
        response = requests.get(url)

        with tempfile.NamedTemporaryFile(dir="/tmp", suffix="") as temp_file:
            temp_file.write(response.content)
            logger.info(f"Downloaded URL to {temp_file.name}")
            return temp_file.name

    except requests.exceptions.RequestException as e:
        
        logger.error(f"Download failed: {e}")
        return None

def lambda_handler(event, context):
    """
    Ingests the URI from the transcription job and converts the output 
    from Transcribe's default format to our custom format. Writes the
    converted transcription to S3, and use Bedrock to invoke a model for
    summarization.
    """
    BUCKET = os.environ["OUTPUT_BUCKET"]

    if event and "detail" in event:

        logger.info("# EVENT")
        logger.info(event)

        transcription_job_name = event["detail"]["TranscriptionJobName"]

        job = transcribe.get_transcription_job(
            TranscriptionJobName=transcription_job_name
        )

        if event["detail"]["TranscriptionJobStatus"] in ["COMPLETED"]:

            response = transcribe.get_transcription_job(
                TranscriptionJobName=transcription_job_name
            )

            # Get the file from S3
            try:
                s3_client.download_file(BUCKET, "transcription/" + transcription_job_name + ".json", "/tmp/" + transcription_job_name + ".json")
                logger.info(f"Downloaded {transcription_job_name}.json from {BUCKET}")
            except ClientError as e:
                
                logger.error(f"Error downloading s3://{BUCKET}/transcription/{transcription_job_name}.json: {e}")
                return {
                    "statusCode": 400,
                    "body": f"Error downloading s3://{BUCKET}/transcription/{transcription_job_name}.json: {e}"
                }

            # Convert to txt file
            transcript_content, transcript_output_file = convert_to_txt_file("/tmp/" + transcription_job_name + ".json")
            if not transcript_output_file or not os.path.exists(transcript_output_file):
                
                logger.error("Error converting transcription to txt file")
                return {
                    "statusCode": 400,
                    "body": "Error converting transcription to txt file"
                }

            logger.info(f"Converted transcription to {transcript_output_file}")

            # Save the transcription to S3 so it can be referenced or reviewed at a later time
            try:
                transcription_job_name_txt = transcription_job_name + ".txt"
                s3_client.upload_file(transcript_output_file, BUCKET, "transcription/" + transcription_job_name_txt)
                logger.info(f"Uploaded to s3://{BUCKET}/transcription/{transcription_job_name_txt}")
            except Exception as e:
                
                logger.error(f"Error uploading converted file to s3://{BUCKET}/transcription/{transcription_job_name_txt}: {e}")
                return {
                    "statusCode": 400,
                    "body": f"Error uploading converted file to s3://{BUCKET}/transcription/{transcription_job_name_txt}: {e}"
                }

            # Prepare a prompt for summarization. The prompt (and `system` prompt below) 
            # are hardcoded here. Ideally, these would be dynamic or configurable by your user.
            prompt = f"""Summarize the following transcript into one or more clear and 
            readable paragraphs. Speakers in the transcript could be denoted by their name,
            or by "spk_x", where `x` is a number. These represent distinct speakers in the 
            conversation. When you refer to a speaker, you may refer to them by "Speaker 1"
            in the case of "spk_1", "Speaker 2" in the case of "spk_2", and so forth. 
            When you summarize, capture any ideas discussed, any hot topics you identify, 
            or any other interesting parts of the conversation between the speakers. 
            At the end of your summary, give a bullet point list of the key action 
            items, to-do's, and followup activities:
            
            {transcript_content}
            """

            # Claude requires the "Anthropic Claude Messages API" format,
            # https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-anthropic-claude-messages.html
            messages = [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": prompt,
                        }
                    ]
                }
            ]

            # Setting default parameters for the model, adjust as needed:
            body = json.dumps(
                {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 2000,
                    "system": "You are an AI assistant that excels at summarizing conversations.",
                    "messages": messages,
                    "temperature": 1.0,
                    "top_p": 0.999,
                    "top_k": 40
                }
            )

            # By default this uses Anthropic Claude 3 Sonnet, but you can change to other
            # models available to you.
            try:
                logger.info("## INVOKING MODEL")
                response = bedrock_client.invoke_model(
                    modelId="anthropic.claude-3-sonnet-20240229-v1:0", body=body
                )
            except ClientError as e:
                logger.error(e)
                return {
                    "statusCode": 400,
                    "body": str(e)
                }

            if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                if "body" in response:
                    return {
                        "statusCode": response["ResponseMetadata"]["HTTPStatusCode"],
                        "body": str(response["body"])
                    }
                else:
                    logger.error(response)
                    return {
                        "statusCode": response["ResponseMetadata"]["HTTPStatusCode"],
                        "body": "An unknown error occured."
                    }
            else:
                response_body = json.loads(response["body"].read())

                # Debug a preview of the Bedrock output
                logger.info("## BEDROCK OUTPUT PREVIEW")
                logger.info(json.dumps(response_body["content"][0]["text"][0:100] + "..."))
                summary = response_body["content"][0]["text"]

                # Write the summary to an S3 file
                processed_key = f"processed/{transcription_job_name_txt}"
                s3_response = s3_client.put_object(
                    Body=summary,
                    Bucket=BUCKET,
                    Key=processed_key
                )

                if s3_response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                    logger.error(s3_response)
                    return {
                        "statusCode": s3_response["ResponseMetadata"]["HTTPStatusCode"],
                        "body": str(s3_response)
                    }
                else:
                    logger.info(f"Summary written to s3://{BUCKET}/{processed_key}")
                    return {
                        "statusCode": 200,
                        "body": processed_key
                    }

            return {
                "statusCode": 200,
                "body": f"s3://{BUCKET}/transcription/{transcription_job_name_txt}"
            }

        elif event["detail"]["TranscriptionJobStatus"] in ["FAILED"]:
            logger.error(f"Unable to process, job {transcription_job_name} failed.")
            return {
                "statusCode": 400,
                "body": f"Unable to process, job {transcription_job_name} failed."
            }

        else:
            logger.error(f"Transcription job {transcription_job_name} is not completed or failed.")
            return {
                "statusCode": 500,
                "body": f"Transcription job {transcription_job_name} is not completed or failed."
            }

    else:
        
        logger.error("Invalid event received.")
        return {
            "statusCode": 500,
            "body": "Invalid event received."
        }

if __name__ == "__main__":

    # Sample event and env vars
    os.environ['OUTPUT_BUCKET'] = "<my-bucket>"
    event = {
        "version": "0",
        "id": "ee50...",
        "detail-type": "Transcribe Job State Change",
        "source": "aws.transcribe",
        "account": "...",
        "time": "2024-04-27T14:09:59Z",
        "region": "us-east-1",
        "resources": [],
        "detail": {
            "TranscriptionJobName": "summarizer-lmrca78v3tug",
            "TranscriptionJobStatus": "COMPLETED"
        }
    } 

    response = lambda_handler(event, None)
    print(response)