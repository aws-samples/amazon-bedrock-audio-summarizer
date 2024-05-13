import os

from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_lambda as lambda_,
    triggers,
    aws_iam as iam,
    aws_ec2 as ec2,
    aws_logs as logs,

    aws_events as events,
    aws_events_targets as targets,
    aws_s3_notifications as s3_notifications,
    
    Duration,
    RemovalPolicy,
)

import aws_cdk.aws_lambda_event_sources as eventsources

from constructs import Construct

class SummarizerStack(Stack):
    def __init__(self, scope, construct_id, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        self.bucket = self.create_bucket()
        
        # Shared policies
        self.s3_policy = iam.Policy(
            self,
            "S3Policy",
            policy_name="S3Policy",
            statements=[
                iam.PolicyStatement(
                    actions=["s3:*"],
                    resources=[self.bucket.bucket_arn, f"{self.bucket.bucket_arn}/*"]
                ),
            ]
        )

        self.transcribe_policy = iam.Policy(
            self,
            "TranscribePolicy",
            policy_name="TranscribePolicy",
            statements=[
                iam.PolicyStatement(
                    actions=["transcribe:*"],
                    resources=[f"arn:aws:transcribe:{self.region}:{self.account}:*"]
                ),
            ]
        )

        self.bedrock_runtime_policy = iam.Policy(
            self,
            "BedrockRuntimePolicy",
            policy_name="BedrockRuntimePolicy",
            statements=[               
                iam.PolicyStatement(
                    actions=["bedrock:InvokeModel"],
                    resources=[f"arn:aws:bedrock:{self.region}::foundation-model/*"]
                )
            ]
        )

        self.lambda_s3_trigger_function = self.create_lambda_s3_trigger_transcribe_function()
        self.lambda_eventbridge_bedrock_inference_function = self.create_eventbridge_bedrock_inference_function()

    # S3
    def create_bucket(self):

        bucket = s3.Bucket(
            self,
            "SummarizerBucket", 
            block_public_access=s3.BlockPublicAccess(
                block_public_acls=True,
                block_public_policy=True,
                ignore_public_acls=True,
                restrict_public_buckets=True
            ),
            removal_policy=RemovalPolicy.RETAIN
        )
    
        return bucket

    # Lambda functions
    def create_lambda_s3_trigger_transcribe_function(self):
        """
        This function will listen to the `source` subdir of the S3 bucket and trigger this Lambda 
        function when a file is uploaded. The function will then create a Transcribe job, using 
        the S3 object as the input.
        """
        lambda_function = lambda_.Function(
            self,
            "S3TriggerTranscribe",
            function_name="s3_trigger_transcribe",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.Code.from_asset(os.path.join(os.getcwd(), "lambda", "s3-trigger-transcribe")), 
            handler="lambda_function.lambda_handler",
            architecture=lambda_.Architecture.ARM_64,
            memory_size=128,
            timeout=Duration.seconds(15),
            environment={
                "OUTPUT_BUCKET": self.bucket.bucket_name
            },
            log_retention=logs.RetentionDays.ONE_WEEK,
        )

        lambda_function.apply_removal_policy(
            RemovalPolicy.DESTROY
        )

        # Attach the shared policies
        lambda_function.role.attach_inline_policy(self.s3_policy)
        lambda_function.role.attach_inline_policy(self.transcribe_policy)
       
        # Event source
        lambda_function.add_event_source(eventsources.S3EventSource(self.bucket,
            events=[s3.EventType.OBJECT_CREATED],
            filters=[s3.NotificationKeyFilter(prefix="source/")]
        ))

        return lambda_function

    def create_eventbridge_bedrock_inference_function(self):
        """
        This function creates an EventBridge rule that listens for COMPLETED or FAILED Transcribe jobs matching 
        the `summarizer-` prefix, then formats the transcript, creates a custom prompt, then calls Bedrock for 
        summarization.
        """
        lambda_function = lambda_.Function(
            self,
            "EventBridgeBedrockInference",
            function_name="eventbridge-bedrock-inference",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.Code.from_asset(os.path.join(os.getcwd(), "lambda", "eventbridge-bedrock-inference")), 
            handler="lambda_function.lambda_handler",
            architecture=lambda_.Architecture.ARM_64,
            memory_size=128,
            timeout=Duration.seconds(90), # Note: since this is a synchronous inference job, we extend the default 15s timeout
            environment={
                "OUTPUT_BUCKET": self.bucket.bucket_name
            },
            log_retention=logs.RetentionDays.ONE_WEEK,
        )

        lambda_function.apply_removal_policy(
            RemovalPolicy.DESTROY
        )

        # Attach shared policies
        lambda_function.role.attach_inline_policy(self.s3_policy)
        lambda_function.role.attach_inline_policy(self.transcribe_policy)
        lambda_function.role.attach_inline_policy(self.bedrock_runtime_policy)
       
        # Event bridge rule/trigger for the function
        triggers.Trigger(self, "EventBridgeTrigger",
            handler=lambda_function,
            invocation_type=triggers.InvocationType.EVENT,
        )

        rule = events.Rule(
            self, "TranscribeRule",
            event_pattern=events.EventPattern(
                source=["aws.transcribe"],
                detail_type=["Transcribe Job State Change"],
                detail={
                    "TranscriptionJobStatus": ["COMPLETED", "FAILED"],
                    "TranscriptionJobName": [{"prefix": "summarizer-"}]
                }
            )
        )

        rule.add_target(
            targets.LambdaFunction(lambda_function) 
        )

        lambda_function.add_permission(
            "AllowEventBridgeInvocation",
            principal=iam.ServicePrincipal("events.amazonaws.com"),
            source_arn=rule.rule_arn
        )

        return lambda_function
