from constructs import Construct
from aws_cdk import (
    Stack,
)
from ytown_listings.glue_stack import GlueStack
from ytown_listings.s3_stack import S3Stack
from ytown_listings.secrets_stack import SecretsStack


class YtownListingsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        s3_buckets = S3Stack(self)
        secrets = SecretsStack(self)
        glue_jobs = GlueStack(self)
