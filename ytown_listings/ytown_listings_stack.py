from constructs import Construct
from aws_cdk import (
    Stack,
)
from ytown_listings.athena_stack import AthenaStack
from ytown_listings.eventbridge_stack import EventbridgeStack
from ytown_listings.glue_stack import GlueStack
from ytown_listings.s3_stack import S3Stack
from ytown_listings.secrets_stack import SecretsStack


class YtownListingsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        s3_stack = S3Stack(self)

        athena_stack = AthenaStack(self, athena_bucket=s3_stack.athena_bucket)

        eventbridge_stack = EventbridgeStack(self)

        glue_stack = GlueStack(
            self,
            buckets={
                "raw_bucket": s3_stack.raw_bucket,
                "staged_bucket": s3_stack.staged_bucket,
                "curated_bucket": s3_stack.curated_bucket,
                "scripts_bucket": s3_stack.scripts_bucket,
                "athena_bucket": s3_stack.athena_bucket,
            },
            workgroup=athena_stack.workgroup,
        )

        secrets_stack = SecretsStack(self)
