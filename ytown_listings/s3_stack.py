from aws_cdk import (
    aws_s3 as s3,
    NestedStack,
    RemovalPolicy,
)
from constructs import Construct
from ytown_listings.config import ACCOUNT_ID


class S3Stack(NestedStack):
    def __init__(self, scope: Construct) -> None:
        super().__init__(scope, "ytown-listings-s3")

        def create_bucket_configuration(bucket_name: str) -> s3.Bucket:
            """
            Returns standardized bucket config b/c I'm too lazy to keep
            rewriting it.
            """

            return s3.Bucket(
                self,
                id=f"{ACCOUNT_ID}-{bucket_name}",
                bucket_name=f"{ACCOUNT_ID}-{bucket_name}",
                block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                encryption=s3.BucketEncryption.S3_MANAGED,
                enforce_ssl=True,
                minimum_tls_version=1.2,
                public_read_access=False,
                removal_policy=RemovalPolicy.RETAIN,
            )

        self.raw_bucket = create_bucket_configuration("ytown-listings-raw")
        self.staged_bucket = create_bucket_configuration("ytown-listings-staged")
        self.curated_bucket = create_bucket_configuration("ytown-listings-curated")
        self.scripts_bucket = create_bucket_configuration("ytown-listings-scripts")
        self.athena_bucket = create_bucket_configuration("ytown-listings-athena")
