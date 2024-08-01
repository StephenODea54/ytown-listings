from constructs import Construct
from aws_cdk import aws_glue as glue, aws_iam as iam, aws_s3 as s3, NestedStack
from ytown_listings.config import ACCOUNT_ID, REGION


class GlueStack(NestedStack):
    def __init__(
        self,
        scope: Construct,
        *,
        buckets: dict[str, s3.IBucket],
    ) -> None:
        super().__init__(scope, "ytown-listings-glue")

        raw_bucket = buckets.get("raw_bucket")

        raw_db = glue.CfnDatabase(
            self,
            "ytown_listings_raw_db",
            catalog_id=ACCOUNT_ID,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="ytown_listings_raw_db"
            ),
        )
