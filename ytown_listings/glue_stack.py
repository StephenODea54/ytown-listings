from constructs import Construct
from aws_cdk import aws_glue as glue, aws_iam as iam, aws_s3 as s3, NestedStack
from ytown_listings.config import ACCOUNT_ID, REGION


class GlueStack(NestedStack):
    def __init__(
        self,
        scope: Construct,
    ) -> None:
        super().__init__(scope, "ytown-listings-glue")

        def create_glue_database(layer: str) -> glue.CfnDatabase:
            return glue.CfnDatabase(
                self,
                f"ytown_listings_{layer}_db",
                catalog_id=ACCOUNT_ID,
                database_input=glue.CfnDatabase.DatabaseInputProperty(
                    name=f"ytown_listings_{layer}_db"
                ),
            )

        raw_db = create_glue_database("raw")
        staged_db = create_glue_database("staged")
