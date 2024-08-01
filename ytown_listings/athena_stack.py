from aws_cdk import (
    aws_athena as athena,
    aws_s3 as s3,
    NestedStack,
)
from constructs import Construct


class AthenaStack(NestedStack):
    def __init__(self, scope: Construct, *, athena_bucket: s3.IBucket) -> None:
        super().__init__(scope, "ytown-listings-athena")

        self.workgroup = athena.CfnWorkGroup(
            self,
            id="YtownListingsWorkGroup",
            name="ytown_listings_workgroup",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{athena_bucket.bucket_name}/listings/query_results",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3"
                    ),
                )
            ),
        )
