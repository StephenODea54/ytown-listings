from constructs import Construct
from aws_cdk import (
    aws_athena as athena,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3_deployment,
    aws_secretsmanager as sm,
    NestedStack,
)
from ytown_listings.config import ACCOUNT_ID, REGION


class GlueStack(NestedStack):
    def __init__(
        self,
        scope: Construct,
        rapid_api_key: sm.Secret,
        buckets: dict[str, s3.Bucket],
        workgroup: athena.CfnWorkGroup,
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
        curated_db = create_glue_database("curated")

        glue_job_policy = iam.Policy(
            self,
            id="YtownListingsGlueJobPolicy",
            policy_name="YtownListingsGlueJobPolicy",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "secretsmanager:GetSecretValue",
                    ],
                    resources=[
                        rapid_api_key.secret_arn,
                    ],
                ),
                iam.PolicyStatement(
                    actions=[
                        "s3:ListBucket",
                    ],
                    resources=[
                        buckets.get("raw_bucket").bucket_arn,
                        buckets.get("scripts_bucket").bucket_arn,
                        buckets.get("staged_bucket").bucket_arn,
                        buckets.get("curated_bucket").bucket_arn,
                        buckets.get("athena_bucket").bucket_arn,
                    ],
                ),
                iam.PolicyStatement(
                    actions=[
                        "s3:GetObject",
                        "s3:GetObjectAcl",
                        "s3:GetBucketLocation",
                        "s3:ListBucketMultipartUploads",
                        "s3:ListMultipartUploadParts",
                    ],
                    resources=[
                        f"{buckets.get('raw_bucket').bucket_arn}/listings",
                        f"{buckets.get('raw_bucket').bucket_arn}/listings/*",
                        f"{buckets.get('staged_bucket').bucket_arn}",
                        f"{buckets.get('staged_bucket').bucket_arn}/*",
                        f"{buckets.get('curated_bucket').bucket_arn}/listings",
                        f"{buckets.get('curated_bucket').bucket_arn}/listings/*",
                        f"{buckets.get('scripts_bucket').bucket_arn}/listings",
                        f"{buckets.get('scripts_bucket').bucket_arn}/listings/*",
                        f"{buckets.get('scripts_bucket').bucket_arn}/listings",
                        f"{buckets.get('scripts_bucket').bucket_arn}/listings/*",
                        f"{buckets.get('athena_bucket').bucket_arn}",
                        f"{buckets.get('athena_bucket').bucket_arn}/*",
                    ],
                ),
                iam.PolicyStatement(
                    actions=[
                        "s3:PutObject",
                        "s3:PutObjectAcl",
                        "s3:DeleteObject",
                        "s3:AbortMultipartUpload",
                    ],
                    resources=[
                        f"{buckets.get('raw_bucket').bucket_arn}/listings",
                        f"{buckets.get('raw_bucket').bucket_arn}/listings/*",
                        f"{buckets.get('staged_bucket').bucket_arn}/listings",
                        f"{buckets.get('staged_bucket').bucket_arn}/listings/*",
                        f"{buckets.get('curated_bucket').bucket_arn}/listings",
                        f"{buckets.get('curated_bucket').bucket_arn}/listings/*",
                        f"{buckets.get('athena_bucket').bucket_arn}/*",
                    ],
                ),
                iam.PolicyStatement(
                    actions=[
                        "athena:Get*",
                        "athena:Start*",
                        "glue:Get*",
                        "glue:List*",
                        "glue:*Partition*",
                        "glue:UpdateTable",
                        "glue:CreateTable",
                        "glue:DeleteTable",
                    ],
                    resources=[
                        f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:catalog",
                        f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:database/{raw_db.database_input.name}",
                        f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:table/{raw_db.database_input.name}/*",
                        f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:database/{staged_db.database_input.name}",
                        f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:table/{staged_db.database_input.name}/*",
                        f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:database/{curated_db.database_input.name}",
                        f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:table/{curated_db.database_input.name}/*",
                        f"arn:aws:athena:{REGION}:{ACCOUNT_ID}:workgroup/{workgroup.name}",
                    ],
                ),
            ],
        )

        glue_job_role = iam.Role(
            self,
            id="YtownListingsGlueJobRole",
            role_name="YtownListingsGlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        )
        glue_job_policy.attach_to_role(glue_job_role)

        raw_listings_upload_script = s3_deployment.BucketDeployment(
            self,
            "YtownRawListingsUploadScript",
            sources=[s3_deployment.Source.asset("./glue_jobs/scripts/raw")],
            destination_bucket=buckets.get("scripts_bucket"),
            destination_key_prefix="listings/raw",
        )

        staged_listings_upload_script = s3_deployment.BucketDeployment(
            self,
            "YtownListingsStagedUploadScript",
            sources=[s3_deployment.Source.asset("./glue_jobs/scripts/staged")],
            destination_bucket=buckets.get("scripts_bucket"),
            destination_key_prefix="listings/staged",
        )

        curated_listings_upload_script = s3_deployment.BucketDeployment(
            self,
            "YtownListingsCuratedUploadScript",
            sources=[s3_deployment.Source.asset("./glue_jobs/scripts/curated")],
            destination_bucket=buckets.get("scripts_bucket"),
            destination_key_prefix="listings/curated",
        )

        utility_scripts = s3_deployment.BucketDeployment(
            self,
            "YtownListingsUtilities",
            sources=[s3_deployment.Source.asset("./glue_jobs/dist")],
            destination_bucket=buckets.get("scripts_bucket"),
            destination_key_prefix="listings/utils",
        )

        glue_workflow = glue.CfnWorkflow(
            self,
            "YtownListingsETLWorkflow",
            name="YtownListingsETLWorkflow",
            max_concurrent_runs=3,
        )

        raw_listings_upload_job = glue.CfnJob(
            self,
            id="YtownListingsRawJob",
            name="YtownListingsRawJob",
            role=glue_job_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="pythonshell",
                python_version="3.9",
                script_location=f"s3://{buckets.get('scripts_bucket').bucket_name}/listings/raw/raw_listings_upload.py",
            ),
            default_arguments={
                "library-set": "analytics",
                "--enable-job-insights": "true",
                "--job-language": "python",
                "--extra-py-files": f"s3://{buckets.get('scripts_bucket').bucket_name}/listings/utils/utils-0.1-py3-none-any.whl",
            },
            glue_version="4.0",
        )

        staged_listings_upload_job = glue.CfnJob(
            self,
            id="YtownListingsStagedJob",
            name="YtownListingsStagedJob",
            role=glue_job_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="pythonshell",
                python_version="3.9",
                script_location=f"s3://{buckets.get('scripts_bucket').bucket_name}/listings/staged/staged_listings_upload.py",
            ),
            default_arguments={
                "library-set": "analytics",
                "--enable-job-insights": "true",
                "--job-language": "python",
                "--extra-py-files": f"s3://{buckets.get('scripts_bucket').bucket_name}/listings/utils/utils-0.1-py3-none-any.whl",
            },
            glue_version="4.0",
        )

        curated_listings_upload_job = glue.CfnJob(
            self,
            id="YtownListingsCuratedJob",
            name="YtownListingsCuratedJob",
            role=glue_job_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="pythonshell",
                python_version="3.9",
                script_location=f"s3://{buckets.get('scripts_bucket').bucket_name}/listings/curated/curated_listings_upload.py",
            ),
            default_arguments={
                "library-set": "analytics",
                "--enable-job-insights": "true",
                "--job-language": "python",
                "--extra-py-files": f"s3://{buckets.get('scripts_bucket').bucket_name}/listings/utils/utils-0.1-py3-none-any.whl",
            },
            glue_version="4.0",
        )

        raw_listings_upload_trigger = glue.CfnTrigger(
            self,
            id="YtownListingsRawListingsUploadTrigger",
            name="YtownListingsRawListingsUploadTrigger",
            type="SCHEDULED",
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=raw_listings_upload_job.name,
                ),
            ],
            schedule="cron(0 9 ? * MON *)",
            start_on_creation=True,
            workflow_name=glue_workflow.name,
        )

        staged_listings_upload_trigger = glue.CfnTrigger(
            self,
            id="YtownListingsStagedListingsUploadTrigger",
            name="YtownListingsStagedListingsUploadTrigger",
            type="CONDITIONAL",
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=staged_listings_upload_job.name,
                )
            ],
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        job_name=raw_listings_upload_job.name,
                        state="SUCCEEDED",
                        logical_operator="EQUALS",
                    )
                ]
            ),
            start_on_creation=True,
            workflow_name=glue_workflow.name,
        )

        curated_listings_upload_trigger = glue.CfnTrigger(
            self,
            id="YtownListingsCuratedListingsUploadTrigger",
            name="YtownListingsCuratedListingsUploadTrigger",
            type="CONDITIONAL",
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=curated_listings_upload_job.name,
                )
            ],
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        job_name=staged_listings_upload_job.name,
                        state="SUCCEEDED",
                        logical_operator="EQUALS",
                    )
                ]
            ),
            start_on_creation=True,
            workflow_name=glue_workflow.name,
        )
