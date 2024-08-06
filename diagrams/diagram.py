from diagrams import Cluster, Diagram
from diagrams.aws.analytics import Athena, Glue, GlueDataCatalog
from diagrams.aws.general import Users
from diagrams.aws.integration import Eventbridge, SNS
from diagrams.aws.security import SecretsManager
from diagrams.aws.storage import S3
from diagrams.onprem.analytics import Metabase
from diagrams.custom import Custom


with Diagram(""):

    with Cluster("Vendor APIs"):
        rapid_api = Custom("", "./icons/rapid_api_icon.png")

    with Cluster("AWS"):
        secrets = SecretsManager("Rapid API Key")

        raw_bucket = S3("Raw Bucket")
        staged_bucket = S3("Staged Bucket")
        curated_bucket = S3("Curated Bucket")

        raw_glue_workflow = Glue("Raw Glue Job")
        staged_glue_workflow = Glue("Staged Glue Job")
        curated_glue_workflow = Glue("Curated Glue Job")

        glue_data_catalog = GlueDataCatalog("Glue Data Catalog")

        athena = Athena("Athena")

        event_bridge = Eventbridge("Event Bridge")
        sns = SNS("SNS Glue Failure Notifications")

    with Cluster("Coolify"):
        metabase = Metabase("Metabase")

    users = Users("Users")

    rapid_api >> raw_glue_workflow
    (
        raw_glue_workflow
        >> raw_bucket
        >> staged_glue_workflow
        >> staged_bucket
        >> curated_glue_workflow
        >> curated_bucket
    )

    secrets >> raw_glue_workflow

    raw_glue_workflow >> glue_data_catalog
    staged_glue_workflow >> glue_data_catalog
    curated_glue_workflow >> glue_data_catalog
    glue_data_catalog >> athena

    metabase >> athena

    raw_glue_workflow >> event_bridge
    staged_glue_workflow >> event_bridge
    curated_glue_workflow >> event_bridge

    event_bridge >> sns

    users >> metabase
