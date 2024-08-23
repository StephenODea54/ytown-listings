import awswrangler as wr
import boto3
import os
import pandas as pd
from awswrangler import _utils
from typing import List, Literal, Union


DATABASE = Union[Literal["raw"], Literal["staged"], Literal["curated"]]


REGION = os.environ.get("REGION")


class AWSClient:
    def __init__(self) -> None:
        self.session = boto3.Session(region_name=REGION)
        self.account_id = wr.sts.get_account_id(boto3_session=self.session)
        self.clients = {
            "glue": _utils.client(service_name="glue", session=self.session)
        }

    def get_partitions(self, database: DATABASE, table: str) -> List[str]:
        hi = _utils.client(service_name="glue")
        try:
            partitions_dict = wr.catalog.get_parquet_partitions(
                database=f"ytown_listings_{database}_db",
                table=table,
                boto3_session=self.session,
            )

            partition_values: List[str] = []

            for partition_value in partitions_dict.values():
                partition_values += partition_value

            return partition_values
        # https://github.com/aws/aws-sdk-pandas/blob/064d3757c65e0562c4835dbcc60b47b23a483497/awswrangler/catalog/_utils.py#L75C5-L80C21
        except self.clients.get("glue").client.exceptions.EntityNotFound:
            return []

    def get_secret(self, secret_name: str) -> Union[str, bytes]:
        return wr.secretsmanager.get_secret(secret_name, self.session)

    def read_query(self, sql: str, database: DATABASE) -> pd.DataFrame:
        return wr.athena.read_sql_query(
            sql=sql,
            database=f"ytown_listings_{database}_db",
            workgroup="ytown_listings_workgroup",
            boto3_session=self.session,
        )

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        database: DATABASE,
        table: str,
    ) -> wr.typing._S3WriteDataReturnValue:
        return wr.s3.to_parquet(
            df=df,
            path=f"s3://{self.account_id}-ytown-listings-{database}/{table}",
            index=False,
            compression="snappy",
            dataset=True,
            mode="overwrite_partitions",
            schema_evolution=True,
            database=f"ytown_listings_{database}_db",
            table=table,
            partition_cols=["as_of_date"],
            boto3_session=self.session,
        )
