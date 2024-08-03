import awswrangler as wr
import boto3
import os
import pandas as pd
from datetime import date, datetime
from typing import List


REGION = os.environ.get("REGION")
TABLE = "listings"


class AWSClient:
    def __init__(self) -> None:
        self.session = boto3.Session(region_name=REGION)
        self.account_id = wr.sts.get_account_id(boto3_session=self.session)

    def get_parquet_partitions(self, database: str) -> List[str]:
        try:
            partitions_dict = wr.catalog.get_parquet_partitions(
                database=database, table=TABLE, boto3_session=self.session
            )

            partition_values: List[str] = []

            for partition_value in partitions_dict.values():
                partition_values += partition_value

            return partition_values
        except:
            return []

    def read_sql_query(self, sql: str, database: str) -> pd.DataFrame:
        return wr.athena.read_sql_query(
            sql=sql,
            database=database,
            workgroup="ytown_listings_workgroup",
            boto3_session=self.session,
        )

    def to_parquet(
        self, bucket: str, database: str, df: pd.DataFrame
    ) -> wr.typing._S3WriteDataReturnValue:
        return wr.s3.to_parquet(
            df=df,
            path=f"s3://{self.account_id}-{bucket}/{TABLE}",
            index=False,
            compression="snappy",
            dataset=True,
            mode="overwrite_partitions",
            schema_evolution=True,
            database=database,
            table=TABLE,
            partition_cols=["as_of_date"],
            boto3_session=self.session,
        )


def main():
    aws_client = AWSClient()

    # Check available partitions from the raw layer
    staged_partitions = aws_client.get_parquet_partitions("ytown_listings_staged_db")

    # Check available partitions from the staged layer
    curated_partitions = aws_client.get_parquet_partitions("ytown_listings_curated_db")

    # Get Unprocessed Partitions
    # If a partition has already been processed, we don't want to overwrite history
    # TODO: Fix weird as_of_date truncation in staged layer
    unprocessed_staged_partitions = [
        partition[:11]
        for partition in staged_partitions
        if partition not in curated_partitions
    ]

    staged_df = aws_client.read_sql_query(
        sql=f"""
            SELECT
                as_of_date,
                city,
                zip_code,
                COUNT(*) AS total_listings,
                APPROX_PERCENTILE(price, 0.5) AS median_listing_price,
                APPROX_PERCENTILE(DATE_DIFF('day', listing_date, as_of_date), 0.5) AS median_days_on_market,
                APPROX_PERCENTILE(price / living_area, 0.5) AS median_price_per_square_ft,
                APPROX_PERCENTILE(lot_area_value / 43560, 0.5) AS avg_lot_size
            FROM listings
            WHERE as_of_date IN ({','.join([f"DATE('{partition}')" for partition in unprocessed_staged_partitions])})
            GROUP BY
                as_of_date,
                city,
                zip_code
        """,
        database="ytown_listings_staged_db",
    )

    # Get Unprocessed Curated Partitions
    # Similar to above, we don't overwrite any partitions that
    # have already been processed
    all_dates = list(set([str(x) for x in staged_df["as_of_date"].dt.date]))
    unprocessed_staged_partitions = [
        partition for partition in all_dates if partition not in staged_partitions
    ]

    for partition in unprocessed_staged_partitions:
        print(partition)
        _tmp_df = staged_df[staged_df["as_of_date"] == partition]
        aws_client.to_parquet(
            "ytown-listings-curated", "ytown_listings_curated_db", _tmp_df
        )


main()
