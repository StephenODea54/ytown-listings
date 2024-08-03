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


class DataClient:
    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df

    def change_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        _tmp_df = df.copy()

        _tmp_df["as_of_date"] = pd.to_datetime(
            df["as_of_date"], format="%Y-%m-%d"
        ).dt.date
        return _tmp_df

    def get_listing_date(self, df: pd.DataFrame) -> pd.DataFrame:
        _tmp_df = df.copy()

        _tmp_df["current_date"] = datetime.now()
        _tmp_df["time_delta"] = pd.to_timedelta(_tmp_df["days_on_zillow"], unit="d")
        _tmp_df["listing_date"] = _tmp_df["current_date"] - _tmp_df["time_delta"]
        _tmp_df["listing_date"] = _tmp_df["listing_date"].dt.date

        return _tmp_df.drop(columns=["current_date", "time_delta"])

    def duplicate_listings(self, df: pd.DataFrame) -> pd.DataFrame:
        min_date = df["listing_date"].min()
        days = pd.date_range(start=min_date, end=date.today())

        dfs: List[pd.DataFrame] = []

        for day in days:
            _tmp_df = df.copy()
            _tmp_df["as_of_date"] = day
            _tmp_df = _tmp_df[_tmp_df["listing_date"] <= _tmp_df["as_of_date"]]
            dfs.append(_tmp_df)

        return pd.concat(dfs)

    def build_listings_df(self) -> pd.DataFrame:
        return (
            self.df.pipe(self.change_dtypes)
            .pipe(self.get_listing_date)
            .pipe(self.duplicate_listings)
        )


def main():
    aws_client = AWSClient()

    # Check available partitions from the raw layer
    raw_partitions = aws_client.get_parquet_partitions("ytown_listings_raw_db")

    # Check available partitions from the staged layer
    staged_partitions = aws_client.get_parquet_partitions("ytown_listings_staged_db")

    # Get Unprocessed Partitions
    # If a partition has already been processed, we don't want to overwrite history
    unprocessed_raw_partitions = [
        partition for partition in raw_partitions if partition not in staged_partitions
    ]

    staged_df = aws_client.read_sql_query(
        sql=f"""
            SELECT DISTINCT
                isshowcaselisting AS is_show_case_listing,
                longitude,
                timeonzillow AS time_on_zillow,
                daysonzillow AS days_on_zillow,
                streetaddress AS street_address,
                taxassessedvalue AS tax_assessed_value,
                isunmappable AS is_unmappable,
                priceforhdp AS price_for_hdp,
                bathrooms,
                state,
                isfeatured AS is_featured,
                ispremierbuilder AS is_premier_builder,
                ispreforeclosureauction AS is_pre_foreclosure_auction,
                lotareavalue AS lot_area_value,
                isnonowneroccupied AS is_non_owner_occupied,
                homestatus AS home_status,
                latitude,
                zipcode AS zip_code,
                bedrooms,
                homestatusforhdp AS home_status_for_hdp,
                hometype AS home_type,
                iszillowowned AS is_zillow_owned,
                shouldhighlight AS should_highlight,
                price,
                zpid,
                openhouse AS open_house,
                city,
                country,
                currency,
                livingarea AS living_area,
                listing_sub_type_is_fsba,
                listing_sub_type_is_openhouse,
                isrentalwithbaseprice AS is_rental_with_base_price,
                listing_sub_type_is_forauction AS listing_sub_type_is_for_auction,
                pricechange AS price_change,
                datepricechanged AS date_price_changed,
                pricereduction AS price_reduction,
                listing_sub_type_is_bankowned,
                newconstructiontype AS new_construction_type,
                listing_sub_type_is_newhome,
                unit,
                zestimate,
                rentzestimate,
                DATE(as_of_date) AS as_of_date
            FROM listings
            WHERE as_of_date IN ({','.join([f"DATE('{partition}')" for partition in unprocessed_raw_partitions])})
        """,
        database="ytown_listings_raw_db",
    )

    # Duplicate individual listings from listing date to current
    # This simplifies any down stream time series analysis
    data_client = DataClient(staged_df)
    staged_df = data_client.build_listings_df()

    # Get Unprocessed Staged Partitions
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
            "ytown-listings-staged", "ytown_listings_staged_db", _tmp_df
        )


main()
