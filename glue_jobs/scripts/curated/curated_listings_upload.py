from utils.aws_client import AWSClient


def main():
    aws_client = AWSClient()

    staged_partitions = aws_client.get_partitions(database="staged", table="listings")
    curated_partitions = aws_client.get_partitions(database="staged", table="listings")

    # Get Unprocessed Partitions
    # If a partition has already been processed, we don't want to overwrite history
    # TODO: Fix weird as_of_date truncation in staged layer
    unprocessed_staged_partitions = [
        partition[:11]
        for partition in staged_partitions
        if partition not in curated_partitions
    ]

    staged_df = aws_client.read_query(
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
        database="staged",
    )

    # Get Unprocessed Curated Partitions
    # Similar to above, we don't overwrite any partitions that
    # have already been processed
    all_dates = list(set([str(x) for x in staged_df["as_of_date"].dt.date]))
    unprocessed_staged_partitions = [
        partition for partition in all_dates if partition not in staged_partitions
    ]

    for partition in unprocessed_staged_partitions:
        _tmp_df = staged_df[staged_df["as_of_date"] == partition]
        aws_client.upload_dataframe(
            df=_tmp_df,
            database="curated",
            table="listings",
        )


main()
