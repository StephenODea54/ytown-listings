import pandas as pd
import requests
import time
from datetime import date
from typing import List, Optional, TypedDict
from utils.aws_client import AWSClient


class ListingSubType(TypedDict):
    is_FSBA: bool


class OpenHouseInfo(TypedDict):
    open_house_start: int
    open_house_end: int


class OpenHouseInfo(TypedDict):
    open_house_showing: List[OpenHouseInfo]


class Listing(TypedDict):
    bathrooms: int
    state: str
    isFeatured: bool
    isPremierBuilder: bool
    lotAreaUnit: str
    isPreforeclosureAuction: bool
    longitude: float
    isNonOwnerOccupied: bool
    lotAreaValue: int
    taxAssessedValue: int
    country: str
    livingArea: int
    homeStatus: str
    daysOnZillow: int
    latitude: float
    isUnmappable: bool
    bedrooms: int
    streetAddress: str
    homeStatusForHDP: str
    isZillowOwned: bool
    shouldHighlight: bool
    zpid: int
    listing_sub_type: ListingSubType
    rentZestimate: int
    zestimate: int
    city: str
    price: int
    homeType: str
    currency: str
    zipcode: str
    priceForHDP: int
    open_house_info: OpenHouseInfo
    isRentalWithBasePrice: bool
    openHouse: str
    priceChange: int
    datePriceChanged: int
    priceReduction: str


class ListingsResponse:
    props: List[Listing]
    resultsPerPage: int
    totalPages: int
    totalResultCount: int
    currentPage: int


URL = "https://www.zillow.com/mahoning-county-oh/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22mapBounds%22%3A%7B%22north%22%3A41.86092787686419%2C%22south%22%3A40.216172776295814%2C%22east%22%3A-79.45971688671875%2C%22west%22%3A-82.14587411328125%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A2399%2C%22regionType%22%3A4%7D%2C%7B%22regionId%22%3A2905%2C%22regionType%22%3A4%7D%2C%7B%22regionId%22%3A2583%2C%22regionType%22%3A4%7D%5D%2C%22isMapVisible%22%3Atrue%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22ah%22%3A%7B%22value%22%3Atrue%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A9%7D"


def get_listing_results(
    api_key: str, listing_url: str, page_num: Optional[int] = 1
) -> ListingsResponse:
    url = "https://zillow-com1.p.rapidapi.com/searchByUrl"
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "zillow-com1.p.rapidapi.com",
    }
    params = {"url": listing_url, "page": page_num}

    response = requests.get(url=url, headers=headers, params=params)
    return response.json()


def main():
    aws_client = AWSClient()

    api_key = aws_client.get_secret("RapidAPIKey")

    # All results will be concatenated into a single list
    # Something like a map state in AWS Step Functions would be nice here but that would
    # violate my one request per second rate limit on my free tier for the API
    listing_dfs: List[pd.DataFrame] = []

    # Initial Loop
    listing_results = get_listing_results(api_key=api_key, listing_url=URL)
    listing_dfs.append(pd.json_normalize(listing_results["props"]))

    total_pages = listing_results["totalPages"]

    # Subsequent Loops Assuming Pagination is Needed
    for page in range(2, total_pages):
        listing_results = get_listing_results(
            api_key=api_key, listing_url=URL, page_num=page
        )
        listing_dfs.append(pd.json_normalize(listing_results["props"]))

        time.sleep(45)

    listing_df = pd.concat(listing_dfs)

    # Yeah, yeah. Raw layer means no transformations. But you know what?
    # I want a partition column that uses a date. Will anyone care? Will
    # anyone actually read my code? Or will I just keep yeeting side
    # projects into the abyss? Is that how you spell abyss!?!?!?!?
    listing_df["as_of_date"] = date.today()

    aws_client.upload_dataframe(df=listing_df, database="raw", table="listings")


main()
