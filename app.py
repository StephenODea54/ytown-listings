#!/usr/bin/env python3
from aws_cdk import App, Tags
from ytown_listings.config import ACCOUNT_ID, REGION
from ytown_listings.ytown_listings_stack import YtownListingsStack


app = App()
env = {
    "account": ACCOUNT_ID,
    "region": REGION,
}

tags = {
    "application": "ytown_listings",
}

ytown_listings_infra_stack = YtownListingsStack(app, "ytown-listings-stack", env=env)

for tag, value in tags.items():
    Tags.of(app).add(tag, value)
app.synth()
