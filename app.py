#!/usr/bin/env python3

import aws_cdk as cdk

from ytown_listings.ytown_listings_stack import YtownListingsStack


app = cdk.App()
YtownListingsStack(app, "YtownListingsStack")

app.synth()
