from aws_cdk import aws_secretsmanager as sm, NestedStack, RemovalPolicy
from constructs import Construct


class SecretsStack(NestedStack):
    def __init__(self, scope: Construct) -> None:
        super().__init__(scope, "ytown-listings-secrets")

        self.scrapeak_api_key = sm.Secret(
            self,
            id="RapidAPIKey",
            secret_name="RapidAPIKey",
            removal_policy=RemovalPolicy.RETAIN,
        )
