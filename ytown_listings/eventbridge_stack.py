from aws_cdk import (
    aws_sns as sns,
    aws_events as events,
    aws_events_targets as targets,
    aws_sns_subscriptions as subscriptions,
    NestedStack,
)
from constructs import Construct


class EventbridgeStack(NestedStack):
    def __init__(self, scope: Construct) -> None:
        super().__init__(scope, "ytown-listings-eventbridge")

        glue_events_rule = events.Rule(
            self, "YtownListingsGlueEventsRule", rule_name="YtownListingsGlueEventsRule"
        )

        glue_events_rule.add_event_pattern(
            source=["aws.states"],
            detail_type=["Step Functions Execution Status Change"],
            detail={"status": ["FAILED", "TIMED_OUT", "ABORTED"]},
        )

        sns_failure_topic = sns.Topic(self, "YtownListingsSNSFailureTopic")

        sns_failure_topic.add_subscription(
            subscriptions.EmailSubscription("odeastephen1@gmail.com")
        )
        glue_events_rule.add_target(targets.SnsTopic(sns_failure_topic))
