"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import os

from singer_sdk.testing import get_standard_tap_tests
from singer_sdk.helpers._util import read_json_file

from tap_impact_publisher.tap import TapImpactPublisher

SAMPLE_CONFIG = {
    "auth_token": os.environ.get("IMPACT_AUTH_TOKEN"),
    "account_sid": os.environ.get("IMPACT_ACCOUNT_SID"),
    "start_date": "2022-04-01T00:00:00Z"
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(
        TapImpactPublisher,
        config=SAMPLE_CONFIG
    )
    for test in tests:
        test()


# TODO: Create additional tests as appropriate for your tap.
