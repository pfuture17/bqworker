import os

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "off-net-dev")
DATASET = os.environ.get("BQ_DATASET", "test")
EVENTS_RAW = os.environ.get("BQ_EVENTS_RAW_TABLE", "events_raw")

TABLE_ID = f'{PROJECT}.{DATASET}.{EVENTS_RAW}'

KEYS_NOT_NEEDED_IN_RESULTS = {"packages", "distro", "distroRelease",
                              "complianceScanPassed", "complianceDistribution", "vulnerabilityDistribution", "vulnerabilityScanPassed", "history", "applications", "collections", "digest"}
KEYS_NOT_NEEDED_IN_COMPLIANCES = {"layerTime", "category"}
KEYS_NOT_NEEDED_IN_VULNERABILITIES = {"vector", "riskFactors",
                                      "impactedVersions", "publishedDate", "discoveredDate", "layerTime", "fixDate"}
CLOUD_EVENT_ATTRIBUTES = [
    "source", "event_type", "id", "metadata", "time_created", "signature",
    "msg_id",
]