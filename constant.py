import os

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "off-net-dev")
DATASET = os.environ.get("BQ_DATASET", "prisma")
EVENTS_RAW = os.environ.get("BQ_EVENTS_RAW_TABLE", "events_raw")
PORT = os.environ.get("PORT", "8080")

KEYS_NOT_NEEDED_IN_RESULTS = {"packages", "distro", "distroRelease",
                              "complianceScanPassed", "complianceDistribution", "vulnerabilityDistribution", "vulnerabilityScanPassed", "history", "applications", "collections", "digest"}
KEYS_NOT_NEEDED_IN_COMPLIANCES = {"layerTime", "category"}
KEYS_NOT_NEEDED_IN_VULNERABILITIES = {"vector", "riskFactors",
                                      "impactedVersions", "publishedDate", "discoveredDate", "layerTime", "fixDate"}