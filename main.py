import json
import sys
import logging
import os
import google.cloud.logging
from google.cloud import bigquery
from google.cloud.functions.context import Context
from typing import Sequence

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "off-net-dev")
DATASET = os.environ.get("BQ_DATASET", "prisma")
ALL_EVENTS_TABLE = os.environ.get("BQ_ALL_EVENTS_TABLE", "events_raw")

KEYS_NOT_NEEDED_IN_RESULTS = {"packages", "distro", "distroRelease",
                              "complianceScanPassed", "complianceDistribution", "vulnerabilityDistribution", "vulnerabilityScanPassed", "history", "applications", "collections", "digest"}

KEYS_NOT_NEEDED_IN_COMPLIANCES = {"layerTime", "category"}

KEYS_NOT_NEEDED_IN_VULNERABILITIES = {"vector", "riskFactors",
                                      "impactedVersions", "publishedDate", "discoveredDate", "layerTime", "fixDate"}
CLOUD_EVENT_ATTRIBUTES = [
    "source", "event_type", "id", "metadata", "time_created", "signature",
    "msg_id",
]


def setup_cloud_logging():
    # Only use the root logger from the 'logging' package to push everything to Cloud Logging.
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        # We only configure Cloud Logging when we have appropriate credentials available.
        google.cloud.logging.Client().setup_logging(log_level=logging.DEBUG)
    logging.basicConfig(
        format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d]: %(message)s",
        level=logging.DEBUG)


def process_cloud_event(cloud_event: dict) -> Sequence[dict]:
    # processing payload/setting up
    metadata = cloud_event["metadata"]
    row = dict()
    for key in CLOUD_EVENT_ATTRIBUTES:
        row[key] = cloud_event.get(key)
    row["metadata"] = json.dumps(metadata)

    table_id = "off-net-dev.prisma.events_raw3"
    log_json_data("Storing cloud deploy data", row)
    return write_to_bigquery(table_id, rows=[row])


def write_to_bigquery(table_id: str, rows: list[dict]) -> Sequence[dict]:
    # insert payload into the bq
    client = bigquery.Client()
    bq_errors = client.insert_rows_json(
        table_id, rows, row_ids=[None] * len(rows))
    return bq_errors


def log_json_data(msg: str, data: dict):
    json_str = json.dumps(data)
    logging.debug(f"{msg}: {json_str}")


def transform_payload(data):
    scan_results = data
    # remove keys that are not needed to reduce payload size
    for not_needed_key in KEYS_NOT_NEEDED_IN_RESULTS:
        scan_results["results"][0].pop(not_needed_key, None)

    if(scan_results["results"][0].get("compliances")):
        # TODO: handle error if complicances is explicitly null or undefined
        for compliance in scan_results["results"][0]["compliances"]:
            for not_needed_key in KEYS_NOT_NEEDED_IN_COMPLIANCES:
                compliance.pop(not_needed_key, None)

    if(scan_results["results"][0].get("vulnerabilities")):
        # TODO: handle error if vulnerabilities is explicitly null or undefined
        for vulnerability in scan_results["results"][0]["vulnerabilities"]:
            for not_needed_key in KEYS_NOT_NEEDED_IN_VULNERABILITIES:
                vulnerability.pop(not_needed_key, None)

    scan_results.pop("consoleURL", None)

    event_payload = {
        "source": "prisma",
        "event_type": "pull_request",
        "id": scan_results["results"][0].get("id"),
        "metadata": scan_results,
        "time_created": scan_results["results"][0].get("scanTime"),
        "signature": scan_results["results"][0].get("scanID"),
        "msg_id": "msg id from pubsub"
    }
    return process_cloud_event(event_payload)


def process_event():
    f = open('raw_payload.json')
    s = open('raw_payload.json')
    data = json.load(s)
    transform_payload(data)


if __name__ == "__main__":
    process_event()
