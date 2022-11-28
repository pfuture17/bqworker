from google.cloud import bigquery
from typing import Sequence
from constant import DATASET, EVENTS_RAW
import google.cloud.logging
import logging
import json
import os


def setup_cloud_logging():
    # Only use the root logger from the 'logging' package to push everything to Cloud Logging.
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        # We only configure Cloud Logging when we have appropriate credentials available.
        google.cloud.logging.Client().setup_logging(log_level=logging.DEBUG)
    logging.basicConfig(
        format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d]: %(message)s",
        level=logging.DEBUG)


def log_json_data(msg: str, data: dict):
    '''Specifically used for logging json data'''
    json_str = json.dumps(data)
    logging.debug(f"{msg}: {json_str}")


def write_to_bigquery(rows: list[tuple]) -> Sequence[dict]:
    '''Insert payload into BigQuery'''
    # will need to be authenticated here
    client = bigquery.Client()
    
    table_ref = client.dataset(DATASET).table(EVENTS_RAW)
    table = client.get_table(table_ref)

    bq_errors = client.insert_rows(
        table, rows)

    if bq_errors is not None:
        raise Exception(bq_errors)


def process_bq_insertion(cloud_event: dict) -> Sequence[dict]:
    '''One last preparation before inserting to BigQuery'''
    cloud_event["metadata"] = json.dumps(cloud_event["metadata"])

    row_to_insert = (
        cloud_event["event_type"],
        cloud_event["id"],
        cloud_event["metadata"],
        cloud_event["time_created"],
        cloud_event["signature"],
        cloud_event["msg_id"],
        cloud_event["source"],
    )

    log_json_data("Row to be inserted to bigquery", row_to_insert)

    write_to_bigquery(rows=[row_to_insert])
