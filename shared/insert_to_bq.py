from google.cloud import bigquery
from typing import Sequence
from constant import CLOUD_EVENT_ATTRIBUTES
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


def write_to_bigquery(table_id: str, rows: list[dict]) -> Sequence[dict]:
    '''Insert payload into BigQuery'''
    # will need to be authenticated here
    client = bigquery.Client()

    bq_errors = client.insert_rows_json(
        table_id, rows, row_ids=[None] * len(rows))
    
    if bq_errors is not None:
        raise Exception(bq_errors)


def process_bq_insertion(cloud_event: dict, table_id: str) -> Sequence[dict]:
    '''One last preparation before inserting to BigQuery'''
    metadata = cloud_event["metadata"]
    row = dict()

    for key in CLOUD_EVENT_ATTRIBUTES:
        row[key] = cloud_event.get(key)
    row["metadata"] = json.dumps(metadata)

    log_json_data("Row to be inserted to bigquery", row)

    write_to_bigquery(table_id, rows=[row])
    
