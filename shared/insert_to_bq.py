from google.cloud import bigquery
from constant import DATASET, EVENTS_RAW
import google.cloud.logging
import logging
import os


def setup_cloud_logging():
    # Only use the root logger from the 'logging' package to push everything to Cloud Logging.
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        # We only configure Cloud Logging when we have appropriate credentials available.
        google.cloud.logging.Client().setup_logging(log_level=logging.DEBUG)
    logging.basicConfig(
        format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d]: %(message)s",
        level=logging.DEBUG)
    

def process_bq_insertion(cloud_event: dict):
    '''One last preparation before inserting to BigQuery'''
    
    logging.info("Execute process_bq_insertion")
    
    client = bigquery.Client()

    table_ref = client.dataset(DATASET).table(EVENTS_RAW)
    table = client.get_table(table_ref)
    
    row_to_insert = [(
        cloud_event["event_type"],
        cloud_event["id"],
        cloud_event["metadata"],
        cloud_event["time_created"],
        cloud_event["signature"],
        cloud_event["msg_id"],
        cloud_event["source"],
    )]

    logging.info(f'Row to be inserted to BigQuery: {row_to_insert}')
    
    logging.info("Inserting to BigQuery...")

    bq_errors = client.insert_rows(
        table, row_to_insert)

    if bq_errors:
        raise Exception(bq_errors)
