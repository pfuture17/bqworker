from google.cloud import bigquery
from constant import DATASET, EVENTS_RAW
import google.cloud.logging
import logging
import os

def process_bq_insertion(raw_event: dict):
    '''One last preparation before inserting to BigQuery'''
    
    logging.info("Execute process_bq_insertion")
    
    client = bigquery.Client()

    table_ref = client.dataset(DATASET).table(EVENTS_RAW)
    table = client.get_table(table_ref)
    
    row_to_insert = [(
        raw_event["event_type"],
        raw_event["id"],
        raw_event["metadata"],
        raw_event["time_created"],
        raw_event["signature"],
        raw_event["msg_id"],
        raw_event["source"],
    )]

    logging.info(f'Row to be inserted to BigQuery: {row_to_insert}')
    
    logging.info("Inserting to BigQuery...")

    bq_errors = client.insert_rows(
        table, row_to_insert)

    if bq_errors:
        raise Exception(bq_errors)
