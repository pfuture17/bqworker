# test bigquery tables, create if not exist

import os
import json
import prisma_schema
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "off-net-dev")
DATASET = os.environ.get("BQ_DATASET", "prisma")
ALL_EVENTS_TABLE = os.environ.get("BQ_ALL_EVENTS_TABLE", "test_table")

client = bigquery.Client()
table_id = f'{PROJECT}.{DATASET}.{ALL_EVENTS_TABLE}'

# -------------------------------------------------------
# if file exist method
# try:
#     file_exists = os.path.exists(
#         f'schema/{DATASET}.{ALL_EVENTS_TABLE}.schema.json')
#     if file_exists:
#         print('insert nalang ang gagawin')
# except:
#     print('gagawa ng table')
# -------------------------------------------------------
# get schema structure
f = open('schema/prisma.events_raw.schema.json')
# --------------------------------------------------------

# TODO(developer): Set table_id to the ID of the table to determine existence.
# table_id = "your-project.your_dataset.your_table"
# schema = []
print(prisma_schema)

# for field in prisma_schema:

# try:
#     client.get_table(table_id)  # Make an API request.
#     print("mag iinsert nalang")
# except NotFound:
#     table = bigquery.Table(table_id, f)
#     table = client.create_table(table)  # Make an API request.
#     print(
#         "Created table {}.{}.{}".format(
#             table.project, table.dataset_id, table.table_id)
#     )
