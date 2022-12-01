import os

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "off-net-dev")
DATASET = os.environ.get("BQ_DATASET", "prisma")
EVENTS_RAW = os.environ.get("BQ_EVENTS_RAW_TABLE", "events_raw")
PORT = os.environ.get("PORT", "8080")