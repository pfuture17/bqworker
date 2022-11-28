import json
import logging
import base64
from constant import KEYS_NOT_NEEDED_IN_RESULTS, KEYS_NOT_NEEDED_IN_COMPLIANCES, KEYS_NOT_NEEDED_IN_VULNERABILITIES, PORT
from shared.insert_to_bq import process_bq_insertion, setup_cloud_logging

from flask import Flask, request

app = Flask(__name__)


@app.route("/", methods=["POST"])
def index():
    """
    Receives messages from a push subscription from Pub/Sub.
    Parses the message, and inserts it into BigQuery.
    """
    logging.debug("Execute index")
    
    # Check request for JSON
    if not request.is_json:
        raise Exception("Expecting JSON payload")
    envelope = request.get_json()

    # Check that message is a valid pub/sub message
    if envelope.get("message") is None:
        raise Exception("Not a valid Pub/Sub Message")
    
    logging.info("Extracting message from pubsub envelope...")
    msg_ingest_layer = envelope["message"]
    data_ingest_layer = json.loads(base64.b64decode(
        msg_ingest_layer["data"]).decode("utf-8"))

    msg = data_ingest_layer["data"]["message"]

    try:
        # these are the main functions that are called throughout the whole process, begin following call stack in transform_payload
        setup_cloud_logging()
        logging.info("Starting process...")
        cloud_event = transform_payload(msg)
        process_bq_insertion(cloud_event)

    except Exception as e:
        entry = {
            "severity": "WARNING",
            "msg": "Data not saved to BigQuery",
            "errors": str(e),
            "json_payload": envelope
        }
        logging.error(json.dumps(entry))

    return "", 204


def transform_payload(msg):
    '''Remove fields that are not needed to reduce payload size and transform the scan results into a cloud event'''
    
    logging.info("Execute transform_payload")

    logging.info("Decoding pubsub message...")
    scan_results = json.loads(base64.b64decode(
        msg["data"]).decode("utf-8"))

    logging.info("Removing fields that are not needed...")
    # remove direct children of result[0] that are not needed
    for not_needed_key in KEYS_NOT_NEEDED_IN_RESULTS:
        scan_results["results"][0].pop(not_needed_key, None)

    # remove direct children of compliances that are not needed
    if(scan_results["results"][0].get("compliances")):
        for compliance in scan_results["results"][0]["compliances"]:
            for not_needed_key in KEYS_NOT_NEEDED_IN_COMPLIANCES:
                compliance.pop(not_needed_key, None)

    # remove direct children of vulnerabilities that are not needed
    if(scan_results["results"][0].get("vulnerabilities")):
        for vulnerability in scan_results["results"][0]["vulnerabilities"]:
            for not_needed_key in KEYS_NOT_NEEDED_IN_VULNERABILITIES:
                vulnerability.pop(not_needed_key, None)

    # we also don't need this
    scan_results.pop("consoleURL", None)

    logging.info("Constructing cloud event...")
    # this is the cloud event
    event_payload = {
        "source": "prisma",
        "event_type": "pull_request",
        "id": scan_results["results"][0].get("id"),
        "metadata": json.dumps(scan_results),
        "time_created": scan_results["results"][0].get("scanTime"),
        # should we use scanId as signaure?
        "signature": scan_results["results"][0].get("scanID"),
        "msg_id": msg.get("message_id")
    }

    logging.info(f'Transformed payload: {event_payload}')

    return event_payload


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(PORT), debug=True)
