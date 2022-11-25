import json
import logging
import base64
from constant import TABLE_ID, KEYS_NOT_NEEDED_IN_RESULTS, KEYS_NOT_NEEDED_IN_COMPLIANCES, KEYS_NOT_NEEDED_IN_VULNERABILITIES, PORT
from shared.insert_to_bq import process_bq_insertion, setup_cloud_logging

from flask import Flask, request

app = Flask(__name__)


@app.route("/", methods=["POST"])
def index():
    """
    Receives messages from a push subscription from Pub/Sub.
    Parses the message, and inserts it into BigQuery.
    """
    # Check request for JSON
    if not request.is_json:
        raise Exception("Expecting JSON payload")
    envelope = request.get_json()

    # Check that message is a valid pub/sub message
    if envelope.get("message") is None:
        raise Exception("Not a valid Pub/Sub Message")

    msg_ingest_layer = envelope["message"]
    data_ingest_layer = json.loads(base64.b64decode(
        msg_ingest_layer["data"]).decode("utf-8"))

    msg = data_ingest_layer["data"]["message"]

    # sample_request = open("sample_request.json", "w")
    # sample_request.write(json.dumps(json.loads(base64.b64decode(
    #     msg["data"]).decode("utf-8"))))
    # sample_request.close()

    try:
        # these are the main functions that are called throughout the whole process, begin following call stack in transform_payload
        setup_cloud_logging()
        cloud_event = transform_payload(msg)
        process_bq_insertion(cloud_event, TABLE_ID)

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

    scan_results = json.loads(base64.b64decode(
        msg["data"]).decode("utf-8"))

    print(type(scan_results))

    # remove direct children of result[0] that are not needed
    for not_needed_key in KEYS_NOT_NEEDED_IN_RESULTS:
        scan_results["results"][0].pop(not_needed_key)

    # remove direct children of compliances that are not needed
    if(scan_results["results"][0].get("compliances")):
        for compliance in scan_results["results"][0]["compliances"]:
            for not_needed_key in KEYS_NOT_NEEDED_IN_COMPLIANCES:
                compliance.pop(not_needed_key)

    # remove direct children of vulnerabilities that are not needed
    if(scan_results["results"][0].get("vulnerabilities")):
        for vulnerability in scan_results["results"][0]["vulnerabilities"]:
            for not_needed_key in KEYS_NOT_NEEDED_IN_VULNERABILITIES:
                vulnerability.pop(not_needed_key)

    # we also don't need this
    scan_results.pop("consoleURL")

    # this is the cloud event
    event_payload = {
        "source": "prisma",
        "event_type": "pull_request",
        "id": scan_results["results"][0].get("id"),
        "metadata": scan_results,
        "time_created": scan_results["results"][0].get("scanTime"),
        # should we use scanId as signaure?
        "signature": scan_results["results"][0].get("scanID"),
        "msg_id": msg.get("message_id")
    }

    logging.info(f'Transformed payload: {event_payload}')
    
    return event_payload


# temporary index
# def process_event():
#     setup_cloud_logging()
#     f = open('raw_payload.json')
#     s = open('raw_payload.json')
#     data = json.load(s)
#     transform_payload(data)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(PORT), debug=True)
