import json
import base64
from constant import PORT
from shared.insert_to_bq import process_bq_insertion

from flask import Flask, request

app = Flask(__name__)


@app.route("/", methods=["POST"])
def index():
    """
    Receives messages from a push subscription from Pub/Sub.
    Parses the message, and inserts it into BigQuery.
    """
    print("Execute index")
    
    # Check request for JSON
    if not request.is_json:
        raise Exception("Expecting JSON payload")
    envelope = request.get_json()

    # Check that message is a valid pub/sub message
    if envelope.get("message") is None:
        raise Exception("Not a valid Pub/Sub Message")
    
    print("Extracting message from pubsub envelope...")
    msg_ingest_layer = envelope["message"]
    data_ingest_layer = json.loads(base64.b64decode(
        msg_ingest_layer["data"]).decode("utf-8"))

    msg = data_ingest_layer["data"]["message"]

    try:
        # these are the main functions that are called throughout the whole process, begin following call stack in construct_raw_event
        print("Starting process...")
        raw_event = construct_raw_event(msg)
        process_bq_insertion(raw_event)

    except Exception as e:
        entry = {
            "severity": "WARNING",
            "msg": "Data not saved to BigQuery",
            "errors": str(e),
            "json_payload": envelope
        }
        print(json.dumps(entry))

    return "", 204


def construct_raw_event(msg):
    '''Contruct the raw event that will be inserted into BigQuery'''
    
    print("Execute construct_raw_event")

    print("Decoding pubsub message...")
    scan_results = json.loads(base64.b64decode(
        msg["data"]).decode("utf-8"))

    print("Constructing raw event...")
    # this is the raw event to be inserted to events_raw table
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

    print(f'Constructed payload: {event_payload}')

    return event_payload


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(PORT), debug=True)
