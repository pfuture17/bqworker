import json
import logging
from constant import TABLE_ID, KEYS_NOT_NEEDED_IN_RESULTS, KEYS_NOT_NEEDED_IN_COMPLIANCES, KEYS_NOT_NEEDED_IN_VULNERABILITIES
from shared.process_cloud_event import process_cloud_event, setup_cloud_logging


def transform_payload(scan_results):
    '''Remove fields that are not needed to reduce payload size and transform the scan results into a cloud event'''

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
        "signature": scan_results["results"][0].get("scanID"),
        "msg_id": "msg id from pubsub"
    }

    logging.info(f'Transformed payload: {event_payload}')

    return process_cloud_event(event_payload, TABLE_ID)


# temporary index for now
def process_event():
    setup_cloud_logging()
    f = open('raw_payload.json')
    s = open('raw_payload.json')
    data = json.load(s)
    transform_payload(data)


if __name__ == "__main__":
    process_event()
