import base64
import json
import os
import main
import mock
import pytest


@pytest.fixture
def client():
    main.app.testing = True
    return main.app.test_client()


def load_test_json_data(data_file: str) -> any:
    """ Fixture to mock an input Pub/Sub event """
    path = os.path.join(os.getcwd(), f"test_data/{data_file}")
    with open(path, 'r', encoding='utf-8') as file:
        return json.loads(file.read())


def test_not_json(client):
    with pytest.raises(Exception) as e:
        client.post("/", data="foo")

    assert "Expecting JSON payload" in str(e.value)


def test_not_pubsub_message(client):
    with pytest.raises(Exception) as e:
        client.post(
            "/",
            data=json.dumps({"foo": "bar"}),
            headers={"Content-Type": "application/json"},
        )

    assert "Not a valid Pub/Sub Message" in str(e.value)


def test_prisma_event_process(client):    
    pubsub_msg = load_test_json_data('pubsub-payload-from-ingest-topic.json')
    prisma_event = load_test_json_data('reduced-prisma-scan-vulnerabilities-pull-request.json')
    
    main.process_bq_insertion = mock.MagicMock()

    r = client.post(
        "/",
        data=json.dumps(pubsub_msg),
        headers={"Content-Type": "application/json"},
    )
    
    prisma_event['metadata'] = json.dumps(prisma_event['metadata'])

    main.process_bq_insertion.assert_called_with(prisma_event)
    assert r.status_code == 204