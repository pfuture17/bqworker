import base64
import json
import os
import main
import mock
import pytest
import shared


@pytest.fixture
def client():
    main.app.testing = True
    return main.app.test_client()


@ pytest.fixture(scope="function")
def mock_process_bq_insertion(mocker):
    """ Fixture to mock the Publisher Client """
    mocked_process_bq_insertion = mocker.patch(
        'shared.insert_to_bq.process_bq_insertion', autospec=True)
    return mocked_process_bq_insertion


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


def test_github_event_processed(client, mock_process_bq_insertion):    
    pubsub_msg = load_test_json_data('pubsub-payload-from-ingest-topic.json')
    github_event = load_test_json_data('reduced-prisma-scan-vulnerabilities-pull-request.json')
    
    main.process_bq_insertion = mock.MagicMock()

    r = client.post(
        "/",
        data=json.dumps(pubsub_msg),
        headers={"Content-Type": "application/json"},
    )

    mock_process_bq_insertion.assert_called_with(github_event)
    assert r.status_code == 204

    main.process_bq_insertion.assert_called_with(github_event)
    assert r.status_code == 204