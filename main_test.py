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
    
    # pubsub_msg = {
    #     "message": {
    #         "data": base64.b64encode(commit).decode("utf-8"),
    #         "attributes": {"headers": json.dumps(headers)},
    #         "message_id": "foobar",
    #     },
    # }

    # github_event = {
    #     "event_type": "pull_request",
    #     "id": "sha256:b2d2872c3dff03356230c2a2c45dee8f16d01c29d3fe8b90e08f44ccd8c59f01",
    #     "metadata": reduced_prisma_scan_results,
    #     "time_created": "2022-11-21T14:14:19.775876608Z",
    #     "signature": "637b87bb0598e00d2e01c30b",
    #     "msg_id": "5877041794375121",
    #     "source": "prisma",
    # }

    r = client.post(
        "/",
        data=json.dumps(pubsub_msg),
        headers={"Content-Type": "application/json"},
    )

    mock_process_bq_insertion.assert_called_with(github_event)
    assert r.status_code == 204


# def test_github_event_avoid_id_conflicts_pull_requests(client):

#     headers = {"X-Github-Event": "pull_request", "X-Hub-Signature": "foo"}
#     commit = json.dumps({
#         "pull_request": {
#             "updated_at": "2021-06-15T13:12:14Z"
#         },
#         "repository": {
#             "name": "reponame"
#         },
#         "number": 477
#     }).encode("utf-8")

#     encoded_commit = {
#         "data": base64.b64encode(commit).decode("utf-8"),
#         "attributes": {"headers": json.dumps(headers)},
#         "message_id": "foobar",
#     }

#     github_event_calculated = main.process_github_event(headers=headers, msg=encoded_commit)
#     github_event_expected = {
#         "id": "reponame/477"
#     }

#     assert github_event_calculated["id"] == github_event_expected["id"]


# def test_github_event_avoid_id_conflicts_issues(client):

#     headers = {"X-Github-Event": "issues", "X-Hub-Signature": "foo"}
#     commit = json.dumps({
#         "issue": {
#             "updated_at": "2021-06-15T13:12:14Z",
#             "number": 477
#         },
#         "repository": {
#             "name": "reponame"
#         }
#     }).encode("utf-8")

#     encoded_commit = {
#         "data": base64.b64encode(commit).decode("utf-8"),
#         "attributes": {"headers": json.dumps(headers)},
#         "message_id": "foobar",
#     }

#     github_event_calculated = main.process_github_event(headers=headers, msg=encoded_commit)
#     github_event_expected = {
#         "id": "reponame/477"
#     }

#     assert github_event_calculated["id"] == github_event_expected["id"]