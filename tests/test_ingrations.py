import json
import pytest

from kicksaw_integration_utils import SalesforceClient
from kicksaw_integration_app_client import KicksawSalesforce, LogLevel

from simple_mockforce import mock_salesforce

INTEGRATION_NAME = "example-integration"
LAMBDA_NAME = "example-lambda"

CONNECTION_OBJECT = {
    "username": "fake",
    "password": "fake",
    "security_token": "fake",
    "domain": "fake",
}


@mock_salesforce(fresh=True)
@pytest.mark.parametrize("namespace", ["", "KicksawEng__"])
def test_kicksaw_salesforce_client_instantiation(namespace):
    KicksawSalesforce.NAMESPACE = namespace

    _salesforce = SalesforceClient(**CONNECTION_OBJECT)
    # test that we spawn an integration if it's missing when create_missing_integration is true
    # but we need to create an integration so that the object exists in mockforce
    KicksawSalesforce.create_integration(_salesforce, "randomname", LAMBDA_NAME)
    salesforce = KicksawSalesforce(
        CONNECTION_OBJECT, INTEGRATION_NAME, {}, create_missing_integration=True
    )

    response = salesforce.query(
        f"Select Id From {KicksawSalesforce.NAMESPACE}{KicksawSalesforce.EXECUTION}"
    )
    assert response["totalSize"] == 1

    records = response["records"]
    record = records[0]

    assert record["Id"] == salesforce.execution_object_id

    # instantiating with an id means we don't create an execution object
    salesforce = KicksawSalesforce.instantiate_from_id(
        CONNECTION_OBJECT,
        execution_object_id=salesforce.execution_object_id,
    )

    # since we provided an id, the above instantiation should not have created another
    # execution object
    response = salesforce.query(
        f"Select Id, {KicksawSalesforce.NAMESPACE}{KicksawSalesforce.INTEGRATION} From {KicksawSalesforce.NAMESPACE}{KicksawSalesforce.EXECUTION}"
    )
    assert response["totalSize"] == 1

    records = response["records"]
    record = records[0]

    assert record["Id"] == salesforce.execution_object_id

    response = salesforce.query(
        f"Select Id From {KicksawSalesforce.NAMESPACE}{KicksawSalesforce.INTEGRATION} Where Name = '{INTEGRATION_NAME}'"
    )
    records = response["records"]
    integration = records[0]

    assert (
        record[f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.INTEGRATION}"]
        == integration["Id"]
    )


@mock_salesforce(fresh=True)
def test_kicksaw_salesforce_client():
    _salesforce = SalesforceClient(**CONNECTION_OBJECT)

    KicksawSalesforce.NAMESPACE = ""
    integration__c = KicksawSalesforce.create_integration(
        _salesforce, INTEGRATION_NAME, LAMBDA_NAME
    )
    integration_id = integration__c["id"]

    step_function_payload = {"start_date": "2021-10-12"}
    salesforce = KicksawSalesforce(
        CONNECTION_OBJECT, INTEGRATION_NAME, step_function_payload
    )

    execution_object = salesforce.get_execution_object()

    assert (
        execution_object[
            f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.INTEGRATION}"
        ]
        == integration_id
    )
    assert execution_object[
        f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.EXECUTION_PAYLOAD}"
    ] == json.dumps(step_function_payload)

    data = [
        {"UpsertKey__c": "1a2b3c", "Name": "Name 1"},
        {"UpsertKey__c": "xyz123", "Name": "Name 2"},
        # note, this is a duplicate id, so this and the first row will fail
        {"UpsertKey__c": "1a2b3c", "Name": "Name 1"},
    ]

    response = salesforce.bulk.CustomObject__c.upsert(data, "UpsertKey__c")
    salesforce.log("Some message", LogLevel.INFO)

    response = salesforce.query(
        f"""
        Select 
            Id, 
            {KicksawSalesforce.NAMESPACE}{KicksawSalesforce.LOG_MESSAGE},
            {KicksawSalesforce.NAMESPACE}{KicksawSalesforce.LOG_LEVEL}
        From
            {KicksawSalesforce.NAMESPACE}{KicksawSalesforce.LOG}
        """
    )
    records = response["records"]
    log = records[0]
    assert (
        log[f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.LOG_MESSAGE}"]
        == "Some message"
    )
    assert log[f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.LOG_LEVEL}"] == "INFO"

    salesforce.complete_execution(response_payload={"AllGood": True})

    response = salesforce.query(
        f"""
        Select
            {f'{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.EXECUTION}'},
            {f'{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.OPERATION}'}, 
            {f'{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.SALESFORCE_OBJECT}'}, 
            {f'{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.ERROR_CODE}'},
            {f'{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.ERROR_MESSAGE}'}, 
            {f'{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.UPSERT_KEY}'}, 
            {f'{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.UPSERT_KEY_VALUE}'},
            {f'{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.OBJECT_PAYLOAD}'}
        From {f'{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.ERROR}'}
        """
    )
    assert response["totalSize"] == 2
    records = response["records"]

    count = 0
    for record in records:
        assert (
            record[f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.EXECUTION}"]
            == execution_object["Id"]
        )
        assert (
            record[f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.OPERATION}"]
            == "upsert"
        )
        assert (
            record[
                f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.SALESFORCE_OBJECT}"
            ]
            == "CustomObject__c"
        )
        assert (
            record[f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.ERROR_CODE}"]
            == "DUPLICATE_EXTERNAL_ID"
        )
        assert (
            record[f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.ERROR_MESSAGE}"]
            == "A user-specified external ID matches more than one record during an upsert."
        )
        assert (
            record[f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.UPSERT_KEY}"]
            == "UpsertKey__c"
        )
        assert (
            record[f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.UPSERT_KEY_VALUE}"]
            == "1a2b3c"
        )
        assert record[
            f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.OBJECT_PAYLOAD}"
        ] == json.dumps(
            {
                "UpsertKey__c": "1a2b3c",
                "Name": "Name 1",
            }
        )

        count += 1

    assert count == 2

    # check that we marked it as completed
    response = salesforce.query(
        f"""
        Select 
            Id,
            {KicksawSalesforce.SUCCESSFUL_COMPLETION},
            {KicksawSalesforce.ERROR_MESSAGE},
            {KicksawSalesforce.RESPONSE_PAYLOAD}
        From
            {KicksawSalesforce.NAMESPACE}{KicksawSalesforce.EXECUTION}
        """
    )
    assert response["totalSize"] == 1

    records = response["records"]
    record = records[0]

    assert record["Id"] == salesforce.execution_object_id
    assert (
        record[
            f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.SUCCESSFUL_COMPLETION}"
        ]
        == True
    )
    assert (
        record[f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.ERROR_MESSAGE}"]
        == None
    )
    assert json.loads(
        record[f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.RESPONSE_PAYLOAD}"]
    ) == {"AllGood": True}

    salesforce.update_execution_object_payload({"success": True})

    # check that we updated the payload
    response = salesforce.query(
        f"""
        Select 
            Id,
            {KicksawSalesforce.EXECUTION_PAYLOAD}
        From
            {KicksawSalesforce.NAMESPACE}{KicksawSalesforce.EXECUTION}
        """
    )
    assert response["totalSize"] == 1

    records = response["records"]
    record = records[0]

    assert (
        record[f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.EXECUTION_PAYLOAD}"]
        == '{"success": true}'
    )


@mock_salesforce(fresh=True)
def test_kicksaw_salesforce_client_exception():
    KicksawSalesforce.NAMESPACE = ""

    _salesforce = SalesforceClient(**CONNECTION_OBJECT)
    KicksawSalesforce.create_integration(_salesforce, INTEGRATION_NAME, LAMBDA_NAME)
    salesforce = KicksawSalesforce(CONNECTION_OBJECT, INTEGRATION_NAME, {})
    salesforce.handle_exception("Code died")

    response = salesforce.query(
        f"""
        Select 
            Id,
            {KicksawSalesforce.SUCCESSFUL_COMPLETION},
            {KicksawSalesforce.ERROR_MESSAGE},
            {KicksawSalesforce.RESPONSE_PAYLOAD}
        From
            {KicksawSalesforce.NAMESPACE}{KicksawSalesforce.EXECUTION}
        """
    )
    assert response["totalSize"] == 1

    records = response["records"]
    record = records[0]

    assert record["Id"] == salesforce.execution_object_id
    assert (
        record[
            f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.SUCCESSFUL_COMPLETION}"
        ]
        == False
    )
    assert (
        record[f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.ERROR_MESSAGE}"]
        == "Code died"
    )
    assert (
        record[f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.RESPONSE_PAYLOAD}"]
        == None
    )

    salesforce.log("Testing...", LogLevel.INFO)

    response = salesforce.query(
        f"""
        Select 
            {KicksawSalesforce.NAMESPACE}{KicksawSalesforce.PARENT_EXECUTION}
        From
            {KicksawSalesforce.NAMESPACE}{KicksawSalesforce.LOG}
        """
    )
    assert response["totalSize"] == 1

    log = response["records"][0]
    assert (
        log[f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.PARENT_EXECUTION}"]
        == salesforce.execution_object_id
    )
