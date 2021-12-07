import json
import pytest

from kicksaw_integration_utils import SalesforceClient
from kicksaw_integration_app_client import KicksawSalesforce

from simple_mockforce import mock_salesforce

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
    getattr(
        _salesforce, f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.INTEGRATION}"
    ).create({"Name": LAMBDA_NAME})
    salesforce = KicksawSalesforce(CONNECTION_OBJECT, LAMBDA_NAME, {})

    response = salesforce.query(
        f"Select Id From {KicksawSalesforce.NAMESPACE}{KicksawSalesforce.EXECUTION}"
    )
    assert response["totalSize"] == 1

    records = response["records"]
    record = records[0]

    assert record["Id"] == salesforce.execution_object_id

    # instantiating with an id means we don't create an execution object
    salesforce = KicksawSalesforce(
        CONNECTION_OBJECT,
        LAMBDA_NAME,
        {},
        execution_object_id=salesforce.execution_object_id,
    )

    # since we provided an id, the above instantiation should not have created another
    # execution object
    response = salesforce.query(
        f"Select Id From {KicksawSalesforce.NAMESPACE}{KicksawSalesforce.EXECUTION}"
    )
    assert response["totalSize"] == 1

    records = response["records"]
    record = records[0]

    assert record["Id"] == salesforce.execution_object_id


@mock_salesforce(fresh=True)
def test_kicksaw_salesforce_client():
    _salesforce = SalesforceClient(**CONNECTION_OBJECT)

    KicksawSalesforce.NAMESPACE = ""

    integration__c = getattr(
        _salesforce, f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.INTEGRATION}"
    ).create({"Name": LAMBDA_NAME})
    integration_id = integration__c["id"]

    step_function_payload = {"start_date": "2021-10-12"}
    salesforce = KicksawSalesforce(
        CONNECTION_OBJECT, LAMBDA_NAME, step_function_payload
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

    salesforce.complete_execution()

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
        Select Id, {KicksawSalesforce.SUCCESSFUL_COMPLETION}, {KicksawSalesforce.ERROR_MESSAGE} 
        From {KicksawSalesforce.NAMESPACE}{KicksawSalesforce.EXECUTION}
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


@mock_salesforce(fresh=True)
def test_kicksaw_salesforce_client_exception():
    KicksawSalesforce.NAMESPACE = ""

    _salesforce = SalesforceClient(**CONNECTION_OBJECT)
    getattr(
        _salesforce, f"{KicksawSalesforce.NAMESPACE}{KicksawSalesforce.INTEGRATION}"
    ).create({"Name": LAMBDA_NAME})
    salesforce = KicksawSalesforce(CONNECTION_OBJECT, LAMBDA_NAME, {})
    salesforce.handle_exception("Code died")

    response = salesforce.query(
        f"""
        Select Id, {KicksawSalesforce.SUCCESSFUL_COMPLETION}, {KicksawSalesforce.ERROR_MESSAGE} 
        From {KicksawSalesforce.NAMESPACE}{KicksawSalesforce.EXECUTION}
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
