import csv
import io
import logging
import time

from typing import Generator

from rich.logging import RichHandler
from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)
logger.handlers = [RichHandler()]
logger.setLevel(logging.DEBUG)


def bulk_v2_query(
    salesforce: Salesforce,
    query: str | None = None,
    max_records: int = 10000,
    job_id: str | None = None,
) -> Generator[dict[str, str], None, None]:
    """
    Query Salesforce using the Bulk 2.0 API.

    Parameters
    ----------
    salesforce : Salesforce
        Salesforce client.
    query : str, optional
        SOQL query. Must be specified if job_id is not specified, by default None.
        If not specified, job_id must be specified.
    max_records : int, optional
        Maximum number of records to return per chunk, by default 10000.
    job_id : str, optional
        Job ID. Use this parameter to resume a previously scheduled query.
        Must be specified if query is not specified, by default None.
        If not specified, query must be specified.

    Yields
    ------
    dict[str, str]
        Single record.

    """
    assert (query is not None) ^ (
        job_id is not None
    ), "Either query or job_id must be specified, but not both"

    # Schedule query if job_id is not specified
    if job_id is None:
        assert query is not None
        logger.debug(
            "Scheduling query '%s'",
            query[:20] + " ... " + query[-20:] if len(query) > 40 else query,
        )
        response = salesforce.session.post(
            "/".join(
                [
                    salesforce.base_url.rstrip("/"),
                    "jobs",
                    "query",
                ]
            ),
            headers=salesforce.headers,
            json={
                "query": query,
                "operation": "query",
            },
            timeout=30,
        )
        if not response.ok:
            raise RuntimeError(
                f"Failed to schedule query '{query}' due to:\n\n{response.json()}"
            )
        response.raise_for_status()
        job_id = response.json()["id"]
        logger.debug("Query scheduled as job '%s'", job_id)

    # Wait for job to complete
    while True:
        response = salesforce.session.get(
            "/".join(
                [
                    salesforce.base_url.rstrip("/"),
                    "jobs",
                    "query",
                    job_id,
                ]
            ),
            headers=salesforce.headers,
            timeout=30,
        )
        response.raise_for_status()
        match state := response.json()["state"]:
            case "UploadComplete" | "InProgress":
                logger.debug(
                    "Job '%s' is in state '%s', sleeping for 5 seconds",
                    job_id,
                    state,
                )
                time.sleep(5)
            case "Failed" | "Aborted":
                raise RuntimeError(
                    f"Job '{job_id}' failed with state '{state}': {response.json()}"
                )
            case _:
                logger.debug("Job '%s' finished with state '%s'", job_id, state)
                break

    # Iterate over query results
    locator = None
    while True:
        response = salesforce.session.get(
            "/".join(
                [
                    salesforce.base_url.rstrip("/"),
                    "jobs",
                    "query",
                    job_id,
                    "results",
                ]
            ),
            params={"locator": locator, "maxRecords": max_records},
            headers=salesforce.headers,
            timeout=60,
        )
        response.raise_for_status()
        if locator is None:
            logger.debug("Returning first chunk")
        else:
            logger.debug("Returning chunk with locator '%s'", locator)
        rows = list(
            csv.reader(
                io.StringIO(
                    response.content.decode("utf-8").replace("\0", "<NULL BYTE>")
                )
            )
        )
        for row in rows[1:]:
            yield {
                key: value.replace("<NULL BYTE>", "\0")
                for key, value in zip(rows[0], row)
            }
        locator = response.headers["Sforce-Locator"]
        if locator == "null":
            logger.debug("Reached end of results")
            break


def main() -> None:
    salesforce = Salesforce(
        username=input("username: "),
        password=input("password: "),
        security_token=input("security_token: "),
        domain=input("domain: "),
    )

    # Read integration erros in batches of 100,000 records
    # and delete in batches of 10,000 records
    while True:
        buffer = []
        query_was_empty = True
        for record in bulk_v2_query(
            salesforce,
            query=" ".join(
                [
                    "SELECT Id, CreatedDate, KicksawEng__IntegrationExecution__c",
                    "FROM KicksawEng__IntegrationError__c",
                    "WHERE CreatedDate < LAST_N_MONTHS:4",
                    "ORDER BY CreatedDate ASC",
                    "LIMIT 100000",
                ]
            ),
        ):
            query_was_empty = False
            buffer.append(record)
            if len(buffer) == 10_000:
                response = salesforce.bulk.KicksawEng__IntegrationError__c.delete(
                    [{"Id": record["Id"]} for record in buffer]
                )
                assert all(
                    record_["success"] for record_ in response
                ), "Failed to delete records"
                buffer = []
        if len(buffer) > 0:
            response = salesforce.bulk.KicksawEng__IntegrationError__c.delete(
                [{"Id": record["Id"]} for record in buffer]
            )
            assert all(
                record_["success"] for record_ in response
            ), "Failed to delete records"
        if query_was_empty:
            logger.info("No more records to process")
            break


if __name__ == "__main__":
    main()
