from datetime import datetime


def test_get_ingest_definition(client, mocker):
    fake_row = {
        "IngestID": 1,
        "SourceSystemName": "CRM",
        "StreamName": "Customers",
        "Backend": None,
        "EntityName": "dbo.Customer",
        "WatermarkColName": "ModifiedDate",
        "LastDeltaDate": datetime(2024, 1, 1),
        "DataFromTimestamp": datetime(2024, 1, 1),
        "DataToTimestamp": datetime(2024, 1, 2),
        "LastDeltaNumber": None,
        "DataFromNumber": None,
        "DataToNumber": None,
        "DataFormat": "parquet",
        "SourceStructure": None,
        "MaxIntervalMinutes": 60,
        "MaxIntervalNumber": None,
        "DataMapping": None,
        "RunSequence": 1,
        "ActiveFlag": True,
        "L1TransformationReqdFlag": True,
        "L2TransformationReqdFlag": False,
        "DelayL1TransformationFlag": False,
        "DelayL2TransformationFlag": False,
        "DestinationRawFileSystem": "raw",
        "DestinationRawFolder": "crm/customers",
        "DestinationRawFile": "customers.parquet",
        "DestinationRawTable": None,
        "SourceSQL": "SELECT * FROM dbo.Customer",
        "StatSQL": "SELECT COUNT(*) FROM dbo.Customer",
        "ReloadFlag": False,
        "ADFPipelineRunID": None,
    }
    mock_execute = mocker.patch(
        "app.routers.ingest_definitions.execute_proc", return_value=[fake_row]
    )

    response = client.get(
        "/api/v1/ingest/definitions",
        params={"source_system_name": "CRM", "stream_name": "Customers", "max_ingest_instance": 5},
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["IngestID"] == 1
    mock_execute.assert_called_once_with(
        "ELT",
        "GetIngestDefinition",
        {"SourceSystemName": "CRM", "StreamName": "Customers", "MaxIngestInstance": 5},
    )


def test_update_ingest_definition(client, mocker):
    mock_execute = mocker.patch("app.routers.ingest_definitions.execute_proc", return_value=[])

    response = client.patch(
        "/api/v1/ingest/definitions/42",
        json={"IngestStatus": "Success", "ReloadFlag": False},
    )

    assert response.status_code == 204
    mock_execute.assert_called_once()
    args, _ = mock_execute.call_args
    assert args[0] == "ELT"
    assert args[1] == "UpdateIngestDefinition"
    assert args[2]["IngestID"] == 42
    assert args[2]["IngestStatus"] == "Success"
