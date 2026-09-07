# ELT Framework ControlDB API

A FastAPI service that exposes the ControlDB stored procedures (`elt-framework/ControlDB/ELT/Stored Procedures`)
as a REST API, so orchestrators (Azure Data Factory, Microsoft Fabric pipelines, Azure Databricks jobs, etc.)
can call ControlDB metadata operations over HTTP instead of executing T-SQL directly.

## Features
- One REST endpoint per stored procedure, grouped under `ingest` and `transform` resources.
- Pydantic request/response models mirroring each stored procedure's parameters and result columns.
- Azure AD (Entra ID) bearer-token authentication — validates tokens issued to calling service principals /
  managed identities.
- `pyodbc` based data access layer with a generic stored-procedure executor.
- Auto-generated OpenAPI docs at `/docs`.

## Project layout
```
api/
  app/
    main.py                 # FastAPI app + router registration
    config.py                # Settings (env vars)
    db/
      session.py             # pyodbc connection factory (AAD token or SQL auth)
      executor.py             # Generic "call stored procedure" helper
    auth/
      aad.py                   # Azure AD bearer token validation dependency
    models/
      ingest.py                 # Pydantic schemas for Ingest* procedures
      transform_l1.py            # Pydantic schemas for L1Transform* procedures
      transform_l2.py            # Pydantic schemas for L2Transform* procedures
    routers/
      ingest_definitions.py
      ingest_instances.py
      transform_definitions.py
      transform_instances.py
    middleware/
      errors.py               # Maps pyodbc/SQL errors to HTTP responses
  tests/
  Dockerfile
  requirements.txt
  requirements-dev.txt
  .env.example
```

## Configuration
Copy `.env.example` to `.env` and fill in values:

| Variable | Description |
|---|---|
| `SQL_SERVER` | ControlDB server hostname |
| `SQL_DATABASE` | Database name (default `controlDB`) |
| `SQL_AUTH_MODE` | `ServicePrincipal` (default, uses `AAD_CLIENT_ID`/`AAD_CLIENT_SECRET`/`AAD_TENANT_ID`) or `ManagedIdentity` |
| `AAD_TENANT_ID` | Entra tenant ID |
| `AAD_CLIENT_ID` | Service principal client ID (DB auth) |
| `AAD_CLIENT_SECRET` | Service principal secret (DB auth) |
| `API_AAD_TENANT_ID` | Entra tenant ID that issues caller tokens (usually same tenant) |
| `API_AAD_AUDIENCE` | Expected `aud` claim / App ID URI for this API's app registration |
| `API_AAD_ALLOWED_APP_IDS` | Comma-separated list of allowed caller `appid`/`azp` claims |

## Running locally
```powershell
cd api
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt -r requirements-dev.txt
copy .env.example .env   # then edit values
uvicorn app.main:app --reload
```
Open http://127.0.0.1:8000/docs for interactive API docs.

## Testing
```powershell
pytest
```

## Docker
```powershell
docker build -t elt-controldb-api .
docker run -p 8000:8000 --env-file .env elt-controldb-api
```

## Endpoint map
| Stored Procedure | Method & Route |
|---|---|
| `ELT.GetIngestDefinition` | `GET /api/v1/ingest/definitions` |
| `ELT.GetIngestDefinition_FileDrop` | `GET /api/v1/ingest/definitions/file-drop` |
| `ELT.UpdateIngestDefinition` | `PATCH /api/v1/ingest/definitions/{ingest_id}` |
| `ELT.InsertIngestInstance` | `POST /api/v1/ingest/instances` |
| `ELT.UpdateIngestInstance` | `PATCH /api/v1/ingest/instances/{adf_ingest_pipeline_run_id}` |
| `ELT.GetTransformDefinition_L1` | `GET /api/v1/transform/definitions/l1` |
| `ELT.GetTransformDefinition_L2` | `GET /api/v1/transform/definitions/l2` |
| `ELT.UpdateTransformDefinition_L2` | `PATCH /api/v1/transform/definitions/l2/{l2_transform_id}` |
| `ELT.GetTransformInstance_L1` | `GET /api/v1/transform/instances/l1` |
| `ELT.GetTransformInstance_L2` | `GET /api/v1/transform/instances/l2` |
| `ELT.InsertTransformInstance_L1` | `POST /api/v1/transform/instances/l1` |
| `ELT.InsertTransformInstance_L2` | `POST /api/v1/transform/instances/l2` |
| `ELT.UpdateTransformInstance_L1` | `PATCH /api/v1/transform/instances/l1/{l1_transform_instance_id}` |
| `ELT.UpdateTransformInstance_L2` | `PATCH /api/v1/transform/instances/l2/{l2_transform_instance_id}` |
| `ELT.GetRunningTransformInstances` | `GET /api/v1/transform/instances/running` |
