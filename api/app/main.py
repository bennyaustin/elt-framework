"""FastAPI application entrypoint for the ControlDB stored-procedure API."""
import logging

import pyodbc
from fastapi import FastAPI

from app.config import get_settings
from app.middleware.errors import pyodbc_error_handler
from app.routers import (
    ingest_definitions,
    ingest_instances,
    transform_definitions,
    transform_instances,
)

settings = get_settings()
logging.basicConfig(level=settings.log_level)

app = FastAPI(
    title="ELT Framework ControlDB API",
    description="REST API layer over the ControlDB (ELT schema) stored procedures.",
    version="0.1.0",
)

app.add_exception_handler(pyodbc.Error, pyodbc_error_handler)

API_PREFIX = "/api/v1"
app.include_router(ingest_definitions.router, prefix=API_PREFIX)
app.include_router(ingest_instances.router, prefix=API_PREFIX)
app.include_router(transform_definitions.router, prefix=API_PREFIX)
app.include_router(transform_instances.router, prefix=API_PREFIX)


@app.get("/health", tags=["health"])
def health() -> dict[str, str]:
    """Liveness probe — does not touch the database."""
    return {"status": "ok"}
