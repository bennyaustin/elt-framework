"""Maps pyodbc / SQL errors raised by routers into consistent HTTP error responses."""
import logging

import pyodbc
from fastapi import Request, status
from fastapi.responses import JSONResponse

logger = logging.getLogger("elt_api.errors")


async def pyodbc_error_handler(request: Request, exc: pyodbc.Error) -> JSONResponse:
    sqlstate = exc.args[0] if exc.args else None
    logger.error("Database error on %s %s: %s", request.method, request.url.path, exc)

    # 23000 class = integrity constraint violation (duplicate key, FK, etc.)
    if isinstance(sqlstate, str) and sqlstate.startswith("23"):
        status_code = status.HTTP_409_CONFLICT
    # 08xxx = connection errors
    elif isinstance(sqlstate, str) and sqlstate.startswith("08"):
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    else:
        status_code = status.HTTP_502_BAD_GATEWAY

    return JSONResponse(
        status_code=status_code,
        content={"detail": "Database operation failed", "sqlstate": sqlstate},
    )
