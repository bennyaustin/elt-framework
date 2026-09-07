"""Generic stored-procedure execution helper.

Wraps pyodbc's `{CALL schema.proc (?, ?, ...)}` syntax so routers only need to supply the
procedure name and an ordered dict of parameters; result sets (if any) are returned as a
list of plain dicts keyed by column name.
"""
from typing import Any, Mapping, Sequence

import pyodbc

from app.db.session import get_connection


def _placeholders(count: int) -> str:
    return ", ".join(["?"] * count)


def execute_proc(
    schema: str,
    proc_name: str,
    params: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Execute a stored procedure and return the first result set as a list of dicts.

    Params are passed positionally (in insertion order) matching the proc's declared
    parameter order — callers (routers) are responsible for building `params` in the
    correct order using the proc's signature.
    """
    params = params or {}
    values: Sequence[Any] = list(params.values())
    call_sql = f"{{CALL {schema}.{proc_name} ({_placeholders(len(values))})}}"

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(call_sql, values)

        rows: list[dict[str, Any]] = []
        if cursor.description:
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

        conn.commit()
        return rows


def execute_proc_scalar(
    schema: str,
    proc_name: str,
    params: Mapping[str, Any] | None = None,
) -> Any:
    """Execute a stored procedure expected to return a single row/column and return that value."""
    rows = execute_proc(schema, proc_name, params)
    if not rows:
        return None
    first_row = rows[0]
    return next(iter(first_row.values()), None)


class StoredProcedureError(Exception):
    """Raised when a stored procedure call fails; wraps the underlying pyodbc error."""

    def __init__(self, proc_name: str, original: pyodbc.Error):
        self.proc_name = proc_name
        self.original = original
        super().__init__(f"Stored procedure {proc_name} failed: {original}")
