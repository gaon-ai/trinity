"""
Client SQL Server Connection Utilities

Connects to on-prem SQL Server via site-to-site VPN.
Uses Airflow Connection for secure credential storage (encrypted with Fernet key).
"""
import pymssql
from contextlib import contextmanager
from typing import List, Dict, Any, Optional

from airflow.hooks.base import BaseHook


# Airflow Connection ID for client SQL Server
CLIENT_SQLSERVER_CONN_ID = 'client_sqlserver'


def _get_connection_details() -> dict:
    """
    Retrieve connection details from Airflow Connection.

    Returns:
        dict with host, port, login, password, schema

    Raises:
        ValueError: If connection not found or incomplete
    """
    try:
        conn = BaseHook.get_connection(CLIENT_SQLSERVER_CONN_ID)
    except Exception as e:
        raise ValueError(
            f"Airflow Connection '{CLIENT_SQLSERVER_CONN_ID}' not found. "
            "Create it via: airflow connections add client_sqlserver --conn-type mssql ..."
        ) from e

    if not conn.host or not conn.login or not conn.password:
        raise ValueError(
            f"Airflow Connection '{CLIENT_SQLSERVER_CONN_ID}' is incomplete. "
            "Ensure host, login, password, and schema are set."
        )

    return {
        'server': conn.host,
        'port': conn.port or 1433,
        'user': conn.login,
        'password': conn.password,
        'database': conn.schema,
    }


def get_client_db_connection() -> pymssql.Connection:
    """
    Create connection to client SQL Server.

    Credentials are retrieved from Airflow Connection (encrypted storage).

    Returns:
        pymssql.Connection: Active database connection

    Raises:
        ValueError: If Airflow Connection not configured
    """
    details = _get_connection_details()

    return pymssql.connect(
        server=details['server'],
        port=details['port'],
        user=details['user'],
        password=details['password'],
        database=details['database'],
        login_timeout=30,
        timeout=60
    )


def get_connection_info() -> dict:
    """
    Get connection info (without password) for logging/display.

    Returns:
        dict with server, port, database (no password)
    """
    details = _get_connection_details()
    return {
        'server': details['server'],
        'port': details['port'],
        'database': details['database'],
        'user': details['user'],
    }


@contextmanager
def client_db_cursor(as_dict: bool = True):
    """
    Context manager for safe cursor handling.

    Automatically handles connection and cursor cleanup.

    Args:
        as_dict: If True, return rows as dictionaries

    Yields:
        pymssql.Cursor: Database cursor

    Example:
        >>> with client_db_cursor() as cursor:
        ...     cursor.execute("SELECT TOP 5 * FROM [dbo].[inv_item_mst]")
        ...     rows = cursor.fetchall()
    """
    conn = get_client_db_connection()
    try:
        cursor = conn.cursor(as_dict=as_dict)
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def test_connection() -> bool:
    """
    Test connectivity to client SQL Server.

    Returns:
        True if connection successful

    Raises:
        Exception: If connection fails
    """
    with client_db_cursor() as cursor:
        cursor.execute("SELECT 1 AS test")
        result = cursor.fetchone()
        return result is not None


def fetch_query(
    query: str,
    params: tuple = None,
    as_dict: bool = True
) -> List[Dict[str, Any]]:
    """
    Execute query and return all results.

    Args:
        query: SQL query to execute
        params: Query parameters (optional)
        as_dict: If True, return rows as dictionaries

    Returns:
        List of rows (as dicts if as_dict=True)

    Example:
        >>> results = fetch_query("SELECT TOP 10 * FROM [dbo].[inv_item_mst]")
        >>> print(f"Got {len(results)} rows")
    """
    with client_db_cursor(as_dict=as_dict) as cursor:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor.fetchall()


def fetch_one(
    query: str,
    params: tuple = None,
    as_dict: bool = True
) -> Optional[Dict[str, Any]]:
    """
    Execute query and return first result.

    Args:
        query: SQL query to execute
        params: Query parameters (optional)
        as_dict: If True, return row as dictionary

    Returns:
        Single row or None if no results
    """
    with client_db_cursor(as_dict=as_dict) as cursor:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor.fetchone()


def execute_query(query: str, params: tuple = None) -> int:
    """
    Execute query and return rows affected.

    Use for INSERT, UPDATE, DELETE operations.

    Args:
        query: SQL query to execute
        params: Query parameters (optional)

    Returns:
        Number of rows affected
    """
    with client_db_cursor(as_dict=False) as cursor:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor.rowcount


def get_table_schema(table_name: str, schema: str = 'dbo') -> List[Dict[str, Any]]:
    """
    Get column information for a table.

    Args:
        table_name: Name of the table
        schema: Schema name (default: dbo)

    Returns:
        List of column definitions
    """
    query = """
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            IS_NULLABLE,
            COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """
    return fetch_query(query, (schema, table_name))


def get_row_count(table_name: str, schema: str = 'dbo') -> int:
    """
    Get approximate row count for a table.

    Args:
        table_name: Name of the table
        schema: Schema name (default: dbo)

    Returns:
        Approximate row count
    """
    # Use sys.dm_db_partition_stats for fast approximate count
    query = """
        SELECT SUM(p.rows) AS row_count
        FROM sys.partitions p
        JOIN sys.tables t ON p.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = %s AND t.name = %s AND p.index_id IN (0, 1)
    """
    result = fetch_one(query, (schema, table_name))
    return int(result['row_count']) if result and result['row_count'] else 0
