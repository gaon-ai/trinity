"""
SQL Templates Loader Module for Trinity ETL pipelines.

This module provides a Jinja2-based SQL templating system with built-in
SQL injection prevention and database-specific quoting utilities.

Usage Examples:
--------------

1. Basic template rendering:

    from lib.sql_templates import render_sql

    # Render a template from plugins/sql/sqlserver/bronze/incremental.sql.j2
    sql = render_sql('sqlserver/bronze/incremental.sql.j2',
                     schema='dbo',
                     table='inv_item_mst',
                     timestamp_column='lastupdated',
                     last_sync_value='2025-01-15')

2. Using custom filters in templates:

    -- SQL Server identifier quoting with brackets
    SELECT * FROM [{{ schema | quote_sqlserver }}].[{{ table | quote_sqlserver }}]

    -- Validate identifiers (raises error if invalid)
    SELECT * FROM [{{ schema | validate_id }}].[{{ table | validate_id }}]

3. Template variables with StrictUndefined:

    -- All variables must be provided, or an error is raised
    -- This prevents silent failures from typos
    SELECT * FROM [{{ schema }}].[{{ tabel }}]  -- Raises UndefinedError for 'tabel'

"""
import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

from jinja2 import Environment, FileSystemLoader, StrictUndefined, TemplateNotFound

logger = logging.getLogger(__name__)

# SQL directory relative to this file (plugins/sql/)
SQL_DIR = Path(__file__).parent.parent / "sql"


class IdentifierFilter:
    """
    SQL identifier validator to prevent SQL injection.

    Only allows alphanumeric characters, underscores, and hyphens.
    This is intentionally restrictive to prevent injection attacks.
    """

    # Pattern for valid SQL identifiers: letters, numbers, underscores, hyphens
    VALID_PATTERN = re.compile(r'^[a-zA-Z0-9_-]+$')

    @classmethod
    def validate(cls, identifier: str) -> str:
        """
        Validate that an identifier contains only safe characters.

        Args:
            identifier: The SQL identifier to validate

        Returns:
            The validated identifier (unchanged if valid)

        Raises:
            ValueError: If the identifier contains invalid characters
        """
        if not identifier:
            raise ValueError("Identifier cannot be empty")

        if not isinstance(identifier, str):
            raise ValueError(f"Identifier must be a string, got {type(identifier).__name__}")

        if not cls.VALID_PATTERN.match(identifier):
            raise ValueError(
                f"Invalid SQL identifier: '{identifier}'. "
                f"Only alphanumeric characters, underscores, and hyphens are allowed."
            )

        return identifier


def quote_sqlserver(identifier: str) -> str:
    """
    Quote an identifier for SQL Server using brackets.

    This function validates the identifier first to prevent SQL injection,
    then wraps it in brackets for SQL Server compatibility.

    Args:
        identifier: The SQL identifier to quote

    Returns:
        The quoted identifier (e.g., [my_table])

    Example:
        {{ table_name | quote_sqlserver }}  ->  [customers]
    """
    validated = IdentifierFilter.validate(identifier)
    return f'[{validated}]'


def validate_id(identifier: str) -> str:
    """
    Validate an identifier without quoting.

    Use this when you need to validate an identifier but don't want
    it to be quoted (e.g., for dynamic SQL or specific database dialects).

    Args:
        identifier: The SQL identifier to validate

    Returns:
        The validated identifier (unchanged)

    Example:
        {{ schema | validate_id }}.{{ table | validate_id }}  ->  dbo.customers
    """
    return IdentifierFilter.validate(identifier)


class SQLTemplates:
    """
    SQL template loader with Jinja2 environment and security features.

    This class provides a secure way to load and render SQL templates with:
    - SQL injection prevention via identifier validation
    - Database-specific quoting filters
    - Strict undefined variable checking
    - LRU caching for performance

    Attributes:
        env: The Jinja2 Environment configured for SQL templating
        sql_dir: Path to the SQL templates directory
    """

    def __init__(self, sql_dir: Optional[Path] = None):
        """
        Initialize the SQL templates loader.

        Args:
            sql_dir: Path to SQL templates directory. Defaults to plugins/sql/
        """
        self.sql_dir = sql_dir or SQL_DIR

        if not self.sql_dir.exists():
            logger.warning(f"SQL templates directory does not exist: {self.sql_dir}")

        self.env = Environment(
            loader=FileSystemLoader(str(self.sql_dir)),
            undefined=StrictUndefined,
            autoescape=False,  # SQL templates should not be HTML-escaped
            trim_blocks=True,
            lstrip_blocks=True,
        )

        # Register custom filters for SQL Server
        self.env.filters['quote_sqlserver'] = quote_sqlserver
        self.env.filters['validate_id'] = validate_id
        # Alias for convenience
        self.env.filters['q'] = quote_sqlserver

    @lru_cache(maxsize=128)
    def _load_template(self, template_path: str):
        """
        Load a template with LRU caching.

        Args:
            template_path: Relative path to the template from sql_dir

        Returns:
            The loaded Jinja2 template

        Raises:
            TemplateNotFound: If the template file doesn't exist
        """
        return self.env.get_template(template_path)

    def render(self, template_path: str, **kwargs: Any) -> str:
        """
        Render a SQL template with the given variables.

        Args:
            template_path: Relative path to the template from sql_dir
                          (e.g., 'sqlserver/bronze/incremental.sql.j2')
            **kwargs: Template variables to substitute

        Returns:
            The rendered SQL string

        Raises:
            TemplateNotFound: If the template file doesn't exist
            jinja2.UndefinedError: If a required variable is missing
            ValueError: If an identifier validation fails

        Example:
            sql = templates.render('sqlserver/bronze/incremental.sql.j2',
                                   schema='dbo',
                                   table='inv_item_mst')
        """
        template = self._load_template(template_path)
        return template.render(**kwargs)

    def render_string(self, template_string: str, **kwargs: Any) -> str:
        """
        Render a SQL template from a string.

        This is useful for ad-hoc templates that aren't stored in files.

        Args:
            template_string: The SQL template as a string
            **kwargs: Template variables to substitute

        Returns:
            The rendered SQL string

        Example:
            sql = templates.render_string(
                'SELECT * FROM [{{ schema | validate_id }}].[{{ table | validate_id }}]',
                schema='dbo',
                table='inv_item_mst'
            )
        """
        template = self.env.from_string(template_string)
        return template.render(**kwargs)

    def clear_cache(self) -> None:
        """Clear the template loading cache."""
        self._load_template.cache_clear()


# Singleton instance
_sql_templates_instance: Optional[SQLTemplates] = None


def get_sql_templates() -> SQLTemplates:
    """
    Get the singleton SQLTemplates instance.

    This function provides a singleton accessor for the SQLTemplates class,
    ensuring that template caching is shared across all callers.

    Returns:
        The shared SQLTemplates instance

    Example:
        templates = get_sql_templates()
        sql = templates.render('sqlserver/bronze/full_refresh.sql.j2', table='customers')
    """
    global _sql_templates_instance
    if _sql_templates_instance is None:
        _sql_templates_instance = SQLTemplates()
    return _sql_templates_instance


def render_sql(template_path: str, **kwargs: Any) -> str:
    """
    Convenience function to render a SQL template.

    This is a shorthand for get_sql_templates().render(...).

    Args:
        template_path: Relative path to the template from plugins/sql/
                      (e.g., 'sqlserver/bronze/incremental.sql.j2')
        **kwargs: Template variables to substitute

    Returns:
        The rendered SQL string

    Example:
        from lib.sql_templates import render_sql

        sql = render_sql('sqlserver/bronze/incremental.sql.j2',
                         schema='dbo',
                         table='inv_item_mst',
                         timestamp_column='lastupdated',
                         last_sync_value='2025-01-15 10:30:00')
    """
    return get_sql_templates().render(template_path, **kwargs)
