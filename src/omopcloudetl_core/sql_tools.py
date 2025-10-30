# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

import json
from typing import Any, Dict, List

import sqlparse
from jinja2 import Environment, StrictUndefined, exceptions

from omopcloudetl_core.exceptions import CompilationError


def render_jinja_template(template_str: str, context: Dict[str, Any]) -> str:
    """
    Renders a Jinja2 template with a given context.

    Args:
        template_str: The Jinja2 template string.
        context: The context dictionary to render the template with.

    Returns:
        The rendered string.
    """
    try:
        env = Environment(undefined=StrictUndefined)
        template = env.from_string(template_str)
        return template.render(context)
    except exceptions.UndefinedError as e:
        raise CompilationError(f"Jinja rendering failed: {e}") from e


def apply_query_tag(sql: str, context: Dict[str, str]) -> str:
    """
    Prepends a JSON-formatted comment block to a SQL statement for tracking.

    Args:
        sql: The SQL statement.
        context: The context dictionary to include in the tag.

    Returns:
        The SQL statement with the prepended query tag.
    """
    tag = f"/* OmopCloudEtlContext: {json.dumps(context)} */"
    return f"{tag}\n{sql}"


def split_sql_script(sql_script: str) -> List[str]:
    """
    Splits a SQL script into a list of individual, non-empty statements,
    stripping comments and trailing semicolons.

    Args:
        sql_script: A string containing one or more SQL statements.

    Returns:
        A list of individual SQL statements.
    """
    # First, remove comments from the script
    formatted_script = sqlparse.format(sql_script, strip_comments=True)
    statements = sqlparse.split(formatted_script)

    # Clean up statements
    cleaned_statements = []
    for stmt in statements:
        stripped_stmt = stmt.strip()
        # Remove trailing semicolon if it exists
        if stripped_stmt.endswith(";"):
            stripped_stmt = stripped_stmt[:-1]

        # Strip again to remove whitespace that may have been before a semicolon
        stripped_stmt = stripped_stmt.strip()

        if stripped_stmt:
            cleaned_statements.append(stripped_stmt)

    return cleaned_statements
