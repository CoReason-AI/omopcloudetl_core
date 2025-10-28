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
from jinja2 import Environment, StrictUndefined


def render_jinja_template(template_str: str, context: Dict[str, Any]) -> str:
    """
    Renders a Jinja2 template with a given context.

    Args:
        template_str: The Jinja2 template string.
        context: A dictionary of variables to use for rendering.

    Returns:
        The rendered string.
    """
    env = Environment(undefined=StrictUndefined)
    template = env.from_string(template_str)
    return template.render(context)


def apply_query_tag(sql: str, context: Dict[str, str]) -> str:
    """
    Prepends a SQL comment to a query with a JSON context.

    Args:
        sql: The SQL query string.
        context: A dictionary of key-value pairs for the tag.

    Returns:
        The SQL query with the prepended context tag.
    """
    # Sanitize context to ensure it's a flat dictionary of strings
    sanitized_context = {
        str(k): str(v) for k, v in context.items() if isinstance(v, (str, int, float, bool))
    }
    json_context = json.dumps(sanitized_context, sort_keys=True)
    return f"/* OmopCloudEtlContext: {json_context} */\n{sql}"


def split_sql_script(sql_script: str) -> List[str]:
    """
    Splits a multi-statement SQL script into a list of individual statements,
    stripping comments and empty statements.

    Args:
        sql_script: The SQL script string.

    Returns:
        A list of individual, non-empty SQL statements.
    """
    # Use sqlparse to remove comments
    filtered_script = sqlparse.format(sql_script, strip_comments=True)
    statements = sqlparse.split(filtered_script)
    return [stmt.strip() for stmt in statements if stmt.strip()]
