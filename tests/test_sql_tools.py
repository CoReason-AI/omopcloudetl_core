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
import pytest
from jinja2 import UndefinedError
from omopcloudetl_core.sql_tools import (
    render_jinja_template,
    apply_query_tag,
    split_sql_script,
)


def test_render_jinja_template_success():
    """Test successful rendering of a Jinja2 template."""
    template = "SELECT * FROM {{ table }} WHERE id = {{ id }};"
    context = {"table": "my_table", "id": 123}
    rendered = render_jinja_template(template, context)
    assert rendered == "SELECT * FROM my_table WHERE id = 123;"


def test_render_jinja_template_strict_undefined():
    """Test that rendering fails if a variable is missing in the context."""
    template = "SELECT * FROM {{ table }};"
    context = {}  # 'table' is missing
    with pytest.raises(UndefinedError):
        render_jinja_template(template, context)


def test_apply_query_tag():
    """Test that the query tag is correctly applied as a JSON comment."""
    sql = "SELECT 1;"
    context = {"workflow": "my_etl", "step": "step1"}
    tagged_sql = apply_query_tag(sql, context)

    # Extract the JSON part for validation
    comment = tagged_sql.split('\n')[0]
    assert comment.startswith("/* OmopCloudEtlContext:")
    assert comment.endswith("*/")

    json_str = comment.replace("/* OmopCloudEtlContext: ", "").replace(" */", "")
    tag_data = json.loads(json_str)

    assert tag_data == context
    assert tagged_sql.endswith(sql)


def test_split_sql_script():
    """Test splitting a script with multiple SQL statements."""
    script = """
    -- First statement
    CREATE TABLE my_table (id INT);

    -- Second statement
    INSERT INTO my_table VALUES (1);

    -- Empty statements should be ignored
    ;

    SELECT * FROM my_table;
    """
    statements = split_sql_script(script)
    assert len(statements) == 3
    assert statements[0] == "CREATE TABLE my_table (id INT);"
    assert statements[1] == "INSERT INTO my_table VALUES (1);"
    assert statements[2] == "SELECT * FROM my_table;"


def test_split_sql_script_single_statement():
    """Test splitting a script with a single statement."""
    script = "SELECT 1;"
    statements = split_sql_script(script)
    assert len(statements) == 1
    assert statements[0] == "SELECT 1;"


def test_split_sql_script_empty_script():
    """Test that an empty script results in an empty list."""
    script = ""
    statements = split_sql_script(script)
    assert len(statements) == 0
