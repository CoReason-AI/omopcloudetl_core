# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omcloudetl_core

import pytest
from omopcloudetl_core.sql_tools import (
    render_jinja_template,
    apply_query_tag,
    split_sql_script,
)
from jinja2.exceptions import UndefinedError


def test_render_jinja_template_success():
    """Tests that a Jinja2 template is rendered correctly."""
    template = "SELECT * FROM {{ schemas.source }}.person;"
    context = {"schemas": {"source": "raw_data"}}
    rendered = render_jinja_template(template, context)
    assert rendered == "SELECT * FROM raw_data.person;"


def test_render_jinja_template_undefined_variable():
    """Tests that rendering fails if a variable is undefined."""
    template = "SELECT * FROM {{ schemas.undefined }};"
    context = {"schemas": {"source": "raw"}}
    with pytest.raises(UndefinedError):
        render_jinja_template(template, context)


def test_apply_query_tag():
    """Tests that a query tag is correctly prepended to a SQL statement."""
    sql = "SELECT 1;"
    context = {"step": "test", "id": "123"}
    tagged_sql = apply_query_tag(sql, context)
    assert "/* OmopCloudEtlContext:" in tagged_sql
    assert '"step": "test"' in tagged_sql
    assert sql in tagged_sql


def test_split_sql_script():
    """Tests that a SQL script is correctly split into individual statements."""
    script = "SELECT 1; SELECT 2; -- A comment\nSELECT 3;"
    statements = split_sql_script(script)
    assert len(statements) == 3
    assert statements[0] == "SELECT 1"
    assert statements[2] == "SELECT 3"
