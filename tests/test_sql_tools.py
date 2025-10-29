# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

import pytest
from omopcloudetl_core.exceptions import CompilationError

from omopcloudetl_core.sql_tools import (
    apply_query_tag,
    render_jinja_template,
    split_sql_script,
)


class TestRenderJinjaTemplate:
    def test_render_with_valid_context(self):
        template = "SELECT * FROM {{ schemas.source }}.my_table;"
        context = {"schemas": {"source": "analytics"}}
        expected = "SELECT * FROM analytics.my_table;"
        assert render_jinja_template(template, context) == expected

    def test_render_with_missing_variable_raises_error(self):
        template = "SELECT * FROM {{ schemas.missing }}.my_table;"
        context = {"schemas": {"source": "analytics"}}
        with pytest.raises(CompilationError):
            render_jinja_template(template, context)


class TestApplyQueryTag:
    def test_apply_query_tag(self):
        sql = "SELECT 1;"
        context = {"step": "test_step", "id": "123"}
        expected_tag = '/* OmopCloudEtlContext: {"step": "test_step", "id": "123"} */'
        tagged_sql = apply_query_tag(sql, context)
        assert tagged_sql.startswith(expected_tag)
        assert tagged_sql.endswith(sql)


class TestSplitSqlScript:
    def test_split_multiple_statements(self):
        script = """
            -- First statement
            CREATE TABLE my_table (id INT);
            -- Second statement
            INSERT INTO my_table (id) VALUES (1);
        """
        statements = split_sql_script(script)
        assert len(statements) == 2
        assert statements[0] == "CREATE TABLE my_table (id INT)"
        assert statements[1] == "INSERT INTO my_table (id) VALUES (1)"

    def test_split_with_empty_statements_and_comments(self):
        script = """
            SELECT 1; -- comment
            ;
            -- another comment
            SELECT 2;
        """
        statements = split_sql_script(script)
        assert len(statements) == 2
        assert statements[0] == "SELECT 1"
        assert statements[1] == "SELECT 2"

    def test_split_empty_script(self):
        script = ""
        statements = split_sql_script(script)
        assert len(statements) == 0

    def test_split_script_with_only_comments(self):
        script = "-- This is a comment"
        statements = split_sql_script(script)
        assert len(statements) == 0
