# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloud_core

import pytest
from omopcloudetl_core.sql_tools import split_sql_script


@pytest.mark.parametrize(
    "sql_input, expected",
    [
        ("", []),
        ("   ", []),
        ("\n\t\r", []),
        ("-- a comment", []),
        ("/* a block comment */", []),
        ("--;", []),
        ("/*;*/", []),
        ("SELECT 1; SELECT 2", ["SELECT 1", "SELECT 2"]),
        ("SELECT 'hello;world';", ["SELECT 'hello;world'"]),
        ("SELECT `hello;world`;", ["SELECT `hello;world`"]),
        ('SELECT "hello;world";', ['SELECT "hello;world"']),
        ("SELECT 1;; SELECT 2;", ["SELECT 1", "SELECT 2"]),
        ("SELECT 1;\n--comment\nSELECT 2;", ["SELECT 1", "SELECT 2"]),
        ("SELECT 1; /* comment */ SELECT 2;", ["SELECT 1", "SELECT 2"]),
        (
            "CREATE TEMP FUNCTION a(x STRING) AS (x); SELECT a(';');",
            ["CREATE TEMP FUNCTION a(x STRING) AS (x)", "SELECT a(';')"],
        ),
        (
            "SELECT $$hello;world$$;",
            ["SELECT $$hello;world$$"],
        ),
        (
            "SELECT $tag$hello;world$tag$;",
            ["SELECT $tag$hello;world$tag$"],
        ),
        (
            "SELECT 1; -- statement;\nSELECT 2;",
            ["SELECT 1", "SELECT 2"],
        ),
        (
            "SELECT 1 /* comment; */ ; SELECT 2",
            ["SELECT 1", "SELECT 2"],
        ),
        (
            "/*\n  multi-line comment;\n*/\nSELECT 1;",
            ["SELECT 1"],
        ),
        (
            "SELECT 'incomplete string",
            ["SELECT 'incomplete string"],
        ),
    ],
)
def test_split_sql_script_edge_cases(sql_input, expected):
    assert split_sql_script(sql_input) == expected
