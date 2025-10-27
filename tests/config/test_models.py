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
from unittest.mock import patch
from pydantic import ValidationError
from omopcloudetl_core.config.models import ConnectionConfig, ProjectConfig


def test_connection_config_env_vars():
    with patch.dict(
        "os.environ",
        {
            "OMOPCLOUDETL_CONN_PROVIDER_TYPE": "test_provider",
            "OMOPCLOUDETL_CONN_HOST": "localhost",
            "OMOPCLOUDETL_CONN_USER": "test_user",
        },
    ):
        config = ConnectionConfig()
        assert config.provider_type == "test_provider"
        assert config.host == "localhost"
        assert config.user == "test_user"


def test_project_config_validation():
    with pytest.raises(ValidationError):
        ProjectConfig(connection={"provider_type": "test"}, orchestrator={"type": "test"}, schemas="not_a_dict")

    # This should pass
    ProjectConfig(connection={"provider_type": "test"}, orchestrator={"type": "test"}, schemas={"source": "my_schema"})
