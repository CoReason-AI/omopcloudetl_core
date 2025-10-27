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
from abc import ABC
from omopcloudetl_core.abstractions.connections import BaseConnection, ScalabilityTier

def test_base_connection_is_abc():
    """Tests that BaseConnection is an abstract base class."""
    assert issubclass(BaseConnection, ABC)

def test_scalability_tier_enum():
    """Tests that the ScalabilityTier enum has the correct values."""
    assert ScalabilityTier.TIER_1_HORIZONTAL.value == 1
    assert ScalabilityTier.TIER_3_SINGLE_NODE.value == 3
