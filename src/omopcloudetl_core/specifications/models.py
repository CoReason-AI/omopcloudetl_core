# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any


class CDMFieldSpec(BaseModel):
    name: str
    type: str
    required: bool
    description: Optional[str] = None


class CDMTableSpec(BaseModel):
    name: str
    fields: List[CDMFieldSpec]
    primary_key: List[str]
    optimizations: Dict[str, Dict[str, Any]] = Field(default_factory=dict)


class CDMSpecification(BaseModel):
    version: str
    tables: Dict[str, CDMTableSpec]
