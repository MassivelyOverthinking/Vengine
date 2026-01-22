# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import polars as pl

from dataclasses import dataclass
from io import StringIO, BytesIO
from os import PathLike
from typing import Any, Mapping, Union, Tuple, Hashable, Dict

# ---------------------------------------------------------------
# CUSTOM DATA TYPES
# ---------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ReaderConfig:
    parameters: Mapping[str, Any]

@dataclass(frozen=True, slots=True)
class ReaderResult:
    frame: pl.LazyFrame
    schema: pl.Schema
    metadata: Dict[str, Any]

@dataclass(frozen=True, slots=True)
class ReaderPlan:
    return_type: type
    schema: Tuple[Tuple[str, pl.DataType], ...]
    fingerprint: Hashable

@dataclass(frozen=True, slots=True)
class ReaderSchema:
    schema: Dict[str, pl.DataType]

InputType = Union[str, PathLike, StringIO, BytesIO]

