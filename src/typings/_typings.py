# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import polars as pl

from dataclasses import dataclass
from io import StringIO, BytesIO
from os import PathLike
from typing import Any, Mapping, Union, Tuple, Hashable

# ---------------------------------------------------------------
# CUSTOM DATA TYPES
# ---------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ReaderConfig:
    parameters: Mapping[str, Any]

@dataclass(frozen=True, slots=True)
class ReaderPlan:
    return_type: type
    schema: Tuple[Tuple[str, pl.DataType], ...]
    fingerprint: Hashable

InputType = Union[str, PathLike, StringIO, BytesIO]

