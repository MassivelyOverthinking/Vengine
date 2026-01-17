# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import polars as pl

from dataclasses import dataclass
from io import StringIO, BytesIO
from os import PathLike
from typing import Any, Mapping, Union

# ---------------------------------------------------------------
# CUSTOM DATA TYPES
# ---------------------------------------------------------------

@dataclass(frozen=True)
class ReaderConfig:
    parameters: Mapping[str, Any]

InputType = Union[str, PathLike, StringIO, BytesIO]

