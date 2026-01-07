# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import pandas as pd
import polars as pl
import pyarrow as pa
import json

from typing import List, Union, Optional, Tuple, Any
from abc import ABC, abstractmethod
from dataclasses import field
from datetime import datetime, timezone

from src.reader import BaseReader
from src.utility.typings import DataTable, InputType

# ---------------------------------------------------------------
# JSONREADER CLASS
# ---------------------------------------------------------------

class JSONReader(BaseReader):

    __slots__ = ()

    def __init__(self, metadata = True):
        super().__init__(metadata)

    def _read_raw(self, input: InputType, engine: str = "pandas") -> DataTable:
        super()._read_raw(input, engine)

        match engine:
            case "pandas":
                df = pd.read_json(input)
            case "polars":
                df = pl.read_json(input)
            case "pyarrow":
                with open(input, 'r') as f:
                    data = json.load(f)
                df = pa.Table.from_pydict(data)

        return df