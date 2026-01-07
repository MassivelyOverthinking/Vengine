# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import pandas as pd
import polars as pl
import pyarrow as pa

from typing import List, Union, Optional, Tuple, Any
from abc import ABC, abstractmethod
from dataclasses import field
from datetime import datetime, timezone

from src.reader import BaseReader
from src.utility.typings import DataTable, InputType

# ---------------------------------------------------------------
# EXCELREADER CLASS
# ---------------------------------------------------------------

class ExcelReader(BaseReader):

    __slots__ = ()

    def __init__(self, metadata = True):
        super().__init__(metadata)

    def _read_raw(self, input: InputType, engine: str = "pandas") -> DataTable:
        super()._read_raw(input, engine)

        match engine:
            case "pandas":
                df = pd.read_excel(input)
            case "polars":
                df = pl.read_excel(input)
            case "pyarrow":
                df = pa.xlsx.read_excel(input)

        return df