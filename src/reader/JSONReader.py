# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import pandas as pd
import polars as pl
import pyarrow as pa
import json

from typing import List, Optional, Any

from src.reader import BaseReader
from src.utility.typings import DataTable, InputType

# ---------------------------------------------------------------
# JSONREADER CLASS
# ---------------------------------------------------------------

class JSONReader(BaseReader):

    __slots__ = (
        "orient",
        "lines",
        "columns",
        "data_types",
        "n_rows",
    )

    def __init__(
        self,
        *,
        orient: str = "records",
        lines: bool = False,
        columns: Optional[List[str]] = None,
        data_types: Optional[dict[str, Any]] = None,
        n_rows: Optional[int] = None,
        **base_kwargs,
    ):
        super().__init__(**base_kwargs)

        self.orient = orient
        self.lines = lines
        self.columns = columns
        self.data_types = data_types
        self.n_rows = n_rows

    def _read_raw(self, input: InputType, engine: str = "pandas") -> DataTable:
        if engine == "pandas":
            df = pd.read_json(
                input,
                orient=self.orient,
                lines=self.lines,
                dtype=self.data_types,
                nrows=self.n_rows,
            )
            if self.columns:
                df = df[self.columns]
        elif engine == "polars":
            df = pl.read_json(
                input,
                read_json_lines=self.lines,
                n_rows=self.n_rows,
            )
            if self.columns:
                df = df.select(self.columns)
        elif engine == "pyarrow":
            with open(input, 'r') as file:
                data = json.load(file)
            
            df = pa.Table.from_pydict(data)
        else:
            self.logger.error(f"Unsupported engine: {engine}")
            raise ValueError(f"Unsupported engine: {engine}")

        return df