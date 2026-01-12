# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import pandas as pd
import polars as pl
import pyarrow as pa

from typing import List, Optional, Any

from src.reader import BaseReader
from src.utility.typings import DataTable, InputType

# ---------------------------------------------------------------
# FEATHERREADER CLASS
# ---------------------------------------------------------------

class FeatherReader(BaseReader):

    __slots__ = (
        "columns",
        "use_threads",
        "memory_map",
        "data_types",
        "n_rows",
    )

    def __init__(self,
        *,
        columns: Optional[List[str]] = None,
        use_threads: bool = True,
        memory_map: bool = False,
        data_types: Optional[dict[str, Any]] = None,
        n_rows: Optional[int] = None,
        **base_kwargs,
    ):
        super().__init__(**base_kwargs)

        self.columns = columns
        self.use_threads = use_threads
        self.memory_map = memory_map
        self.data_types = data_types
        self.n_rows = n_rows
        

    def _read_raw(self, input: InputType, engine: str = "pandas") -> DataTable:
        if engine == "pandas":
            df = pd.read_feather(
                input,
                columns=self.columns,
                use_threads=self.use_threads,
                memory_map=self.memory_map,
                nrows=self.n_rows,
                dtype=self.data_types,
            )
        elif engine == "polars":
            df = pl.read_ipc(
                input,
                columns=self.columns,
                n_rows=self.n_rows,
                use_threads=self.use_threads,
                memory_map=self.memory_map,
            )
        elif engine == "pyarrow":
            df = pa.feather.read_table(
                input,
                columns=self.columns,
                use_threads=self.use_threads,
                memory_map=self.memory_map,
            )
        else:
            self.logger.error(f"Unsupported engine: {engine}")
            raise ValueError(f"Unsupported engine: {engine}")

        return df