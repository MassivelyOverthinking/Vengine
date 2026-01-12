# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import pandas as pd
import polars as pl
import pyarrow as pa

from typing import List, Optional, Tuple, Any

from src.reader import BaseReader
from src.utility.typings import DataTable, InputType

# ---------------------------------------------------------------
# PARQUETREADER CLASS
# ---------------------------------------------------------------

class ParquetReader(BaseReader):

    __slots__ = (
        "columns",
        "use_threads",
        "engine",
        "filters",
        "n_rows",
    )

    def __init__(
        self,
        *,
        columns: Optional[List[str]] = None,
        use_threads: bool = True,
        engine: str = "pyarrow",
        filters: Optional[List[Tuple[str, str, Any]]] = None,
        n_rows: Optional[int] = None,
        **base_kwargs,
    ):
        super().__init__(**base_kwargs)

        self.columns = columns
        self.use_threads = use_threads
        self.engine = engine
        self.filters = filters
        self.n_rows = n_rows


    def _read_raw(self, input: InputType, engine: str = "pandas") -> DataTable:
        if engine == "pandas":
            df = pd.read_parquet(
                input,
                columns=self.columns,
                engine=self.engine,
                filters=self.filters,
                use_nullable_dtypes=True,
            )
            if self.n_rows:
                df = df.head(self.n_rows)
        elif engine == "polars":
            df = pl.read_parquet(
                input,
                columns=self.columns,
                use_threads=self.use_threads,
                filters=self.filters,
            )
        elif engine == "pyarrow":
            df = pa.parquet.read_table(
                input,
                columns=self.columns,
                use_threads=self.use_threads,
                filters=self.filters,
            )
        else:
            self.logger.error(f"Unsupported engine: {engine}")
            raise ValueError(f"Unsupported engine: {engine}")

        return df