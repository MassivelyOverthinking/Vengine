# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import polars as pl

from typing import List, Optional, Tuple, Any

from src.reader import BaseReader
from src.typings import ReaderConfig, InputType

# ---------------------------------------------------------------
# PARQUETREADER CLASS
# ---------------------------------------------------------------

class ParquetReader(BaseReader):

    __slots__ = (
        "n_rows",
        "row_index_name",
        "rechunk",
        "low_memory",
    )

    def __init__(
        self,
        *,
        n_rows: Optional[int] = None,
        row_index_name: Optional[str] = None,
        rechunk: bool = False,
        low_memory: bool = False,
        verbosity: int = 0,
        **base_kwargs,
    ):
        super().__init__(verbosity=verbosity, **base_kwargs)

        self.n_rows = n_rows
        self.row_index_name = row_index_name
        self.rechunk = rechunk
        self.low_memory = low_memory

    def _materialize_config(self) -> ReaderConfig:
        return ReaderConfig(
            parameters={
                "n_rows": self.n_rows,
                "row_index_name": self.row_index_name,
                "rechunk": self.rechunk,
                "low_memory": self.low_memory,
            }
        )
    
    def _discover_schema(self, input: InputType) -> pl.Schema:
        
        lf = pl.scan_parquet(
            source=input,
            n_rows=self.n_rows,
            row_index_name=self.row_index_name,
            rechunk=self.rechunk,
            low_memory=self.low_memory,
        )

        schema = lf.schema
        self._logger(f"Schema initialized: {schema}")

        return schema
    
    def _to_lazyframe(self, input: InputType) -> pl.LazyFrame:
        
        lf = pl.scan_parquet(
            source=input,
            n_rows=self.n_rows,
            row_index_name=self.row_index_name,
            rechunk=self.rechunk,
            low_memory=self.low_memory,
        )

        self._logger(f"Data succesfully loaded into LazyFrame.")
        return lf

