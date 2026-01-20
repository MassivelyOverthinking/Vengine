# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import polars as pl

from typing import Optional

from src.reader import BaseReader
from src.typings import ReaderConfig, InputType

# ---------------------------------------------------------------
# JSONREADER CLASS
# ---------------------------------------------------------------

class JSONReader(BaseReader):

    __slots__ = (
        "use_columns",
        "n_rows",
        "low_memory",
        "rechunk",
        "row_index_name",
    )

    def __init__(
        self,
        *,
        use_columns: Optional[list[str]] = None,
        n_rows: Optional[int] = None,
        low_memory: bool = False,
        rechunk: bool = False,
        row_index_name: Optional[str] = None,
        verbosity: int = 0,
        **base_kwargs,
        
    ):
        super().__init__(verbosity=verbosity, **base_kwargs)

        self.use_columns = use_columns
        self.n_rows = n_rows
        self.low_memory = low_memory
        self.rechunk = rechunk
        self.row_index_name = row_index_name

    def _materialize_config(self) -> ReaderConfig:
        return ReaderConfig(
            parameters={
                "use_columns": self.use_columns,
                "n_rows": self.n_rows,
                "low_memory": self.low_memory,
                "rechunk": self.rechunk,
                "row_index_name": self.row_index_name
            }
        )
    
    def _discover_schema(self, input: InputType) -> pl.Schema:
    
        lf = pl.scan_ndjson(
            source=input,
            schema=self.use_columns,
            n_rows=self.n_rows,
            low_memory=self.low_memory,
            rechunk=self.rechunk,
            row_index_name=self.row_index_name
        )

        schema = lf.schema
        self._logger(f"Schema initialized: {schema}")

        return schema
    
    def _to_lazyframe(self, input: InputType) -> pl.LazyFrame:
        
        lf = pl.scan_ndjson(
            source=input,
            schema=self.use_columns,
            n_rows=self.n_rows,
            low_memory=self.low_memory,
            rechunk=self.rechunk,
            row_index_name=self.row_index_name
        )

        self._logger.info(f"Data succesfully loaded into LazyFrame.")
        return lf