# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import polars as pl

from typing import Optional

from src.reader import BaseReader
from src.typings import ReaderConfig, InputType

# ---------------------------------------------------------------
# FEATHERREADER CLASS
# ---------------------------------------------------------------

class FeatherReader(BaseReader):

    __slots__ = (
        "n_rows",
        "cache",
        "rechunk",
        "row_index_name",
        "memory_map",
    )

    def __init__(self,
        *,
        n_rows: Optional[int] = None,
        cache: bool = True,
        rechunk: bool = False,
        row_index_name: Optional[str] = None,
        memory_map: bool = False,
        verbosity: int = 0,
        **base_kwargs,
    ):
        super().__init__(verbosity=verbosity, **base_kwargs)

        self.n_rows = n_rows
        self.cache = cache
        self.rechunk = rechunk
        self.row_index_name = row_index_name
        self.memory_map = memory_map
        

    def _materialize_config(self) -> ReaderConfig:
        return ReaderConfig(
            parameters={
                "n_rows": self.n_rows,
                "cache": self.cache,
                "rechunk": self.rechunk,
                "row_index_name": self.row_index_name,
                "memory_map": self.memory_map
            }
        )
    
    def _discover_schema(self, input: InputType) -> pl.Schema:
        
        lf = pl.scan_ipc(
            source=input,
            n_rows=self.n_rows,
            cache=self.cache,
            rechunk=self.rechunk,
            row_index_name=self.row_index_name,
            memory_map=self.memory_map
        )

        schema = lf.schema
        self._logger.info(f"Schema initialized: {schema}")

        return schema
    
    def _to_lazyframe(self, input: InputType) -> pl.LazyFrame:
        
        lf = pl.scan_ipc(
            source=input,
            n_rows=self.n_rows,
            cache=self.cache,
            rechunk=self.rechunk,
            row_index_name=self.row_index_name,
            memory_map=self.memory_map
        )

        self._logger(f"Data succesfully loaded into LazyFrame.")

        return lf