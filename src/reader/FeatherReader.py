# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import polars as pl

from typing import Optional, Dict
from polars.lazyframe import LazyFrame

from src.reader import BaseReader
from src.errors import ReaderConfigError
from src.typings import ReaderConfig, InputType

# ---------------------------------------------------------------
# FEATHERREADER CLASS
# ---------------------------------------------------------------

class FeatherReader(BaseReader):

    __slots__ = (
        "schema",
        "n_rows",
        "cache",
        "rechunk",
        "row_index_name",
        "memory_map",
    )

    def __init__(
        self,
        *,
        schema: Dict[str, pl.DataType] | pl.Schema = None,
        dtypes: Optional[Dict[str, pl.Schema]] = None,
        n_rows: Optional[int] = None,
        cache: bool = True,
        rechunk: bool = False,
        row_index_name: Optional[str] = None,
        memory_map: bool = False,
        infer_schema: bool = False,
        infer_rows: int = 100,
        verbosity: int = 0,
        **base_kwargs,
    ):
        super().__init__(
            schema=schema,
            infer_schema=infer_schema,
            infer_rows=infer_rows,
            verbosity=verbosity,
            **base_kwargs
        )

        self.dtypes = dtypes
        self.n_rows = n_rows
        self.cache = cache
        self.rechunk = rechunk
        self.row_index_name = row_index_name
        self.memory_map = memory_map
        
    # Convert and return the internal configuration -> Used for _signature (Hashing).
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
    
    # Utilise Polars to read specified Data -> Wrapped in ReaderResult-class and returned to Conduit. 
    def _to_lazyframe(self, input: InputType) -> LazyFrame:

        if self.dtypes:
            if not isinstance(self.dtypes, dict):
                raise ReaderConfigError(
                    self,
                    f"Data types must be a Dict[str, polars.Datatype] - Recieved {type(self.dtypes)}"
                )
            
            dtypes = pl.Schema(self.dtypes)
        else:
            dtypes = self.schema
        
        lf = pl.scan_ipc(
            source=input,
            dtypes=dtypes,
            n_rows=self.n_rows,
            cache=self.cache,
            rechunk=self.rechunk,
            row_index_name=self.row_index_name,
            memory_map=self.memory_map
        )

        self._logger(f"Data succesfully loaded into LazyFrame.")

        return lf