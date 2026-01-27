# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import polars as pl

from typing import Optional, Union, Dict
from polars.lazyframe import LazyFrame

from src.reader import BaseReader
from src.errors import ReaderConfigError
from src.typings import ReaderConfig, InputType

# ---------------------------------------------------------------
# PARQUETREADER CLASS
# ---------------------------------------------------------------

class ParquetReader(BaseReader):

    __slots__ = (
        "dtypes",
        "n_rows",
        "row_index_name",
        "rechunk",
        "low_memory",
    )

    def __init__(
        self,
        *,
        schema: Dict[str, pl.DataType] | pl.Schema = None,
        dtypes: Optional[Dict[str, pl.Schema]] = None,
        n_rows: Optional[int] = None,
        row_index_name: Optional[str] = None,
        rechunk: bool = False,
        low_memory: bool = False,
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
        self.row_index_name = row_index_name
        self.rechunk = rechunk
        self.low_memory = low_memory

    # Convert and return the internal configuration -> Used for _signature (Hashing).
    def _materialize_config(self) -> ReaderConfig:
        return ReaderConfig(
            parameters={
                "n_rows": self.n_rows,
                "row_index_name": self.row_index_name,
                "rechunk": self.rechunk,
                "low_memory": self.low_memory,
            }
        )
    
    # Utilise Polars to read specified Data -> Wrapped in ReaderResult-class and returned to Conduit. 
    def _to_lazyframe(self, input: InputType) -> LazyFrame:

        if self.dtypes:
            if not isinstance(self.dtypes, dict):
                raise ReaderConfigError(
                    f"Data types must be a Dict[str, polars.Datatype] - Recieved {type(self.dtypes)}"
                )
            
            dtypes = pl.Schema(self.dtypes)
        else:
            dtypes = self.schema
        
        lf = pl.scan_parquet(
            source=input,
            dtypes=dtypes,
            n_rows=self.n_rows,
            row_index_name=self.row_index_name,
            rechunk=self.rechunk,
            low_memory=self.low_memory,
        )

        self._logger(f"Data succesfully loaded into LazyFrame.")

        return lf

