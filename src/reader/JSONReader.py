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
# JSONREADER CLASS
# ---------------------------------------------------------------

class JSONReader(BaseReader):

    __slots__ = (
        "dtypes",
        "use_columns",
        "n_rows",
        "low_memory",
        "rechunk",
        "row_index_name",
    )

    def __init__(
        self,
        *,
        schema: Dict[str, pl.DataType] | pl.Schema = None,
        dtypes: Optional[Dict[str, pl.Schema]] = None,
        use_columns: Optional[list[str]] = None,
        n_rows: Optional[int] = None,
        low_memory: bool = False,
        rechunk: bool = False,
        row_index_name: Optional[str] = None,
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
        self.use_columns = use_columns
        self.n_rows = n_rows
        self.low_memory = low_memory
        self.rechunk = rechunk
        self.row_index_name = row_index_name

    # Convert and return the internal configuration -> Used for _signature (Hashing).
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

        lf = pl.scan_ndjson(
            source=input,
            dtypes=dtypes,
            schema=self.use_columns,
            n_rows=self.n_rows,
            low_memory=self.low_memory,
            rechunk=self.rechunk,
            row_index_name=self.row_index_name
        )

        self._logger.info(f"Data succesfully loaded into LazyFrame.")

        return lf