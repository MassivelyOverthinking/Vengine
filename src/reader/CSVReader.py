# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import polars as pl

from typing import List, Optional, Union, Dict
from polars.lazyframe import LazyFrame

from src.reader import BaseReader
from src.typings import ReaderConfig, ReaderSchema, InputType

# ---------------------------------------------------------------
# CSVREADER CLASS
# ---------------------------------------------------------------

class CSVReader(BaseReader):

    __slots__ = (
        "schema",
        "seperator",
        "header",
        "skip_rows",
        "skip_lines",
        "encoding",
        "null_values",
        "use_columnns",
        "data_types",
        "n_rows",
        "low_memory",
        "discover_schema",
        "discover_rows"
    )

    def __init__(
        self,
        *,
        schema: Union[pl.Schema, Dict[str, pl.DataType]] = None,
        separator: str = ",",
        header: Optional[bool] = True,
        skip_rows: int = 0,
        skip_lines: int = 0,
        encoding: str = "utf-8",
        null_values: Optional[List[str]] = None,
        use_columns: Optional[List[str]] = None,
        n_rows: Optional[int] = None,
        low_memory: bool = True,
        discover_schema: bool = False,
        discover_rows: int = 1000,
        verbosity: int = 0,
        **base_kwargs,
    ):
        if schema is not None and discover_schema:
            raise ValueError(f"Please provide either an explicit ReaderSchema or enable discover_schema - Not both!")
        
        super().__init__(verbosity=verbosity, **base_kwargs)

        self.schema = schema
        self.separator = separator
        self.header = 0 if header else None
        self.skip_rows = skip_rows
        self.skip_lines = skip_lines
        self.encoding = encoding
        self.null_values = null_values
        self.use_columns = use_columns
        self.n_rows = n_rows
        self.low_memory = low_memory
        self.discover_schema = discover_schema
        self.discver_rows = discover_rows

    def discover(self, input: InputType) -> None:
        if self._assert_not_built:
            self.schema = self._discover_schema_from_sample(input=input)

        self._logger.info(
            f"CSVReader: Internal schema successfully discovered: {self.schema}"
        )

    def _materialize_config(self) -> ReaderConfig:
        return ReaderConfig(
            parameters={
                "dtypes": self.dtypes,
                "separator": self.separator,
                "header": self.header,
                "skip_rows": self.skip_rows,
                "skip_lines": self.skip_lines,
                "encoding": self.encoding,
                "null_values": self.null_values,
                "use_columns": self.use_columns,
                "n_rows": self.n_rows,
                "low_memory": self.low_memory
            }
        )

    def _discover_schema_from_sample(self, input: InputType) -> pl.Schema:
        df = pl.scan_csv(
            input,
            n_rows=self.discver_rows,
            infer_schema_length=self.discver_rows,
            try_parse_dates=True
        )

        return df.schema


    def _to_lazyframe(self, input: InputType) -> LazyFrame:

        if isinstance(self.schema, dict):
            self.schema = pl.Schema(self.schema)

        lf = pl.scan_csv(
            input,
            dtypes=self.schema,
            sep=self.separator,
            has_header=self.header is not None,
            skip_rows=self.skip_rows,
            skip_rows_after_header=self.skip_lines,
            encoding=self.encoding,
            null_values=self.null_values,
            columns=self.use_columns,
            n_rows=self.n_rows,
            try_parse_dates=False,
            low_memory=self.low_memory,
        )

        self._logger.info(f"Data succesfully loaded into LazyFrame.")

        return lf

    