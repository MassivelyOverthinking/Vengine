# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import polars as pl

from typing import List, Optional, Any
from polars.lazyframe import LazyFrame

from src.reader import BaseReader
from src.typings import ReaderConfig, ReaderSchema, InputType

# ---------------------------------------------------------------
# CSVREADER CLASS
# ---------------------------------------------------------------

class CSVReader(BaseReader):

    __slots__ = (
        "dtypes",
        "seperator",
        "header",
        "skip_rows",
        "skip_lines",
        "encoding",
        "null_values",
        "use_columnns",
        "data_types",
        "n_rows",
        "try_parse_dates",
        "low_memory",
        "schema"
    )

    def __init__(
        self,
        *,
        dtypes: dict[str, pl.DataType] = None,
        separator: str = ",",
        header: Optional[bool] = True,
        skip_rows: int = 0,
        skip_lines: int = 0,
        encoding: str = "utf-8",
        null_values: Optional[List[str]] = None,
        use_columns: Optional[List[str]] = None,
        data_types: Optional[dict[str, Any]] = None,
        n_rows: Optional[int] = None,
        try_parse_dates: bool = True,
        low_memory: bool = True,
        verbosity: int = 0,
        **base_kwargs,
    ):
        super().__init__(verbosity=verbosity, **base_kwargs)

        self.dtypes = dtypes
        self.separator = separator
        self.header = 0 if header else None
        self.skip_rows = skip_rows
        self.skip_lines = skip_lines
        self.encoding = encoding
        self.null_values = null_values
        self.use_columns = use_columns
        self.data_types = data_types
        self.n_rows = n_rows
        self.try_parse_dates = try_parse_dates
        self.low_memory = low_memory
        self.schema = None

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
                "data_types": self.data_types,
                "n_rows": self.n_rows,
                "try_parse_dates": self.try_parse_dates,
                "low_memory": self.low_memory
            }
        )
    
    def _discover_schema(self, input: InputType) -> None:
        
        if self.dtypes is not None:
            self.schema = self.dtypes
        else:

            sample = pl.read_csv(
                input,
                separator=self.separator,
                n_rows=1,
                encoding=self.encoding
            )

        self.schema = {col: pl.Utf8 for col in sample.columns}

    def _to_lazyframe(self, input: InputType) -> LazyFrame:

        if self.schema is None:
            self._discover_schema(input=input)

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
            dtypes=self.data_types,
            n_rows=self.n_rows,
            try_parse_dates=False,
            low_memory=self.low_memory,
        )

        missing_columns = [col for col in self.schema if col not in lf.columns]
        if missing_columns:
            raise ValueError(
                f"{self.__class__.__name__}: Missing expected columns - {missing_columns}"
            )

        self._logger.info(f"Data succesfully loaded into LazyFrame.")

        return lf

    