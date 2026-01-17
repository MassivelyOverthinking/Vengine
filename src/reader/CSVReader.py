# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import polars as pl

from typing import List, Optional, Any
from polars.lazyframe import LazyFrame

from src.reader import BaseReader
from src.typings import ReaderConfig, InputType

# ---------------------------------------------------------------
# CSVREADER CLASS
# ---------------------------------------------------------------

class CSVReader(BaseReader):

    __slots__ = (
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
    )

    def __init__(
        self,
        *,
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
        **base_kwargs,
    ):
        super().__init__(**base_kwargs)

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

    def _materialize_config(self) -> ReaderConfig:
        return ReaderConfig(
            parameters={
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

    def _read_raw(self, input: InputType) -> LazyFrame:
        df = pl.scan_csv(
            input,
            sep=self.separator,
            has_header=self.header is not None,
            skip_rows=self.skip_rows,
            skip_rows_after_header=self.skip_lines,
            encoding=self.encoding,
            null_values=self.null_values,
            columns=self.use_columns,
            dtypes=self.data_types,
            n_rows=self.n_rows,
            try_parse_dates=self.try_parse_dates,
            low_memory=self.low_memory,
        ).lazy()

        return df
    