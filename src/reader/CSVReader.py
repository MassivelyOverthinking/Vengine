# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import polars as pl

from typing import List, Optional, Union, Dict, Any
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
        "low_memory",
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

        self.separator = separator
        self.header = 0 if header else None
        self.skip_rows = skip_rows
        self.skip_lines = skip_lines
        self.encoding = encoding
        self.null_values = null_values
        self.use_columns = use_columns
        self.n_rows = n_rows
        self.low_memory = low_memory

    def discover(self, input: InputType) -> None:
        if self._assert_not_built:
            self._schema = self._discover_schema_from_sample(input=input)
            self._config = self._materialize_config()

        self._logger.info(
            f"{self.__class__.__name__}: Internal schema successfully discovered: {self.schema}"
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

    