# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import pandas as pd
import polars as pl
import pyarrow as pa

from typing import List, Optional, Any
from abc import ABC, abstractmethod
from dataclasses import field

from src.reader import BaseReader
from src.utility.typings import DataTable, InputType

# ---------------------------------------------------------------
# CSVREADER CLASS
# ---------------------------------------------------------------

class CSVReader(BaseReader):

    __slots__ = (
        "seperator",
        "header",
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
        separator: str = ",",
        header: bool = True,
        encoding: str = "utf-8",
        null_values: Optional[List[str]] = None,
        use_columns: Optional[List[str]] = None,
        data_types: Optional[dict[str, Any]] = None,
        n_rows: Optional[int] = None,
        low_memory: bool = True,
        **base_kwargs,
    ):
        super().__init__(**base_kwargs)

        self.separator = separator
        self.header = 0 if header else None
        self.encoding = encoding
        self.null_values = null_values
        self.use_columns = use_columns
        self.data_types = data_types
        self.n_rows = n_rows
        self.low_memory = low_memory

    def _read_raw(self, input: InputType, engine: str = "pandas") -> DataTable:
        if engine == "pandas":
            df = pd.read_csv(
                input,
                sep=self.separator,
                header=self.header,
                encoding=self.encoding,
                na_values=self.null_values,
                usecols=self.use_columns,
                dtype=self.data_types,
                nrows=self.n_rows,
                low_memory=self.low_memory,
            )
        elif engine == "polars":
            df = pl.read_csv(
                input,
                separator=self.separator,
                has_header=self.header is not None,
                encoding=self.encoding,
                null_values=self.null_values,
                columns=self.use_columns,
                dtypes=self.data_types,
                n_rows=self.n_rows,
            )
        elif engine == "pyarrow":
            read_options = pa.csv.ReadOptions(
                use_threads=True,
                autogenrate_column_names=self.header is None,
            )

            parse_options = pa.csv.ParseOptions(
                delimiter=self.separator,
            )

            convert_options = pa.csv.ConvertOptions(
                column_types=self.data_types,
                null_values=self.null_values,
                include_columns=self.use_columns,
            )

            return pa.csv.read_csv(
                input,
                read_options=read_options,
                parse_options=parse_options,
                convert_options=convert_options,
            )
        else:
            self.logger.error(f"Unsupported engine: {engine}")
            raise ValueError(f"Unsupported engine: {engine}")

        return df
    