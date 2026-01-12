# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import pandas as pd
import polars as pl

from typing import List, Union, Optional, Any

from src.reader import BaseReader
from src.utility.typings import DataTable, InputType

# ---------------------------------------------------------------
# EXCELREADER CLASS
# ---------------------------------------------------------------

class ExcelReader(BaseReader):

    __slots__ = (
        "sheet_name",
        "header",
        "use_columns",
        "data_types",
        "n_rows",
        "skip_rows",
        "na_values",
        "excel_engine",
    )

    def __init__(
        self,
        *,
        sheet_name: Union[str, int] = 0,
        header: bool = True,
        use_columns: Optional[List[str]] = None,
        data_types: Optional[dict[str, Any]] = None,
        n_rows: Optional[int] = None,
        skip_rows: Optional[list[int]] = None,
        na_values: Optional[List[str]] = None,
        excel_engine: Optional[str] = None,
        **base_kwargs,
    ):
        super().__init__(**base_kwargs)

        self.sheet_name = sheet_name
        self.header = 0 if header else None
        self.use_columns = use_columns
        self.data_types = data_types
        self.n_rows = n_rows
        self.skip_rows = skip_rows
        self.na_values = na_values
        self.excel_engine = excel_engine
        

    def _read_raw(self, input: InputType, engine: str = "pandas") -> DataTable:
        if engine == "pandas":
            df = pd.read_excel(
                input,
                sheet_name=self.sheet_name,
                header=self.header,
                usecols=self.use_columns,
                dtype=self.data_types,
                nrows=self.n_rows,
                skiprows=self.skip_rows,
                na_values=self.na_values,
                engine=self.excel_engine,
            )
        elif engine == "polars":
            df = pl.read_excel(
                input,
                sheet_name=self.sheet_name,
                has_header=self.header,
                columns=self.use_columns,
                dtypes=self.data_types,
                n_rows=self.n_rows,
                skip_rows=self.skip_rows,
                na_values=self.na_values,
                engine=self.excel_engine,
            )
        elif engine == "pyarrow":
            self.logger.error("PyArrow does not support direct Excel ingestion.")
            raise ValueError(
                "PyArrow does not support direct Excel ingestion. "
                "Use pandas or convert to CSV/Parquet first."
            )
        else:
            self.logger.error(f"Unsupported Excel engine: {engine}")
            raise ValueError(f"Unsupported Excel engine: {engine}")

        return df