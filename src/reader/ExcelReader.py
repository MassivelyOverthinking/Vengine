# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import polars as pl

from typing import List, Union, Optional

from src.reader import BaseReader
from src.typings import ReaderConfig, InputType

# ---------------------------------------------------------------
# EXCELREADER CLASS
# ---------------------------------------------------------------

class ExcelReader(BaseReader):

    __slots__ = (
        "sheet_name",
        "header",
        "use_columns",
        "drop_empty_columns",
        "drop_empty_rows",
        "excel_engine",
    )

    def __init__(
        self,
        *,
        sheet_name: Union[str, int] = 0,
        header: bool = True,
        use_columns: Optional[List[str]] = None,
        excel_engine: Optional[str] = "calamine",
        drop_empty_columns: bool = False,
        drop_empty_rows: bool = False,
        verbosity: int = 0,
        **base_kwargs,
    ):
        super().__init__(verbosity=verbosity, **base_kwargs)

        self.sheet_name = sheet_name
        self.header = 0 if header else None
        self.use_columns = use_columns
        self.drop_empty_columns = drop_empty_columns
        self.drop_empty_rows = drop_empty_rows
        self.excel_engine = excel_engine

    def _materialize_config(self) -> ReaderConfig:
        return ReaderConfig(
            parameters={
                "sheet_name": self.sheet_name,
                "header": self.header,
                "use_columns": self.use_columns,
                "drop_empty_columns": self.drop_empty_columns,
                "drop_empty_rows": self.drop_empty_rows,
                "engine": self.excel_engine,
            }
        )
    
    def _discover_schema(self, input: InputType) -> pl.Schema:
        
        lf = pl.read_excel(
            input,
            sheet_name=self.sheet_name,
            has_header=self.header,
            columns=self.use_columns,
            drop_empty_cols=self.drop_empty_columns,
            drop_empty_rows=self.drop_empty_rows,
            engine=self.excel_engine
        ).lazy()

        schema = lf.schema
        self._logger.info(f"Schema initialized: {schema}")

        return schema
        
    def _to_lazyframe(self, input: InputType):
        
        lf = pl.read_excel(
            input,
            sheet_name=self.sheet_name,
            has_header=self.header,
            columns=self.use_columns,
            drop_empty_cols=self.drop_empty_columns,
            drop_empty_rows=self.drop_empty_rows,
            engine=self.excel_engine
        ).lazy()

        self._logger.info(f"Data succesfully loaded into LazyFrame.")
        return lf