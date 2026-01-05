# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import pandas as pd
import polars as pl
import pyarrow as pa

from src.utility import retrieve_output_format


from typing import List, Union, Optional, Tuple, Any, Dict
from io import StringIO, BytesIO
from abc import abstractmethod
from dataclasses import field
from datetime import datetime, timezone

# ---------------------------------------------------------------
# BASEREADER CLASS -> ABSTRACTION
# ---------------------------------------------------------------



class BaseReader():

    __slots__ = ("output_engine", "collect_metadata", "history")

    def __init__(
        self,
        output_engine: str = "pandas",
        collect_metadata: bool = True,
    ):
        self.output_format = field(default_factory=retrieve_output_format(input=output_engine))
        self.collect_metadata = collect_metadata
        self.history = []

    def read(
        self,
        input: str | StringIO | BytesIO,
    ) -> pd.DataFrame | pl.DataFrame | pa.Table:
        pass

    def _handle_input(
        self,
        input: str | StringIO | BytesIO
    ) -> str | StringIO | BytesIO:
        pass

    @abstractmethod
    def _read_raw(self) -> pd.DataFrame | pl.DataFrame | pa.Table:
        pass

    @abstractmethod
    def _process_format():
        pass

    @abstractmethod
    def _collect_metadata():
        pass

    @property
    def history(self) -> List[Dict[str, Any]]:
        return self.history