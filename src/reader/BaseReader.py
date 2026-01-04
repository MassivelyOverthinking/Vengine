# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import pandas as pd
import polars as pl

from src.utility import retrieve_output_format

from pandas._typing import FilePath, ReadCsvBuffer
from typing import List, Union, Optional, Tuple, Any, Dict
from abc import ABC, abstractmethod
from dataclasses import field
from datetime import datetime, timezone

# ---------------------------------------------------------------
# BASEREADER CLASS -> ABSTRACTION
# ---------------------------------------------------------------

class BaseReader(ABC):

    __slots__ = ("output_format", "metadata", "history")

    def __init__(
        self,
        output_format: str = "pandas",
        metadata: bool = True,
    ):
        super().__init__(),
        self.output_format = field(default_factory=retrieve_output_format(input=output_format))
        self.metadata = metadata
        self.history = []


    @abstractmethod
    def read(
        self,
        input: FilePath | ReadCsvBuffer[bytes] | ReadCsvBuffer[str],
    ) -> pd.DataFrame | pl.DataFrame:
        pass

    @property
    @abstractmethod
    def history(self) -> List[Dict[str, Any]]:
        return self.history