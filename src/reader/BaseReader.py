# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import pandas as pd
import polars as pl
import pyarrow as pa
import json

from src.utility import retrieve_output_format
from src.utility.typings import DataTable, InputType

from typing import List, Union, Optional, Tuple, Any, Dict
from abc import abstractmethod
from dataclasses import field
from datetime import datetime, timezone
from time import perf_counter

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
        input: InputType,
    ) -> DataTable:
        start_time = perf_counter()

        raw_data = self._read_raw(input)
        processed_data = self._process_format(raw_data)

        if self.collect_metadata:
            self._collect_metadata(processed_data)

        end_stime = perf_counter()
        elapsed_time = end_stime - start_time
        self._collect_history(elapsed_time)

        return processed_data

    def _handle_input(
        self,
        input: InputType
    ) -> DataTable:
        pass

    def _collect_history(self, elapsed_time: float) -> None:
        information_dict = {
            "reader_id": id(self),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "elapsed_time": elapsed_time,
        }

        json_str = json.dumps(information_dict)
        self.history.append(json_str)

    @abstractmethod
    def _read_raw(self) -> DataTable:
        pass

    @abstractmethod
    def _process_format(self):
        pass

    @abstractmethod
    def _collect_metadata(self, data: DataTable) -> None:
        if isinstance(data, pd.DataFrame):
            metadata = {
                "reader_id": id(self),
                "class": self.__class__.__name__,
                "frame_type": "pandas",
                "num_rows": data.shape[0],
                "num_columns": data.shape[1],
                "columns": data.columns.tolist(),
                "dtypes": data.dtypes.apply(lambda x: x.name).to_dict(),
            }
        elif isinstance(data, pl.DataFrame):
            metadata = {
                "reader_id": id(self),
                "class": self.__class__.__name__,
                "frame_type": "polars",
                "num_rows": data.height,
                "num_columns": data.width,
                "columns": data.columns,
                "dtypes": {col: str(dtype) for col, dtype in zip(data.columns, data.dtypes)},
            }
        elif isinstance(data, pa.Table):
            metadata = {
                "reader_id": id(self),
                "class": self.__class__.__name__,
                "frame_type": "pyarrow",
                "num_rows": data.num_rows,
                "num_columns": data.num_columns,
                "columns": data.column_names,
                "dtypes": {col: str(data.schema.field(col).type) for col in data.column_names},
            }
        else:
            metadata = {}

    @property
    def history(self) -> List[Dict[str, Any]]:
        return self.history