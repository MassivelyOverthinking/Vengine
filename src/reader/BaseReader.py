# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import pandas as pd
import polars as pl
import pyarrow as pa
import json
import sys

from src.utility import retrieve_output_format
from src.utility.typings import DataTable, InputType

from typing import List, Union, Optional, Tuple, Any, Dict
from collections import deque
from abc import abstractmethod
from dataclasses import field
from datetime import datetime, timezone
from time import perf_counter

# ---------------------------------------------------------------
# BASEREADER CLASS -> ABSTRACTION
# ---------------------------------------------------------------

class BaseReader():

    __slots__ = (
        "default_engine",
        "metadata",
        "history",
    )

    def __init__(
        self,
        default_engine: str = "pandas",
        collect_metadata: bool = True,
        collect_history: bool = True,
        history_max_size: int = 100,
        metadata_max_size: int = 100,
    ):
        self.default_engine = field(default_factory=retrieve_output_format(input=default_engine))
        self.metadata = deque(maxlen=metadata_max_size) if collect_metadata else None
        self.history = deque(maxlen=history_max_size) if collect_history else None

    def read(
        self,
        input: InputType,
        engine: Optional[str] = None
    ) -> DataTable:
        if engine is None:
            engine = self.default_engine

        if self.history is not None:
            start_time = perf_counter()

            raw_data = self._read_raw(input, engine=engine)

            end_stime = perf_counter()
            elapsed_time = end_stime - start_time
        else:
            raw_data = self._read_raw(input, engine=engine)

        if self.history is not None:
            self._collect_history(elapsed_time)

        return raw_data

    def _collect_history(self, elapsed_time: float) -> None:
        information_dict = {
            "reader_id": id(self),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "elapsed_time": elapsed_time,
        }

        json_str = json.dumps(information_dict)
        self.history.append(json_str)

    @abstractmethod
    def _read_raw(self, input: InputType, engine: str) -> DataTable:
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
                "memory_size": sys.getsizeof(data),
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
                "memory_size": sys.getsizeof(data),
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
                "memory_size": sys.getsizeof(data),
                "num_rows": data.num_rows,
                "num_columns": data.num_columns,
                "columns": data.column_names,
                "dtypes": {col: str(data.schema.field(col).type) for col in data.column_names},
            }
        else:
            metadata = {}

        self.metadata.append(metadata)

    @property
    def history(self) -> Optional[List[Dict[str, Any]]]:
        return self.history
    
    @property
    def metadata(self) -> Optional[List[Dict[str, Any]]]:
        return self.metadata

    def clear_metadata(self) -> None:
        if self.metadata is not None:
            self.metadata = []

    def clear_history(self) -> None:
        if self.history is not None:
            self.history = []

    def __repr__(self):
        pass

    def __str__(self):
        pass

    def __hash__(self):
        pass