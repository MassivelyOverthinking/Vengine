# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import pandas as pd
import polars as pl
import pyarrow as pa
import sys

from src.utility import retrieve_output_format
from src.utility.typings import DataTable, InputType
from src.utility.setup_logger import get_class_logger

from typing import List, Optional, Tuple, Any, Dict
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
        "collect_metadata",
        "collect_history",
        "collect_lifecycle",
        "metadata",
        "history",
        "lifecycle",
        "logger"
    )

    def __init__(
        self,
        default_engine: str = "pandas",
        collect_metadata: bool = True,
        collect_history: bool = True,
        collect_lifecycle: bool = True,
        history_max_size: Optional[int] = None,
        metadata_max_size: Optional[int] = None,
        verbosity: int = 0,
    ):
        self.default_engine = field(default_factory=retrieve_output_format(input=default_engine))
        self.collect_metadata = collect_metadata
        self.collect_history = collect_history
        self.collect_lifecycle = collect_lifecycle
        self.metadata = deque(maxlen=metadata_max_size) if collect_metadata else None
        self.history = deque(maxlen=history_max_size) if collect_history else None
        self.lifecycle = field(default_factory=self._initialize_lifecycle) if collect_lifecycle else None
        self.logger = get_class_logger(self.__class__, verbosity=verbosity)

    def read(
        self,
        input: InputType,
        engine: Optional[str] = None
    ) -> DataTable:
        if not isinstance(input, InputType):
            raise TypeError(f"Input must be of type InputType - Recieved {type(input)}")
        if engine is not None and not isinstance(engine, str):
            raise TypeError(f"Engine must be of type str - Recieved {type(engine)}")
        
        self.logger.info(f"Starting Read operation!")

        engine = retrieve_output_format(input=engine) if engine is not None else self.default_engine

        if self.history is not None:
            start_time = perf_counter()

            raw_data = self._read_raw(input, engine=engine)

            end_stime = perf_counter()
            elapsed_time = end_stime - start_time

            self._collect_history(elapsed_time)
            if self.metadata is not None:
                self._collect_metadata(raw_data)
        else:
            raw_data = self._read_raw(input, engine=engine)
            if self.metadata is not None:
                self._collect_metadata(raw_data)

        self.logger.info(f"Read operation completed successfully!")

        return raw_data

    def _collect_history(self, elapsed_time: float) -> None:
        information_dict = {
            "reader_id": id(self),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "elapsed_time": elapsed_time,
        }

        self.history.append(information_dict)

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
            raise TypeError(f"Unsupported data type for metadata collection: {type(data)}")

        self.metadata.append(metadata)

    def _initialize_lifecycle(self) -> Dict[str, Any]:
        lifecycle_info = {
            "reader_id": id(self),
            "class": self.__class__.__name__,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "default_engine": self.default_engine,
            "total_reads": 0,
            "successful_reads": 0,
            "failed_reads": 0,
            "success_rate": 0.0,
            "last_read_at": None,
            "total_elapsed_time": 0.0,
            "average_read_time": 0.0,
            "engines_used": {
                "pandas": 0,
                "polars": 0,
                "pyarrow": 0,
            }
        }

        self.logger.info("Lifecycle information initialized.")
        return lifecycle_info

    @abstractmethod
    def _read_raw(self, input: InputType, engine: str) -> DataTable:
        pass

    @property
    def history(self) -> Optional[List[Dict[str, Any]]]:
        return self.history
    
    @property
    def metadata(self) -> Optional[List[Dict[str, Any]]]:
        return self.metadata
    
    @property
    def lifecycle(self) -> Optional[Dict[str, Any]]:
        return self.lifecycle

    def clear_metadata(self) -> None:
        if self.metadata is not None:
            self.metadata = []
            self.logger.info("Metadata has been cleared.")

        self.logger.warning("No metadata to clear.")

    def clear_history(self) -> None:
        if self.history is not None:
            self.history = []
            self.logger.info("History has been cleared.")

        self.logger.warning("No history to clear.")

    def _key(self) -> Tuple:
        return (self.default_engine, type(self))
    
    def __len__(self) -> int:
        return self.lifecycle["total_reads"] if self.lifecycle is not None else 0
    
    def __eq__(self, value) -> bool:
        if not isinstance(value, self.__class__):
            return False
        return self._key() == value._key()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} id={id(self)}>"

    def __str__(self) -> str:
        return f"<{self.__class__.__name__} id={id(self)}>"

    def __hash__(self) -> int:
        return hash(self._key())