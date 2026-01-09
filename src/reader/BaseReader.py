# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import pandas as pd
import polars as pl
import pyarrow as pa
import sys
import json

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

    @property
    def history(self) -> Optional[List[Dict[str, Any]]]:
        return self.history
    
    @property
    def metadata(self) -> Optional[List[Dict[str, Any]]]:
        return self.metadata
    
    @property
    def lifecycle(self) -> Optional[Dict[str, Any]]:
        return self.lifecycle

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

        start_time = perf_counter()

        try:
            raw_data = self._read_raw(input, engine=engine)
        except Exception as e:
            self.logger.error(f"Read operation failed with error: {e}")
            self._update_lifecycle(engine, 0.0, success=False)
            raise
        finally:
            end_time = perf_counter()
            elapsed_time = end_time - start_time

        if self.collect_history:
            self._collect_history(elapsed_time)
        if self.collect_metadata:
            self._collect_metadata(raw_data)

        self._update_lifecycle(engine, elapsed_time, success=True)

        self.logger.info(f"Read operation completed successfully!")

        return raw_data

    def _collect_history(self, elapsed_time: float) -> None:
        information_dict = {
            "reader_id": id(self),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "elapsed_time": elapsed_time,
        }

        self.history.append(information_dict)
        self.logger.info("History collected successfully.")

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
            self.logger.error(f"Unsupported data type for metadata collection: {type(data)}")
            raise TypeError(f"Unsupported data type for metadata collection: {type(data)}")

        self.metadata.append(metadata)
        self.logger.info("Metadata collected successfully.")

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
    
    def _update_lifecycle(self, engine: str, elapsed_time: float, success: bool) -> None:
        if self.lifecycle is None:
            self.logger.warning("Lifecycle collection is disabled! Cannot update lifecycle information.")
            return

        self.lifecycle["total_reads"] += 1
        if success:
            self.lifecycle["successful_reads"] += 1
        else:
            self.lifecycle["failed_reads"] += 1

        self.lifecycle["success_rate"] = (
            self.lifecycle["successful_reads"] / self.lifecycle["total_reads"]
        ) * 100.0

        self.lifecycle["last_read_at"] = datetime.now(timezone.utc).isoformat()
        self.lifecycle["total_elapsed_time"] += elapsed_time
        self.lifecycle["average_read_time"] = (
            self.lifecycle["total_elapsed_time"] / self.lifecycle["total_reads"]
        )

        if engine in self.lifecycle["engines_used"]:
            self.lifecycle["engines_used"][engine] += 1

        self.logger.info(f"Lifecycle information updated! Read operation success: {success}")

    @abstractmethod
    def _read_raw(self, input: InputType, engine: str) -> DataTable:
        pass

    def get_state(self) -> str:
        state_dict = {
            "class": self.__class__.__name__,
            "default_engine": self.default_engine,
            "collect_metadata": self.collect_metadata,
            "collect_history": self.collect_history,
            "collect_lifecycle": self.collect_lifecycle,
            "verbosity": self.logger.level,
        }

        json_str = json.dumps(state_dict, indent=4)
        self.logger.info("State retrieved successfully.")
        return json_str

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
    
    def __copy__(self):
        cls = self.__class__
        new_instance = cls.__new__(cls)

        new_instance.default_engine = self.default_engine
        new_instance.collect_metadata = self.collect_metadata
        new_instance.collect_history = self.collect_history
        new_instance.collect_lifecycle = self.collect_lifecycle
        new_instance.logger = self.logger

        return new_instance

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} id={id(self)}>"

    def __str__(self) -> str:
        return f"<{self.__class__.__name__} id={id(self)}>"

    def __hash__(self) -> int:
        return hash(self._key())