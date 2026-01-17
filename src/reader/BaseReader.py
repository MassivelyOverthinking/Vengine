# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import polars as pl

from src.typings import ReaderConfig, InputType

from typing import List, Optional, Tuple, Any, Dict
from collections import deque
from abc import abstractmethod
from datetime import datetime, timezone
from time import perf_counter

# ---------------------------------------------------------------
# BASEREADER CLASS -> ABSTRACTION
# ---------------------------------------------------------------

class BaseReader():

    __slots__ = ("_built", "_config")

    def __init__(self, config):
        self._config = self._materialize_config()
        self._built = False

    @property
    @abstractmethod
    def config(self) -> ReaderConfig:
        return self._config

    @abstractmethod
    def _materialize_config(self) -> ReaderConfig:
        pass

    @abstractmethod
    def _read_raw(self, input: InputType) -> pl.LazyFrame:
        pass

    def _canonicalize(self, input: Any) -> pl.LazyFrame:
        if isinstance(input, dict):
            return tuple(
                (key, self._canonicalize(value))
                for key, value in sorted(input.items())
            )
        elif isinstance(input, (list, tuple)):
            return tuple(self._canonicalize(item) for item in input)
        elif isinstance(input, set):
            return tuple(sorted(self._canonicalize(item) for item in input))
        else:
            return input
        
    def __str__(self):
        return f"Type={type(self).__name__}, Config=({self._config})"
    
    def __bool__(self):
        return self._built

    def __hash__(self):
        return hash((type(self), self._canonicalize(self._config)))