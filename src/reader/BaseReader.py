# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import polars as pl

from src.typings import ReaderConfig, ReaderPlan, InputType

from typing import List, Optional, Tuple, Any, Dict, Hashable
from abc import abstractmethod


# ---------------------------------------------------------------
# BASEREADER CLASS -> ABSTRACTION
# ---------------------------------------------------------------

class BaseReader():

    __slots__ = ("_built", "_config", "_schema")

    def __init__(self):
        self._config = self._materialize_config()
        self._built = False
        self._schema = None

    @property
    def config(self) -> ReaderConfig:
        return self._config
    
    @property
    def schema(self) -> Optional[pl.Schema]:
        if self._assert_built():
            return self._schema
        
    @property
    def columns(self) -> Tuple[str, ...]:
        if self._assert_built():
            return tuple(self._schema.keys())

    @property
    def dtypes(self) -> Dict[str, pl.DataType]:
        if self._assert_built():
            return dict(self._schema.items())

    @property
    def is_built(self) -> bool:
        return self._built
    
    def _assert_built(self) -> bool:
        if not self._built:
            raise RuntimeError(
                f"Reader of type {type(self).__name__} is not built. "
                "Please call the 'build' method before using it."
            )
        
        return True
    
    def _assert_not_built(self) -> bool:
        if self._built:
            raise RuntimeError(
                f"Reader of type {type(self).__name__} is already built. "
                "Please create a new instance to modify its configuration."
            )
        
        return True
    
    @abstractmethod
    def _discover_schema(self, input: InputType) -> pl.Schema:
        pass
    
    def build(self) -> None:

        if self._built:
            return
        
        schema = self._discover_schema(None)

        if not isinstance(schema, pl.Schema):
            raise TypeError(
                f"Schema discovery method must return a polars.Schema object, "
                f"got {type(schema).__name__} instead."
            )
        
        self._schema = schema
        self._built = True

    def _plan_signature(self) -> Hashable:
        if self._assert_built():
            
            return ReaderPlan(
                type(self),
                self._canonicalize(self._config.parameters),
                tuple(self._schema.items()),
            )

    @abstractmethod
    def _materialize_config(self) -> ReaderConfig:
        pass

    @abstractmethod
    def _collect_metadata(self, input: pl.LazyFrame) -> Dict[str, Any]:
        pass

    @abstractmethod
    def _to_lazyframe(self, input: InputType) -> pl.LazyFrame:
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
        return f"Type={type(self).__name__}, Config=({self._canonicalize(self._config.parameters)})"
    
    def __bool__(self):
        return self._built
    
    def __eq__(self, other):
        if not isinstance(other, BaseReader):
            return False
        return self._plan_signature() == other._plan_signature()

    def __hash__(self):
        return hash(self._plan_signature())