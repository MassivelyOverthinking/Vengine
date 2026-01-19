# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import polars as pl

from src.typings import ReaderConfig, ReaderPlan, InputType
from src.utility.setup_logger import get_class_logger

from typing import List, Optional, Tuple, Any, Dict, Hashable
from abc import abstractmethod


# ---------------------------------------------------------------
# BASEREADER CLASS -> ABSTRACTION
# ---------------------------------------------------------------

class BaseReader():

    __slots__ = ("_built", "_config", "_schema", "_logger")

    def __init__(self, verbosity: int = 0) -> None:
        self._config = self._materialize_config()
        self._built = False
        self._schema = None
        self._logger = get_class_logger(self.__class__, verbosity)

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
            error_str = f"Reader: {type(self).__name__} is not constructed." \
                        "Please call the 'build' method before using it."
            
            self._logger.error(error_str)
            raise RuntimeError(error_str)
        
        return True
    
    def _assert_not_built(self) -> bool:
        if self._built:
            error_str = f"Reader: {type(self).__name__} is already constructed." \
                        "Please create a new instance to modify its configuration."
            
            self._logger.error(error_str)
            raise RuntimeError(error_str)
        
        return True
    
    @abstractmethod
    def _discover_schema(self, input: InputType) -> pl.Schema:
        pass
    
    def build(self) -> None:

        if self._built:
            return
        
        schema = self._discover_schema(None)

        if not isinstance(schema, pl.Schema):
            error_str = f"Schema discovery method must return a polars.Schema object." \
                        f"Received {type(schema).__name__} instead."
            
            self._logger.error(error_str)
            raise TypeError(error_str)
        
        self._schema = schema
        self._built = True
        self._logger.info(
            f"Reader: {type(self).__name__} built successfully with schema: {self._schema}"
        )

    def execute(self, input: InputType) -> pl.LazyFrame:
        if self._assert_built():

            lf = self._to_lazyframe(input)

            self._validate_schema(lf)

            self._logger.info(
                f"Reader: {type(self).__name__} executed successfully."
            )
            return lf

    def _signature(self) -> Hashable:
        if self._assert_built():
            
            return ReaderPlan(
                type(self),
                self._canonicalize(self._config.parameters),
                tuple(self._schema.items()),
            )
        
    def _validate_schema(self, lf: pl.LazyFrame) -> None:
        
        if self._assert_built():
            actual_schema = lf.schema

            for col, type in self._schema.items():
                if col not in actual_schema:
                    raise ValueError(
                        f"Column '{col}' is missing from the input data."
                    )
                if actual_schema[col] != type:
                    raise TypeError(
                        f"Column '{col}' has type '{actual_schema[col]}', "
                        f"expected type '{type}'."
                    )
        
        self._logger.info(
            f"Schema validation passed for reader: {type(self).__name__}."
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
        return self._signature() == other._signature()

    def __copy__(self):

        cls = self.__class__
        new_obj = cls.__new__(cls)
        new_obj._config = self._config
        new_obj._built = self._built
        new_obj._logger = self._logger
        if hasattr(self, '_schema'):
            new_obj._schema = self._schema

        return new_obj

    def __hash__(self):
        return hash(self._signature())