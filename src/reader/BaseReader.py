# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import polars as pl

from src.typings import ReaderConfig, ReaderPlan, ReaderSchema, ReaderResult, InputType
from src.utility.setup_logger import get_class_logger

from typing import Tuple, Any, Dict, Hashable
from abc import abstractmethod
from datetime import datetime
from time import perf_counter
from logging import Logger


# ---------------------------------------------------------------
# BASEREADER CLASS -> ABSTRACTION
# ---------------------------------------------------------------

class BaseReader():

    __slots__ = ("_built", "_config", "_schema", "_logger")

    def __init__(self, verbosity: int = 0) -> None:
        self._config: ReaderConfig   = self._materialize_config()
        self._built: bool            = False
        self._schema: pl.Schema      = None
        self._logger: Logger         = get_class_logger(self.__class__, verbosity)

    @property
    def config(self) -> ReaderConfig:
        return self._config
    
    @property
    def schema(self) -> pl.Schema:
        if self._assert_built():
            return self._schema
        
    @property
    def columns(self) -> Tuple[str, ...]:
        if self._assert_built():
            return tuple(self._schema.keys())

    @property
    def dtypes(self) -> Tuple[pl.DataType, ...]:
        if self._assert_built():
            return tuple(self._schema.values())
        
    @property
    def column_types(self) -> Dict[str, pl.DataType]:
        return dict(self._schema) if self._built else {}

    @property
    def is_built(self) -> bool:
        return self._built
    
    @abstractmethod
    def _to_lazyframe(self, input: InputType) -> pl.LazyFrame:
        pass

    @abstractmethod
    def _discover_schema(self, input: InputType) -> ReaderSchema:
        pass

    @abstractmethod
    def _materialize_config(self) -> ReaderConfig:
        pass
    
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

    def has_column(self, column: str) -> bool:
        if not self._built:
            self._logger(f"Reader: {type(self).__name__} is not constructed!")
            return False
        
        column = column.lower()
        if column in self._schema:
            self._logger(f"Column: {column} found in Reader: {type(self).__name__}")
            return True
        
        self._logger(f"Column: {column} not found in Reader: {type(self).__name__}")
        return False
    
    def build(self) -> None:

        if self._built:
            self._logger(f"Reader: {type(self).__name__} is already constructed!")
            return
        
        schema = self._discover_schema(None)

        if not isinstance(schema, ReaderSchema):
            error_str = f"Schema discovery method must return a polars.Schema object." \
                        f"Received {type(schema).__name__} instead."
            
            self._logger.error(error_str)
            raise TypeError(error_str)
        
        self._schema = schema
        self._built = True
        self._logger.info(
            f"Reader: {type(self).__name__} built successfully with schema: {self._schema}"
        )

    def execute(self, input: InputType) -> ReaderResult:
        if self._assert_built():
            start_time = perf_counter()

            try:
                lf = self._to_lazyframe(input)

                self._validate_schema(lf)
            except Exception as err:
                end_time = perf_counter()
                final_time = end_time - start_time
                metadata = self._collect_metadata(input=input, time=final_time, success=False)
                self._logger(f"Reader: {type(self).__name__} execution was unsuccessful.")
                raise err

            end_time = perf_counter()
            final_time = end_time - start_time
            metadata = self._collect_metadata(input=input, time=final_time, success=False)

            self._logger.info(f"Reader: {type(self).__name__} executed successfully.")
            return ReaderResult(
                frame=lf,
                schema=self._schema,
                metadata=metadata
            )
        
    def summary(self, input: InputType, n: int = 5) -> pl.DataFrame:
        if self._assert_built():

            lf: pl.LazyFrame = self.execute(input)
            df_summary: pl.DataFrame = lf.head(n=n).collect()

            self._logger.info(f"Reader: {type(self).__name__} summary generated successfully.")
        
            return df_summary
        
    def _validate_schema(self, lf: pl.LazyFrame) -> None:
        expexted_schema: pl.Schema  = lf.schema
        actual_schema: pl.Schema    = self._schema

        missing_cols = expexted_schema.keys() - actual_schema.keys()

        if missing_cols:
            raise ValueError(f"Missins columns: {sorted(missing_cols)}")
        
        for column, type in expexted_schema.items():
            exp_type = actual_schema[column]
            if exp_type != type:
                raise TypeError(
                    f"Column: {column} has data type: {exp_type} - Expected type: {type}"
                )
        
        self._logger.info(
            f"Schema validation passed for reader: {type(self).__name__}."
        )

    def _collect_metadata(self, input: pl.LazyFrame, time: float, success: bool) -> Dict[str, Any]:
        return {
            "reader": self.__class__.__name__,
            "built": self._built,
            "configuration": self._config,
            "schema": self._schema,
            "datetime": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "exec_time": 0,
            "success": True,
        }

    def _signature(self) -> Hashable:
        if self._assert_built():
            
            return ReaderPlan(
                return_type=type(self),
                schema=self._canonicalize(self._config.parameters),
                fingerprint=tuple(self._schema.items()),
            )

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

    def reset(self) -> None:
        if self._assert_built():
            self._built = False
            self._schema = None

    def __repr__(self):
        built_str = "Built" if self._built else "Not Built"
        schema_str = dict(self._schema) if self._schema else None

        return (f"{type(self).__name__}({built_str}, "
                f"Config={self._canonicalize(self._config.parameters)}, "
                f"Schema={schema_str})"
        )
        
    def __str__(self):
        return f"Type={type(self).__name__}, Config=({self._canonicalize(self._config.parameters)})"
    
    def __len__(self):
        if self._built and self._schema:
            return len(self._schema)
        
        return 0

    def __iter__(self):
        if self._built and self._schema:
            return iter(self._schema.keys())
        
        return iter(())
    
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