# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import polars as pl

from src.utility import get_class_logger
from src.errors import WaypointBuildError

from polars.dataframe import DataFrame
from typing import List, Union, Optional, Tuple, Any, Dict
from datetime import datetime, timezone
from abc import abstractmethod
from logging import Logger

# ---------------------------------------------------------------
# BASEPOINT CLASS -> ABSTRACTION
# ---------------------------------------------------------------

class BasePoint():

    __slots__ = (
        "_built",
        "_logger"
    )

    def __init__(
        self,
        verbosity: int = 0, 
    ):
        self._logger: Logger    = get_class_logger(self.__class__, verbosity)

    @property
    def is_built(self) -> bool:
        return self._built

    def build(self) -> None:
        
        if self._built:
            self._logger.info(f"Waypoint: {self.__class__.__name__} is already constructed!")
            return
        
        self._built = True

        self._logger.info(
            f"Waypoint: {self.__class__.__name__} built successfully!"
        )
    
    def validate(self, data: DataFrame) -> Dict[str, Any]:
        pass

    def _assert_built(self) -> bool:
        if not self._built:
            error_str = f"Waypoint-instance is currently not constructed." \
                        "Please call the 'build' method before using it."
            
            self._logger.error(error_str)
            raise WaypointBuildError(error_str)
        
        return True
    
    def _assert_not_built(self) -> bool:
        if self._built:
            error_str = f"Waypoint-instance already constructed." \
                        "Please create a new instance to modify its configuration."
            
            self._logger.error(error_str)
            raise WaypointBuildError(error_str)
        
        return True

    def _key(self) -> Tuple:
        pass

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>"
    
    def __str__(self) -> str:
        return f"{self.__class__.__name__} instance"
    
    def __eq__(self, other: Any) -> bool:
        pass

    def __hash__(self):
        return hash(self._key())
    



    