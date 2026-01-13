# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

from typing import List, Union, Optional, Tuple, Any
from datetime import datetime, timezone
from abc import abstractmethod

# ---------------------------------------------------------------
# BASEPOINT CLASS -> ABSTRACTION
# ---------------------------------------------------------------

class BasePoint():

    __slots__ = (

    )

    def __init__(self):
        pass

    @abstractmethod
    def validate(self) -> dict:
        pass

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
    



    