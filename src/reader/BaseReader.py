# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

from typing import List, Union, Optional, Tuple, Any
from abc import ABC, abstractmethod
from dataclasses import field
from datetime import datetime, timezone

# ---------------------------------------------------------------
# BASEREADER CLASS -> ABSTRACTION
# ---------------------------------------------------------------

class BaseReader(ABC):

    __slots__ = ()

    def __init__(self):
        super().__init__(),

    @abstractmethod
    def read() -> Any:
        pass
    
class Other:
    pass # Placeholder class -> Type checking.