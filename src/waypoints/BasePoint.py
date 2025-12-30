# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

from typing import List, Union, Optional, Tuple, Any
from abc import ABC, abstractmethod
from dataclasses import field
from datetime import datetime, timezone

# ---------------------------------------------------------------
# BASEPOINT CLASS -> ABSTRACTION
# ---------------------------------------------------------------

class BasePoint(ABC):

    __slots__ = ()

    def __init__(self):
        super().__init__(),

    @abstractmethod
    def verify() -> Tuple[bool, "Other"]:
        pass
    
class Other:
    pass # Placeholder class -> Type checking.