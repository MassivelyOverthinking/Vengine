# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

from typing import List, Union, Optional, Tuple, Any
from abc import ABC, abstractmethod
from dataclasses import field
from datetime import datetime, timezone

# ---------------------------------------------------------------
# BASEFACTORY CLASS -> ABSTRACTION
# ---------------------------------------------------------------

class BaseFactory(ABC):

    __slots__ = ()

    def __init__(self):
        super().__init__(),

    @abstractmethod
    def produce() -> Any:
        pass
    
class Other:
    pass # Placeholder class -> Type checking.