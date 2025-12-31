# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

from src.pipeline import BaseConduit

from typing import List, Union, Optional, Tuple, Any, Dict
from dataclasses import dataclass
from abc import ABC, abstractmethod
from dataclasses import field
from datetime import datetime, timezone

# ---------------------------------------------------------------
# STORAGE INSTANCE -> CONDUITS
# ---------------------------------------------------------------

class ConduitStore():

    __slots__ = ("conduits")

    def __init__(
        self,
        conduits: Dict[str, BaseConduit] = {},
    ):
        self.conduits = conduits