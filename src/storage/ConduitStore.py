# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

from src.pipeline import BaseConduit

from typing import List, Union, Optional, Tuple, Any, Dict
from collections import OrderedDict
from dataclasses import dataclass
from abc import ABC, abstractmethod
from dataclasses import field
from datetime import datetime, timezone

# ---------------------------------------------------------------
# STORAGE INSTANCE -> CONDUITS
# ---------------------------------------------------------------

class ConduitStore():

    __slots__ = ("conduits", "ordered")

    def __init__(
        self,
        conduits: Dict[str, BaseConduit] = {},
        ordered: bool = False,
    ):
        self.conduits = self._produce_ordered_dict(dict=conduits)
        self.ordered = ordered

    def add(identifier: str, conduit: BaseConduit) -> bool:
        pass

    def retrieve(identifier: str) -> Optional[BaseConduit]:
        pass

    @property
    def keys() -> List[str]:
        pass

    @property
    def conduits() -> List[BaseConduit]:
        pass

    def _produce_ordered_dict(self, dict: Dict[str, BaseConduit]) -> Union[dict, OrderedDict]:
        if self.ordered:
            return OrderedDict(dict)
        else:
            return dict