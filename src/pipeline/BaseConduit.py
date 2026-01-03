# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

from typing import List, Union, Optional, Dict, Any
from collections import OrderedDict
from abc import ABC, abstractmethod
from dataclasses import field
from datetime import datetime, timezone

# ---------------------------------------------------------------
# BASECONDUIT CLASS -> ABSTRACTION
# ---------------------------------------------------------------

class BaseConduit(ABC):

    __slots__ = (
        "reader",
        "schema"
        "waypoints",
        "factories",
        "severity",
        "format",
        "created_at",
        "history",
        "verbosity"
    )

    def __init__(
        self,
        reader: "Other" = None,
        schema: "Other" = None,
        waypoints: List["Other"] = [],
        factories: List["Other"] = [],
        severity: str = "fatal",
        format: str = "json",
        verbosity: int = 1,
    ):
        self.reader = reader
        self.schema = schema
        self.waypoints = waypoints
        self.factories = factories
        self.severity = severity
        self.format = format
        self.created_at = field(default_factory=datetime.now(timezone.utc))
        self.history = OrderedDict()
        self.verbosity = verbosity

    @abstractmethod
    def execute(self) -> "Other":
        pass

    @abstractmethod
    def add_waypoint(self, waypoint: "Other") -> bool:
        pass

    @abstractmethod
    def add_factory(self, factory: "Other") -> bool:
        pass

    @property
    @abstractmethod
    def reader(self) -> Union["Other", None]:
        return self.reader

    @property
    @abstractmethod
    def schema(self) -> Union["Other", None]:
        return self.schema

    @property
    @abstractmethod
    def waypoints(self) -> Union[List["Other"], "Other"]:
        return self.waypoints

    @property
    @abstractmethod
    def factories(self) -> Union[List["Other"], "Other"]:
        return self.factories
    
    @abstractmethod
    def get_state(self) -> Dict[str, Any]:
        pass

class Other:
    pass # Placeholder class -> Type checking.