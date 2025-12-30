# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

from typing import List, Union, Optional
from abc import ABC, abstractmethod

# ---------------------------------------------------------------
# PACKAGE MANAGEMENT
# ---------------------------------------------------------------

class BaseConduit(ABC):

    __slots__ = ("reader", "schema" "waypoints", "factories", "verbosity")

    def __init__(
        self,
        reader: "Other" = None,
        schema: "Other" = None,
        waypoints: List["Other"] = [],
        factories: List["Other"] = [],
        verbosity: int = 1,
    ):
        self.reader = reader
        self.schema = schema
        self.waypoints = waypoints
        self.factories = factories
        self.verbosity = verbosity

    @abstractmethod
    def execute(self):
        pass

    @abstractmethod
    def add_waypoint(self, waypoint: "Other") -> bool:
        pass

    @abstractmethod
    def add_factory(self, factory: "Other") -> bool:
        pass

    @property
    @abstractmethod
    def reader(self) -> "Other":
        pass

    @property
    @abstractmethod
    def schema(self) -> "Other":
        pass

    @property
    @abstractmethod
    def waypoints(self) -> "Other":
        pass

    @property
    @abstractmethod
    def factories(self) -> "Other":
        pass

class Other:
    pass # Placeholder class -> Type checking.