# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import typing

from src.pipeline.BaseConduit import BaseConduit

from typing import List, Union, Optional
from abc import ABC, abstractmethod
from dataclasses import field
from datetime import datetime, timezone

# ---------------------------------------------------------------
# MULTI-THREADED CONDUIT CLASS
# ---------------------------------------------------------------

class ThreadConduit(BaseConduit):

    __slots__ = ()

    def __init__(self, reader = None, schema = None, waypoints = ..., factories = ..., verbosity = 1):
        super().__init__(reader, schema, waypoints, factories, verbosity)

    @typing.Override
    def execute(self):
        pass