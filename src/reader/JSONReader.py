# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

from typing import List, Union, Optional, Tuple, Any
from abc import ABC, abstractmethod
from dataclasses import field
from datetime import datetime, timezone

from src.reader import BaseReader

# ---------------------------------------------------------------
# JSONREADER CLASS
# ---------------------------------------------------------------

class JSONReader(BaseReader):

    __slots__ = ()

    def __init__(self, metadata = True):
        super().__init__(metadata)