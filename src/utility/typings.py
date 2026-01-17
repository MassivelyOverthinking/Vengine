# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

from pandas import DataFrame as pd_DataFrame
from polars import DataFrame as pl_DataFrame
from pyarrow import Table as pa_Table

from io import StringIO, BytesIO
from os import PathLike

from typing import Union

# Custom Input type -> Reader input format
InputType = Union[str, PathLike, StringIO, BytesIO]