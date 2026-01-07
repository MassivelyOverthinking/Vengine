# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

from pandas import DataFrame as pd_DataFrame
from polars import DataFrame as pl_DataFrame
from pyarrow import Table as pa_Table

from io import StringIO, BytesIO
from os import PathLike

from typing import Union

# ---------------------------------------------------------------
# CUSTOM TYPES
# ---------------------------------------------------------------

# Custom Data type -> Reader output format
DataTable = Union[pd_DataFrame, pl_DataFrame, pa_Table]

# Custom Input type -> Reader input format
InputType = Union[str, PathLike, StringIO, BytesIO]