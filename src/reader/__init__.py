# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

from src.reader.BaseReader import BaseReader
from src.reader.CSVReader import CSVReader
from src.reader.JSONReader import JSONReader
from src.reader.ExcelReader import ExcelReader
from src.reader.ParquetReader import ParquetReader
from src.reader.FeatherReader import FeatherReader

# ---------------------------------------------------------------
# PACKAGE MANAGEMENT
# ---------------------------------------------------------------

__all__ = [
    "BaseReader",
    "CSVReader",
    "JSONReader",
    "ExcelReader",
    "ParquetReader",
    "FeatherReader",
]
__version__ = "0.0.1"
__author__ = "HysingerDev"