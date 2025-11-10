# parsers/__init__.py
from .southern_glazers import SouthernGlazersParser
from .nevada_beverage import NevadaBeverageParser

ALL_PARSERS = {
    "southern_glazers": SouthernGlazersParser,
    "nevada_beverage": NevadaBeverageParser,
}

__all__ = ["ALL_PARSERS", "SouthernGlazersParser", "NevadaBeverageParser"]
