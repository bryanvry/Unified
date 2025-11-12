# parsers/__init__.py
# Pre-Breakthru registry (Unified, SG, Nevada only)

from .unified_parser import UnifiedParser
from .southern_glazers import SouthernGlazersParser
from .nevada_beverage import NevadaBeverageParser

ALL_PARSERS = {
    "Unified (SVMERCH)": ("unified", UnifiedParser),
    "Southern Glazer's": ("southern_glazers", SouthernGlazersParser),
    "Nevada Beverage": ("nevada_beverage", NevadaBeverageParser),
}
