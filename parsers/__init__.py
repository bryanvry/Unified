# parsers/__init__.py
# Existing vendors + NEW Breakthru (added only one line + import)

from .unified_parser import UnifiedParser
from .southern_glazers import SouthernGlazersParser
from .nevada_beverage import NevadaBeverageParser
from .breakthru import BreakthruParser  # <-- NEW

ALL_PARSERS = {
    "Unified (SVMERCH)": ("unified", UnifiedParser),
    "Southern Glazer's": ("southern_glazers", SouthernGlazersParser),
    "Nevada Beverage": ("nevada_beverage", NevadaBeverageParser),
    "Breakthru": ("breakthru", BreakthruParser),  # <-- NEW
}
