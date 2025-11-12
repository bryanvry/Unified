# parsers/__init__.py
# Registry of all supported vendor parsers

# These files must exist alongside this __init__.py:
#   parsers/unified.py
#   parsers/southern_glazers.py
#   parsers/nevada_beverage.py
#   parsers/breakthru.py

from .unified import UnifiedParser
from .southern_glazers import SouthernGlazersParser
from .nevada_beverage import NevadaBeverageParser
from .breakthru import BreakthruParser

# Map label shown in the app → (slug, class)
ALL_PARSERS = {
    "Unified (SVMERCH)": ("unified", UnifiedParser),
    "Southern Glazer's": ("southern_glazers", SouthernGlazersParser),
    "Nevada Beverage": ("nevada_beverage", NevadaBeverageParser),
    "Breakthru": ("breakthru", BreakthruParser),
}
