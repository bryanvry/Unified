# parsers/__init__.py
# Registry of all supported vendor parsers

from .unified import UnifiedParser
from .southern_glazers import SouthernGlazersParser
from .nevada_beverage import NevadaBeverageParser
from .breakthru import BreakthruParser

ALL_PARSERS = {
    # key shown in UI : (slug, parser_class)
    "Unified (SVMERCH)": ("unified", UnifiedParser),
    "Southern Glazer's": ("southern_glazers", SouthernGlazersParser),
    "Nevada Beverage": ("nevada_beverage", NevadaBeverageParser),
    "Breakthru": ("breakthru", BreakthruParser),
}
