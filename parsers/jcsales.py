# parsers/jcsales.py
# JC Sales TEXT Parser → returns (rows_df, invoice_number)
# rows_df columns: ITEM, DESCRIPTION, PACK, COST, UNIT

from __future__ import annotations
from typing import Tuple, Optional
import re
import numpy as np
import pandas as pd

WANT_COLS = ["ITEM", "DESCRIPTION", "PACK", "COST", "UNIT"]
_MONEY = r"(\$?\d{1,3}(?:,\d{3})*\.\d{2}|\$?\d+\.\d{2})"

# Regex for the text format:
# Line# | [Flag] | ItemCode | Desc | RQty | SQty | UM | [Pack] | Unit | Cost | Ext | [Pack2]
LINE_RE = re.compile(
    rf"""
    ^\s*
    (?P<linenum>\d+)\s+          # Line Number (e.g. 1)
    (?:[A-Z]\s+)?                # Optional Flag (T, C, etc.)
    (?P<item>\d{{4,6}})\s+       # Item Code (e.g. 14158)
    (?P<desc>.+?)\s+             # Description (Lazy match)
    (?P<rqty>\d+)\s+             # R-Qty
    (?P<sqty>\d+)\s+             # S-Qty
    (?P<um>[A-Z]+)\s+            # UM (e.g. PK)
    (?:(?P<pack>\d+)\s+)?        # Optional Middle Pack
    (?P<unit>{_MONEY})\s+        # Unit Price
    (?P<cost>{_MONEY})\s+        # Case Cost
    {_MONEY}                     # Extension (Ignore)
    (?:\s+(?P<pack2>\d+))?       # Optional End Pack
    \s*$
    """,
    re.IGNORECASE | re.VERBOSE
)

def _to_float(x):
    if x is None: return np.nan
    s = str(x).replace("$", "").replace(",", "").strip()
    try:
        return float(s)
    except:
        return np.nan

def _to_int(x, default=0):
    if x is None: return default
    try:
        return int(round(float(str(x).replace(",", "").strip())))
    except:
        return default

class JCSalesParser:
    name = "JC Sales (Text Paste)"

    def parse(self, text_input: str) -> Tuple[pd.DataFrame, Optional[str]]:
        """
        Parses raw text pasted from the PDF.
        """
        rows = []
        
        # Split input into lines
        lines = (text_input or "").strip().splitlines()
        
        for line in lines:
            line = line.strip()
            if not line: continue
            
            # Try to match our clean regex
            m = LINE_RE.match(line)
            if not m:
                continue
                
            data = m.groupdict()
            
            item_code = data["item"]
            desc = data["desc"].strip()
            
            # Logic: Pack might be in middle or end
            pack = _to_int(data["pack"])
            pack2 = _to_int(data["pack2"])
            final_pack = pack2 if pack2 > 0 else pack
            if final_pack <= 0: final_pack = 1
            
            unit = _to_float(data["unit"])
            cost = _to_float(data["cost"])
            
            if np.isnan(unit) or np.isnan(cost):
                continue
                
            # Swap if needed (Unit > Cost is usually wrong for Pack > 1)
            if unit > cost and final_pack > 1:
                unit, cost = cost, unit
                
            rows.append({
                "ITEM": item_code,
                "DESCRIPTION": desc,
                "PACK": int(final_pack),
                "COST": float(cost),
                "UNIT": float(unit),
                "_order": int(data["linenum"])
            })

        if not rows:
            return pd.DataFrame(columns=WANT_COLS), None
            
        # Sort by Line Number
        df = pd.DataFrame(rows).sort_values("_order").reset_index(drop=True)
        
        # Attempt to find Invoice Number in the text
        inv_match = re.search(r"\b(OSI\d{5,})\b", text_input, re.IGNORECASE)
        invoice_number = inv_match.group(1).upper() if inv_match else "MANUAL_PASTE"
        
        return df[WANT_COLS], invoice_number
