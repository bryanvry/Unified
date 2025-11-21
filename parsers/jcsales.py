# parsers/jcsales.py
# JC Sales PDF parser → returns (rows_df, invoice_number)
# rows_df columns: ITEM, DESCRIPTION, PACK, COST, UNIT

from __future__ import annotations
from typing import Tuple, List, Optional
import re
import numpy as np
import pandas as pd

try:
    import pdfplumber
except Exception:
    pdfplumber = None

WANT_COLS = ["ITEM", "DESCRIPTION", "PACK", "COST", "UNIT"]

_MONEY = r"(\$?\d{1,3}(?:,\d{3})*\.\d{2}|\$?\d+\.\d{2})"

INVOICE_RE = re.compile(r"\b(OSI\d{5,})\b", re.IGNORECASE)

# Regex to find the start of a new item line
# Matches: Start of line, Line Number, Optional Flag (T/C), Item Code (4-6 digits)
# Example: "52 T 116870 " or "1 14158 "
ITEM_START_RE = re.compile(r'^\s*(\d+)\s+(?:[A-Z]\s+)?(?:\d{4,6})\b', re.IGNORECASE)

def _to_float(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return np.nan
    if isinstance(x, (int, float, np.number)):
        return float(x)
    s = str(x).replace("$", "").replace(",", "").strip()
    s = re.sub(r"[^\d.\-]", "", s)
    try:
        return float(s)
    except Exception:
        return np.nan

def _to_int(x, default=0):
    if x is None:
        return default
    try:
        return int(round(float(str(x).replace(",", "").strip())))
    except Exception:
        return default

def _extract_invoice_number(all_text: str) -> Optional[str]:
    m = INVOICE_RE.search(all_text or "")
    return m.group(1).upper() if m else None

def _fix_merged_qty_tokens(line: str) -> str:
    """
    Fixes common OCR merging issues.
    e.g. "OZ1" -> "OZ 1"
    e.g. "1PK" -> "1 PK"
    """
    # insert a space at any letter→digit boundary (e.g., "OZ1" → "OZ 1")
    line = re.sub(r'(?<=[A-Za-z])(?=\d)', ' ', line)
    # insert a space at any digit→letter boundary (e.g., "1PK" → "1 PK")
    line = re.sub(r'(?<=\d)(?=[A-Za-z])', ' ', line)
    return " ".join(line.split())

def _parse_lines(text: str) -> pd.DataFrame:
    # Split full text into physical lines
    raw_lines = (text or "").splitlines()
    
    # Group physical lines into logical "Item Blocks" based on Line Numbers
    item_blocks = []
    current_block = []
    
    for line in raw_lines:
        cleaned_line = line.strip()
        if not cleaned_line:
            continue
            
        # Check if this line starts a new item (e.g. "52 T 116870...")
        if ITEM_START_RE.match(cleaned_line):
            # If we have a previous block accumulating, save it
            if current_block:
                item_blocks.append(" ".join(current_block))
            # Start new block
            current_block = [cleaned_line]
        else:
            # This is likely a wrapped description line or garbage header
            # We append it to the current block if one exists
            if current_block:
                current_block.append(cleaned_line)
                
    # Append the final block
    if current_block:
        item_blocks.append(" ".join(current_block))

    # Now parse each block independently
    rows = []
    
    # Regex to extract data from the TAIL of the block
    # We look for the sequence of numbers at the end of the string.
    # Pattern: R-QTY, S-QTY, UM, [PACK], UNIT, COST, EXT, [PACK2]
    tail_re = re.compile(
        rf"""
        \s+
        (?P<rqty>\d+)\s+
        (?P<sqty>\d+)\s+
        (?P<um>[A-Z]+)\s+
        (?:(?P<pack>\d+)\s+)?          # Optional Middle Pack
        (?P<unit>{_MONEY})\s+
        (?P<cost>{_MONEY})\s+
        {_MONEY}                       # Extension (ignored)
        (?:\s+(?P<pack2>\d+))?         # Optional End Pack
        \s*$
        """,
        re.IGNORECASE | re.VERBOSE
    )
    
    # Regex to extract data from the HEAD of the block
    head_re = re.compile(
        r"""
        ^\s*
        (?P<linenum>\d+)\s+
        (?:[A-Z]\s+)?
        (?P<item>\d{4,6})\s+
        (?P<desc_start>.*?)
        $
        """, 
        re.VERBOSE
    )

    for block_idx, raw_block in enumerate(item_blocks):
        # 1. Fix merged tokens (e.g. OZ1 -> OZ 1)
        block = _fix_merged_qty_tokens(raw_block)
        
        # 2. Find the Numeric Tail
        m_tail = tail_re.search(block)
        if not m_tail:
            # If we can't find the math columns, skip this block (likely header/footer garbage)
            continue
            
        tail_data = m_tail.groupdict()
        
        # 3. Isolate the Head (Everything before the tail)
        head_part = block[:m_tail.start()]
        
        # 4. Parse the Head
        m_head = head_re.match(head_part)
        if not m_head:
            continue
            
        item_code = m_head.group("item").strip()
        # The description is whatever is left in the head part
        description = m_head.group("desc_start").strip()
        
        # 5. Resolve Pack Size
        # Pack might be in the middle group ('pack') or at the end ('pack2')
        pack = _to_int(tail_data['pack'])
        pack2 = _to_int(tail_data['pack2'])
        
        # Use the trailing pack if present, otherwise the middle pack
        final_pack = pack2 if pack2 > 0 else pack
        if final_pack <= 0:
            final_pack = 1
            
        # 6. Resolve Cost and Unit
        unit = _to_float(tail_data['unit'])
        cost = _to_float(tail_data['cost'])
        
        if unit is None or np.isnan(unit) or cost is None or np.isnan(cost):
            continue

        # Sanity swap: if unit > cost (and pack > 1), it's likely swapped or Unit is actually Case Cost
        if unit > cost and final_pack > 1:
            unit, cost = cost, unit
            
        rows.append({
            "ITEM": item_code,
            "DESCRIPTION": description,
            "PACK": int(final_pack),
            "COST": float(cost),
            "UNIT": float(unit),
            "_order": int(m_head.group("linenum")) # Keep line number for sorting
        })

    if not rows:
        return pd.DataFrame(columns=WANT_COLS)

    # Sort by line number to ensure original invoice order
    df = pd.DataFrame(rows).sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)
    return df[WANT_COLS]

class JCSalesParser:
    name = "JC Sales"

    def parse(self, uploaded_pdf) -> Tuple[pd.DataFrame, Optional[str]]:
        if pdfplumber is None:
            return pd.DataFrame(columns=WANT_COLS), None
        try:
            uploaded_pdf.seek(0)
        except Exception:
            pass

        fname = (getattr(uploaded_pdf, "name", "") or "").lower()
        if not fname.endswith(".pdf"):
            return pd.DataFrame(columns=WANT_COLS), None

        try:
            with pdfplumber.open(uploaded_pdf) as pdf:
                texts = [(p.extract_text() or "") for p in pdf.pages]
                all_text = "\n".join(texts)
                df = _parse_lines(all_text)
                inv = _extract_invoice_number(all_text)
                return df, inv
        except Exception:
            return pd.DataFrame(columns=WANT_COLS), None
