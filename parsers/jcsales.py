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

def _fix_merged_qty_tokens(text: str) -> str:
    """
    Global cleanup for the entire text block.
    """
    # Insert space between Letter and Digit (e.g. "OZ1" -> "OZ 1")
    text = re.sub(r'(?<=[A-Za-z])(?=\d)', ' ', text)
    # Insert space between Digit and Letter (e.g. "1PK" -> "1 PK", "52T" -> "52 T")
    text = re.sub(r'(?<=\d)(?=[A-Za-z])', ' ', text)
    return text

def _parse_lines(full_text: str) -> pd.DataFrame:
    # 1. Clean the entire text at once
    clean_text = _fix_merged_qty_tokens(full_text)
    
    # 2. Identify the positions of every Item Start
    # We look for: WordBoundary + LineNum + Spaces + Optional Flag + Spaces + ItemCode (4-6 digits)
    # We collect them as (LineNum, StartIndex)
    item_starts = []
    
    # We iterate 1 to 200 (or until we stop finding them) to find valid item headers
    # We use a regex that is specific to the pattern "N [Flag] ItemCode"
    # We rely on the fact that N is sequential.
    
    current_pos = 0
    expected_num = 1
    
    while True:
        # Search for the next item number, starting from the last found position
        # Pattern: 
        # \b{num}\s+               -> The line number (e.g. "1 ")
        # (?:[A-Z]\s+)?            -> Optional flag (e.g. "T ")
        # \d{4,6}\b                -> The item code
        
        pattern = re.compile(rf"\b{expected_num}\s+(?:[A-Z]\s+)?\d{{4,6}}\b")
        match = pattern.search(clean_text, current_pos)
        
        if not match:
            # If we can't find item #1, maybe something is wrong.
            # If we can't find item #158, we are likely done.
            if expected_num > 1:
                break
            else:
                # Verify we aren't just missing #1. Try #2 just in case? 
                # Nah, let's assume sequential integrity.
                break
        
        start_index = match.start()
        item_starts.append((expected_num, start_index))
        
        # Update position to slightly after this match to avoid re-matching
        current_pos = start_index + 1
        expected_num += 1

    # 3. Slice the text into blocks based on these start positions
    rows = []
    
    # Regex for the numeric tail (Price, Pack, etc.)
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
        \s*
        """,
        re.IGNORECASE | re.VERBOSE
    )
    
    # Regex to parse the header (Item Code)
    header_re = re.compile(r"\b(?P<linenum>\d+)\s+(?:(?P<flag>[A-Z])\s+)?(?P<item>\d{4,6})\b")

    for i in range(len(item_starts)):
        line_num, start_idx = item_starts[i]
        
        # The block ends where the next item starts, or at the end of text
        if i < len(item_starts) - 1:
            end_idx = item_starts[i+1][1]
        else:
            end_idx = len(clean_text)
            
        block = clean_text[start_idx:end_idx]
        
        # Parse this block
        # 1. Find Tail (Searching from the end is safer, but tail_re works on the block)
        # We want the *last* match in the block if there are multiple (unlikely)
        # But usually there is just one numeric tail per item block.
        matches = list(tail_re.finditer(block))
        if not matches:
            continue
        m_tail = matches[-1] # Take the last one found in this chunk
        tail_data = m_tail.groupdict()
        
        # 2. Find Head (Item Code)
        # It should be at the very beginning of the block
        m_head = header_re.match(block)
        if not m_head:
            continue
            
        item_code = m_head.group("item")
        
        # 3. Extract Description
        # Text between Head end and Tail start
        desc_start = m_head.end()
        desc_end = m_tail.start()
        description = block[desc_start:desc_end].strip()
        
        # Clean description (remove newlines)
        description = " ".join(description.split())
        
        # 4. Resolve Math
        pack = _to_int(tail_data['pack'])
        pack2 = _to_int(tail_data['pack2'])
        final_pack = pack2 if pack2 > 0 else pack
        if final_pack <= 0:
            final_pack = 1
            
        unit = _to_float(tail_data['unit'])
        cost = _to_float(tail_data['cost'])
        
        if unit is None or np.isnan(unit) or cost is None or np.isnan(cost):
            continue

        if unit > cost and final_pack > 1:
            unit, cost = cost, unit
            
        rows.append({
            "ITEM": item_code,
            "DESCRIPTION": description,
            "PACK": int(final_pack),
            "COST": float(cost),
            "UNIT": float(unit),
            "_order": line_num
        })

    if not rows:
        return pd.DataFrame(columns=WANT_COLS)

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
