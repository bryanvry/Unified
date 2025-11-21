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
    # Insert space between Letter and Digit (e.g. "OZ1" -> "OZ 1")
    text = re.sub(r'(?<=[A-Za-z])(?=\d)', ' ', text)
    # Insert space between Digit and Letter (e.g. "1PK" -> "1 PK", "52T" -> "52 T")
    text = re.sub(r'(?<=\d)(?=[A-Za-z])', ' ', text)
    return text

def _parse_lines(full_text: str) -> pd.DataFrame:
    # 1. Global Clean
    clean_text = _fix_merged_qty_tokens(full_text)
    
    # 2. Find all "Numeric Tails" (The math part at the end of each item)
    # Pattern: R-QTY, S-QTY, UM, [PACK], UNIT, COST, EXT, [PACK2]
    # We capture the start and end of every tail.
    tail_pattern = re.compile(
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
        \s* # Trailing spaces
        """,
        re.IGNORECASE | re.VERBOSE
    )
    
    tails = list(tail_pattern.finditer(clean_text))
    
    rows = []
    
    # 3. Iterate through tails to process items
    # The first item is everything before tails[0].
    # The second item is everything between tails[0] and tails[1].
    
    start_idx = 0
    
    # We look for the Item Code (4-6 digits) in the text chunk.
    # We ignore small numbers (1-3 digits) which are likely Line Numbers or Garbage.
    item_code_re = re.compile(r'\b(\d{4,6})\b')
    
    for i, match in enumerate(tails):
        # Define the "Chunk" of text for this item
        # It starts after the previous tail ended (or 0 for the first item)
        # It ends right before the current tail starts.
        end_idx = match.start()
        
        chunk = clean_text[start_idx:end_idx]
        tail_data = match.groupdict()
        
        # Update start_idx for the next loop
        start_idx = match.end()
        
        # Process the chunk to find Item Code and Description
        # Clean up newlines/tabs
        chunk_clean = " ".join(chunk.split())
        
        # Find the first 4-6 digit number in this chunk.
        # This skips "Line Numbers" (usually 1-3 digits) and flags.
        m_item = item_code_re.search(chunk_clean)
        
        if not m_item:
            # If no item code found, maybe this tail is a false positive or header junk?
            # But JCSales invoices are usually dense. We'll skip to be safe.
            continue
            
        item_code = m_item.group(1)
        
        # Description is everything AFTER the Item Code in this chunk.
        desc_start = m_item.end()
        description = chunk_clean[desc_start:].strip()
        
        # Filter out common garbage from Description if it captured header info
        # e.g. "DESCRIPTION Line # Item" or "Page 1"
        # We take the *last* part if there are multiple distinctive parts?
        # Actually, usually the description is just the text. 
        # But if a Header appeared in the middle, like: "Page 2 ... Line Item Desc", 
        # the Item Code logic `\d{4,6}` effectively skips the "Page 2" part 
        # IF the header doesn't contain a 4-6 digit number.
        # "2025" (Year) is 4 digits. This is a risk.
        # "90058" (Zip) is 5 digits.
        # So we must be careful. Item codes appear usually *after* the Line Number.
        # Since we pick the *first* 4-6 digit number, we might hit a Zip code if the chunk spans a header.
        # Heuristic: Item Codes are usually followed by text (Description).
        # Zip codes are usually followed by "TEL" or nothing.
        # Let's assume the first match is correct for now, as tails are frequent enough to limit chunk size.
        
        # Resolve Math
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
            "_order": i  # Use loop index to preserve order
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
