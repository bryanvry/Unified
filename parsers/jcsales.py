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

def _fix_merged_qty_tokens(line: str) -> str:
    """
    Fixes common OCR merging issues.
    e.g. "OZ1" -> "OZ 1"
    e.g. "1PK" -> "1 PK"
    e.g. "52T" -> "52 T"
    """
    # insert a space at any letter→digit boundary (e.g., "OZ1" → "OZ 1")
    line = re.sub(r'(?<=[A-Za-z])(?=\d)', ' ', line)
    # insert a space at any digit→letter boundary (e.g., "1PK" → "1 PK")
    line = re.sub(r'(?<=\d)(?=[A-Za-z])', ' ', line)
    return " ".join(line.split())

def _parse_lines(text: str) -> pd.DataFrame:
    # Split full text into physical lines
    raw_lines = (text or "").splitlines()
    
    # ---------------------------------------------------------
    # BLOCK BUILDER: Sequential Scan
    # We know items are numbered 1..N. We look for these numbers 
    # at the start of lines to chop the text into Item Blocks.
    # ---------------------------------------------------------
    item_blocks = []
    current_block_lines = []
    next_target_line_num = 1
    
    # Buffer to hold lines before the first item (headers)
    # or lines between items that belong to the previous item.
    
    for raw_line in raw_lines:
        line = _fix_merged_qty_tokens(raw_line.strip())
        if not line:
            continue
            
        # Check if this line starts with our next target number (e.g. "1 ", "2 ", "119 ")
        # We use \b to ensure "1" doesn't match "100"
        # We allow optional 'T' or 'C' flags merged or separate, but broadly just look for the number.
        
        is_target = False
        
        # Regex: Start of line, Target Number, Word Boundary
        # e.g. "^1\b" matches "1 14158..." or "1" or "1 T..."
        if re.match(rf'^{next_target_line_num}\b', line):
            is_target = True
        
        if is_target:
            # If we were building a block, save it
            if current_block_lines:
                item_blocks.append(" ".join(current_block_lines))
            
            # Start new block
            current_block_lines = [line]
            next_target_line_num += 1
        else:
            # Not a new item start. 
            # If we have an active block (i.e. we found line 1 already), append to it.
            # This captures wrapped descriptions, split item codes, etc.
            if current_block_lines:
                current_block_lines.append(line)

    # Append the final block
    if current_block_lines:
        item_blocks.append(" ".join(current_block_lines))

    # ---------------------------------------------------------
    # BLOCK PARSER
    # Now we have chunks of text, each guaranteed* to contain one Item.
    # We just need to fish out the Item Code and the Math.
    # ---------------------------------------------------------
    rows = []
    
    # Regex to find the Item Code: 4-6 digits.
    # We skip the Line Number (which we know is at the start).
    # We look for the FIRST 4-6 digit sequence after the line number.
    item_code_re = re.compile(r'\b(\d{4,6})\b')

    # Regex to find the Numeric Tail (Price/Pack) at the END of the block.
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

    for block in item_blocks:
        # 1. Find the Tail (Math)
        m_tail = tail_re.search(block)
        if not m_tail:
            # If no math found, this might be a false positive block or header junk.
            continue
        
        tail_data = m_tail.groupdict()
        
        # 2. Separate Head (Text) from Tail
        head_part = block[:m_tail.start()]
        
        # 3. Extract Item Code
        # The head starts with "LineNum ...". We want the next number.
        # Let's strip the leading Line Number to avoid matching it as the item code.
        # (e.g. Line "14158" might be mistaken for Item "14158" if Line is huge? Unlikely)
        # But safely: Remove the first token (Line Number).
        tokens = head_part.split()
        if not tokens:
            continue
        
        # Pop the line number (we know it's there because we built the block that way)
        # But we verify if the first token looks like a number.
        line_num_token = tokens.pop(0) 
        
        # Also pop optional flag 'T' or 'C' if present
        if tokens and tokens[0].upper() in ['T', 'C']:
            tokens.pop(0)
            
        # Reconstruct remaining head to find Item Code
        remaining_head = " ".join(tokens)
        
        m_item = item_code_re.search(remaining_head)
        if not m_item:
            continue
            
        item_code = m_item.group(1)
        
        # 4. Extract Description
        # Description is everything in remaining_head AFTER the item code.
        # We split by the item code match.
        desc_start_idx = m_item.end()
        description = remaining_head[desc_start_idx:].strip()
        
        # 5. Resolve Pack/Cost/Unit
        pack = _to_int(tail_data['pack'])
        pack2 = _to_int(tail_data['pack2'])
        final_pack = pack2 if pack2 > 0 else pack
        if final_pack <= 0:
            final_pack = 1
            
        unit = _to_float(tail_data['unit'])
        cost = _to_float(tail_data['cost'])
        
        if unit is None or np.isnan(unit) or cost is None or np.isnan(cost):
            continue

        # Sanity swap
        if unit > cost and final_pack > 1:
            unit, cost = cost, unit
            
        rows.append({
            "ITEM": item_code,
            "DESCRIPTION": description,
            "PACK": int(final_pack),
            "COST": float(cost),
            "UNIT": float(unit),
            "_order": _to_int(line_num_token)
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
