# parsers/jcsales.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Tuple, List
import re
import pandas as pd

try:
    import pdfplumber
except Exception:
    pdfplumber = None


@dataclass
class JCLine:
    line_no: int
    item: str
    desc: str
    r_qty: int
    s_qty: int
    um: str
    pack: int
    unit_p: float   # UNIT_P (unit price printed on invoice)
    um_p: float     # UM_P (case price)  -> this is COST
    ext_p: float    # EXT_P


class JCSalesParser:
    WANT_COLS = ["UPC", "DESCRIPTION", "PACK", "COST", "UNIT", "RETAIL", "NOW", "DELTA"]

    def __init__(self):
        pass

    # ---------------- core text parsing ----------------
    def _iter_text_lines(self, pdf: "pdfplumber.PDF") -> List[str]:
        lines: List[str] = []
        for page in pdf.pages:
            t = page.extract_text() or ""
            # Keep original order, drop obvious header/footer noise lines
            for raw in t.splitlines():
                line = raw.strip()
                if not line:
                    continue
                # Skip section headers
                if line.startswith("LINE # ITEM") or "Customer Copy" in line:
                    continue
                if "Printed." in line and "Page" in line:
                    continue
                lines.append(line)
        return lines

    def _parse_line(self, line: str) -> Optional[JCLine]:
        """
        Expect the tail of a valid item line to look like:
           ... <R-QTY> <S-QTY> <UM> <UNIT_P> <UM_P> <EXT_P> <PACK>
        Example:
           '1 14158 AXION ... 1 1 PK 2.39 28.68 28.68 12'
        We'll:
          - parse the 7 trailing fields first (right-anchored)
          - then peel off the left part to read LINE#, [optional T/C], ITEM, DESCRIPTION
        """
        # Right-anchored capture of the 7 final tokens
        tail_regex = re.compile(
            r"""
            \s
            (?P<rqty>\d+)
            \s+(?P<sqty>\d+)
            \s+(?P<um>[A-Z]{1,3})
            \s+(?P<unit_p>\d+\.\d{2})
            \s+(?P<um_p>\d+\.\d{2})
            \s+(?P<ext_p>\d+\.\d{2})
            \s+(?P<pack>\d+)
            \s*$""",
            re.VERBOSE,
        )
        mt = tail_regex.search(line)
        if not mt:
            return None

        # Split head vs tail
        head = line[: mt.start()].strip()
        # Head looks like:  '<LINE#> [T|C]? <ITEM> <DESCRIPTION...>'
        # Be tolerant about the optional flag and multi-space blobs.
        head_regex = re.compile(
            r"""
            ^\s*
            (?P<lineno>\d+)
            (?:\s+[TC])?            # optional T/C flag
            \s+(?P<item>\d+)
            \s+(?P<desc>.+?)\s*$    # greedy description
            """,
            re.VERBOSE,
        )
        mh = head_regex.match(head)
        if not mh:
            return None

        try:
            return JCLine(
                line_no=int(mh.group("lineno")),
                item=mh.group("item"),
                desc=mh.group("desc").strip(),
                r_qty=int(mt.group("rqty")),
                s_qty=int(mt.group("sqty")),
                um=mt.group("um"),
                pack=int(mt.group("pack")),
                unit_p=float(mt.group("unit_p")),
                um_p=float(mt.group("um_p")),
                ext_p=float(mt.group("ext_p")),
            )
        except Exception:
            return None

    def _parse_pdf(self, uploaded_pdf) -> Tuple[pd.DataFrame, str]:
        """
        Return (rows_df, invoice_no)
        rows_df has columns: ITEM, DESCRIPTION, PACK, COST(=UM_P), UNIT, RETAIL
        """
        if pdfplumber is None:
            return pd.DataFrame(), ""

        try:
            uploaded_pdf.seek(0)
        except Exception:
            pass

        with pdfplumber.open(uploaded_pdf) as pdf:
            lines = self._iter_text_lines(pdf)

        # Try to extract invoice number from the buffer (e.g., '*OSI014135*' or 'JCSALES OSI014135 ...')
        raw_text = "\n".join(lines)
        m_inv = re.search(r"\bOSI0?\d{5}\b", raw_text)
        invoice_no = m_inv.group(0) if m_inv else "UNKNOWN"

        parsed: List[JCLine] = []
        for li, raw in enumerate(lines):
            rec = self._parse_line(raw)
            if rec:
                parsed.append(rec)

        if not parsed:
            return pd.DataFrame(), invoice_no

        # Build DataFrame (keep original order by line_no)
        parsed.sort(key=lambda r: r.line_no)
        df = pd.DataFrame(
            {
                "LINE": [r.line_no for r in parsed],
                "ITEM": [r.item for r in parsed],
                "DESCRIPTION": [r.desc for r in parsed],
                "PACK": [r.pack for r in parsed],
                # COST uses UM_P per spec
                "COST": [r.um_p for r in parsed],
                # sanity extras (not returned to app, but could be helpful)
                "_UNIT_P": [r.unit_p for r in parsed],
                "_EXT_P": [r.ext_p for r in parsed],
            }
        )

        # Derive UNIT & RETAIL
        df["UNIT"] = (pd.to_numeric(df["COST"], errors="coerce") / df["PACK"].replace(0, pd.NA)).round(2)
        df["RETAIL"] = (df["UNIT"] * 2).round(2)

        # Keep only expected output columns for the next stage (UPC, NOW, DELTA added in app using pricebook)
        return df[["ITEM", "DESCRIPTION", "PACK", "COST", "UNIT", "RETAIL"]], invoice_no

    # ---------------- public API ----------------
    def parse(self, uploaded_pdf) -> Tuple[pd.DataFrame, str]:
        """
        Returns a DF with columns: ITEM, DESCRIPTION, PACK, COST, UNIT, RETAIL
        and the invoice number string.
        """
        if not uploaded_pdf or (getattr(uploaded_pdf, "name", "") or "").lower().endswith(".pdf") is False:
            return pd.DataFrame(), ""

        try:
            rows, inv_no = self._parse_pdf(uploaded_pdf)
        except Exception:
            return pd.DataFrame(), ""

        return rows, inv_no


# --------------- helpers used by app.py ---------------

def normalize_upc_str(s: str) -> str:
    """Strip non-digits and drop leading zeros for matching purposes."""
    digits = re.sub(r"\D", "", str(s or ""))
    return digits.lstrip("0") or ("0" if digits else "")


def resolve_upc_from_master(pricebook_df: pd.DataFrame, master_df: pd.DataFrame, item_series: pd.Series) -> pd.Series:
    """
    Use ITEM to lookup (UPC1/UPC2) in master, then check which one exists in pricebook['Upc'] after zero-strip normalization.
    If neither match, return "No Match <ITEM>".
    """
    # Minimal columns
    m = master_df.copy()
    m_cols = {c.lower(): c for c in m.columns}
    item_col = m_cols.get("item", "ITEM")
    upc1_col = m_cols.get("upc1", "UPC1")
    upc2_col = m_cols.get("upc2", "UPC2")

    # Build ITEM→(UPC1, UPC2)
    m_small = m[[item_col, upc1_col, upc2_col]].copy()
    m_small.columns = ["ITEM", "UPC1", "UPC2"]
    m_small["ITEM"] = m_small["ITEM"].astype(str).str.strip()

    # Pricebook UPC map (normalized)
    pb = pricebook_df.copy()
    pb_cols = {c.lower(): c for c in pb.columns}
    pb_upc = pb_cols.get("upc", "Upc")
    pb["__upc_norm"] = pb[pb_upc].astype(str).map(normalize_upc_str)

    pb_set = set(pb["__upc_norm"])

    # Map items
    out = []
    item_series = item_series.astype(str).str.strip()
    master_map = {str(r.ITEM): (r.UPC1, r.UPC2) for r in m_small.itertuples(index=False)}

    for itm in item_series.tolist():
        upc1, upc2 = master_map.get(itm, (None, None))
        u1 = normalize_upc_str(upc1)
        u2 = normalize_upc_str(upc2)
        if u1 and u1 in pb_set:
            # return as proper 12–14 digit with leading zeros from PB (first match)
            row = pb.loc[pb["__upc_norm"] == u1].iloc[0]
            out.append(str(row[pb_upc]))
        elif u2 and u2 in pb_set:
            row = pb.loc[pb["__upc_norm"] == u2].iloc[0]
            out.append(str(row[pb_upc]))
        else:
            out.append(f"No Match {itm}")

    return pd.Series(out, index=item_series.index)
