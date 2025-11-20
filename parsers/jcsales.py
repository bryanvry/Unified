# parsers/jcsales.py
# JC Sales PDF parser -> returns parsed rows for app to build:
#   ["UPC","DESCRIPTION","PACK","COST","UNIT","RETAIL","NOW","DELTA","ITEM"]
#
# Robust line pattern support:
#   - Optional leading line number
#   - Optional single-letter flag (T/C/etc.)
#   - ITEM is 3–6 digits (may have leading zeros)
#   - R-QTY S-QTY present
#   - PACK layout variants supported:
#       • "PK 12"   (UM then number)
#       • "12PK"    (number then UM)
#       • "12 PK"   (number then UM separated)
#     plus optional trailing pack override at end of row.
#   - UNIT_P, UM_P, EXT_P are consecutive prices at the end.
#
# COST = UM_P (case price), UNIT = UNIT_P
# Skip rows where S-QTY <= 0 or PACK <= 0
#
from __future__ import annotations
import re
from typing import Tuple, Optional, List
import numpy as np
import pandas as pd

try:
    import pdfplumber
except Exception:
    pdfplumber = None


class JCSalesParser:
    WANT_COLS = ["ITEM", "DESCRIPTION", "PACK", "COST", "UNIT"]

    # --- helpers ---
    @staticmethod
    def _to_float(x) -> float | None:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return None
        s = str(x).replace("$", "").replace(",", "").strip()
        s = re.sub(r"[^\d.\-]", "", s)
        try:
            return float(s)
        except Exception:
            return None

    @staticmethod
    def _to_int(x) -> int | None:
        if x is None:
            return None
        s = re.sub(r"[^\d\-]", "", str(x))
        if s == "":
            return None
        try:
            return int(s)
        except Exception:
            try:
                return int(float(s))
            except Exception:
                return None

    @staticmethod
    def _lz_strip(s: str) -> str:
        s = re.sub(r"\D", "", str(s or ""))
        return s.lstrip("0") or "0"

    # Prices like 2.39, 28.68, etc.
    _MONEY = r"(\d+\.\d{2})"

    # Primary regex — accepts:
    #   [optional LINE#] [optional FLAG] ITEM DESC RQ SQ (PK 12 | 12PK | 12 PK) UNIT_P UM_P EXT_P [PACK_OVERRIDE]
    _LINE_RX = re.compile(
        rf"""
        ^
        \s*
        (?:\d+\s+)?                # optional leading line number
        (?:[A-Z]\s+)?              # optional flag (T/C/etc.)
        (?P<item>\d{{3,6}})\s+     # ITEM 3–6 digits
        (?P<desc>.*?)\s+           # DESCRIPTION (greedy up to numerics)
        (?P<rqty>\d+)\s+           # R-QTY
        (?P<sqty>\d+)\s+           # S-QTY

        (?:
            # Variant A: UM then number -> "PK 12"
            (?P<umA>[A-Z]+)\s+(?P<packA>\d+)
            |
            # Variant B: number then UM (joined or spaced) -> "12PK" or "12 PK"
            (?P<packB>\d+)\s*(?P<umB>[A-Z]+)
        )
        \s+

        (?P<unit>{_MONEY})\s+      # UNIT_P
        (?P<cost>{_MONEY})\s+      # UM_P (case cost)
        {_MONEY}                   # EXT_P (ignored)
        (?:\s+(?P<packZ>\d+))?     # optional trailing pack override
        \s*
        $
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    # Fallback: looser description capture; still respects numeric tail
    _LINE_FALLBACK_RX = re.compile(
        rf"""
        ^
        \s*
        (?:\d+\s+)?(?:[A-Z]\s+)?   # optional line number + flag
        (?P<item>\d{{3,6}})\s+
        (?P<desc>.+?)\s+
        (?P<rqty>\d+)\s+(?P<sqty>\d+)\s+
        (?:
            (?P<umA>[A-Z]+)\s+(?P<packA>\d+)
            |
            (?P<packB>\d+)\s*(?P<umB>[A-Z]+)
        )
        \s+
        (?P<unit>{_MONEY})\s+(?P<cost>{_MONEY})\s+{_MONEY}
        (?:\s+(?P<packZ>\d+))?
        \s*$
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    def _parse_pdf_lines(self, pdf) -> pd.DataFrame:
        rows: List[dict] = []

        for page in pdf.pages:
            text = page.extract_text() or ""
            if not text.strip():
                continue

            for raw in text.splitlines():
                line = raw.strip()
                if not line or len(line) < 8:
                    continue
                if "LINE # ITEM DESCRIPTION" in line.upper():
                    continue

                m = self._LINE_RX.match(line) or self._LINE_FALLBACK_RX.match(line)
                if not m:
                    continue

                item = m.group("item").strip()
                desc = re.sub(r"\s{2,}", " ", m.group("desc")).strip()
                rqty = self._to_int(m.group("rqty")) or 0
                sqty = self._to_int(m.group("sqty")) or 0

                # choose PACK from variants (trailing override wins)
                pack_override = self._to_int(m.group("packZ"))
                packA = self._to_int(m.group("packA"))
                packB = self._to_int(m.group("packB"))
                pack = pack_override or packA or packB or 0

                unit_p = self._to_float(m.group("unit"))
                um_p = self._to_float(m.group("cost"))  # case price

                # must have shipped and valid pack/price
                if sqty <= 0 or pack <= 0 or um_p is None:
                    continue

                # guard: if UNIT > COST, swap (rare OCR quirks)
                if unit_p is not None and unit_p > um_p:
                    unit_p, um_p = um_p, unit_p

                rows.append(
                    {
                        "ITEM": item,
                        "DESCRIPTION": desc,
                        "PACK": int(pack),
                        "COST": float(um_p),
                        "UNIT": float(unit_p) if unit_p is not None else float(um_p) / float(pack),
                        "_order": len(rows),
                    }
                )

        if not rows:
            return pd.DataFrame(columns=self.WANT_COLS)

        out = pd.DataFrame(rows).sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)
        return out[["ITEM", "DESCRIPTION", "PACK", "COST", "UNIT"]]

    # Public API used by app.py
    def parse_invoice(
        self,
        invoice_pdf,          # UploadedFile (pdf)
        jc_master_xlsx,       # UploadedFile (xlsx)
        pricebook_csv,        # UploadedFile (csv)
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Returns:
          parsed_df  -> ["UPC","DESCRIPTION","PACK","COST","UNIT","RETAIL","NOW","DELTA","ITEM"]
          pos_slice  -> pricebook rows (matched UPCs only) with cost_qty/cost_cents updated
        """
        if pdfplumber is None:
            return pd.DataFrame(), pd.DataFrame()

        # 1) parse PDF lines
        try:
            invoice_pdf.seek(0)
        except Exception:
            pass
        try:
            with pdfplumber.open(invoice_pdf) as pdf:
                body = self._parse_pdf_lines(pdf)
        except Exception:
            body = pd.DataFrame(columns=self.WANT_COLS)

        if body.empty:
            return pd.DataFrame(), pd.DataFrame()

        # 2) load master + pricebook
        try:
            jc_master = pd.read_excel(jc_master_xlsx)
        except Exception:
            jc_master = pd.DataFrame()

        try:
            pb = pd.read_csv(pricebook_csv, dtype=str)
        except Exception:
            pb = pd.DataFrame()

        for col in ["Upc", "cents", "cost_cents", "cost_qty"]:
            if col not in pb.columns:
                pb[col] = ""

        pb["_Upc_norm"] = pb["Upc"].astype(str).str.replace(r"\D", "", regex=True)
        pb["_Upc_norm"] = pb["_Upc_norm"].apply(self._lz_strip)

        # 3) map ITEM -> (UPC1, UPC2) from master; pick one that exists in pricebook (leading-zero-insensitive)
        if not jc_master.empty:
            mcols = {c.lower(): c for c in jc_master.columns}
            col_item = mcols.get("item")
            col_u1 = mcols.get("upc1")
            col_u2 = mcols.get("upc2")
            if col_item:
                master = jc_master[[col_item] + [c for c in [col_u1, col_u2] if c]].copy()
                master.columns = ["ITEM"] + ([ "UPC1" ] if col_u1 else []) + ([ "UPC2" ] if col_u2 else [])
                master["ITEM"] = master["ITEM"].astype(str).str.strip()
                master = master.drop_duplicates("ITEM").set_index("ITEM")
            else:
                master = pd.DataFrame(columns=["UPC1","UPC2"])
        else:
            master = pd.DataFrame(columns=["UPC1","UPC2"])

        pb_set = set(pb["_Upc_norm"].tolist())

        def resolve_upc(item_str: str) -> str:
            if item_str in master.index:
                u1 = str(master.loc[item_str, "UPC1"]) if "UPC1" in master.columns else ""
                u2 = str(master.loc[item_str, "UPC2"]) if "UPC2" in master.columns else ""
            else:
                u1 = u2 = ""
            u1n = self._lz_strip(u1)
            u2n = self._lz_strip(u2)
            if u1n != "0" and u1n in pb_set:
                return re.sub(r"\D", "", u1).zfill(12)
            if u2n != "0" and u2n in pb_set:
                return re.sub(r"\D", "", u2).zfill(12)
            return f"No Match {item_str}"

        parsed = body.copy()
        parsed["UPC"] = parsed["ITEM"].astype(str).apply(resolve_upc)

        # 4) compute UNIT/RETAIL/NOW/DELTA
        parsed["UNIT"] = parsed["UNIT"].astype(float)
        parsed["RETAIL"] = parsed["UNIT"] * 2

        pb_num = pb.copy()
        pb_num["cents"] = pd.to_numeric(pb_num["cents"], errors="coerce")
        pb_num["cost_cents"] = pd.to_numeric(pb_num["cost_cents"], errors="coerce")
        pb_num["cost_qty"] = pd.to_numeric(pb_num["cost_qty"], errors="coerce")

        def now_for_upc(u: str) -> float:
            if not u or u.startswith("No Match"):
                return np.nan
            key = self._lz_strip(u)
            m = pb_num.loc[pb_num["_Upc_norm"] == key, "cents"]
            if m.empty or m.isna().all():
                return np.nan
            return float(m.iloc[0]) / 100.0

        def delta_for_upc(u: str, unit_price: float) -> float:
            if not u or u.startswith("No Match") or unit_price is None or np.isnan(unit_price):
                return np.nan
            key = self._lz_strip(u)
            row = pb_num.loc[pb_num["_Upc_norm"] == key, ["cost_cents","cost_qty"]]
            if row.empty:
                return np.nan
            cc = row["cost_cents"].iloc[0]
            cq = row["cost_qty"].iloc[0]
            if pd.isna(cc) or pd.isna(cq) or cq <= 0:
                return np.nan
            return unit_price - (float(cc) / float(cq) / 100.0)

        parsed["NOW"] = parsed["UPC"].apply(now_for_upc)
        parsed["DELTA"] = [delta_for_upc(u, up) for u, up in zip(parsed["UPC"], parsed["UNIT"])]

        parsed = parsed[["UPC","DESCRIPTION","PACK","COST","UNIT","RETAIL","NOW","DELTA","ITEM"]]

        # 5) POS slice (matched UPCs only)
        matched = parsed[~parsed["UPC"].astype(str).str.startswith("No Match")].copy()
        if matched.empty:
            pos_slice = pd.DataFrame(columns=pb.columns)
        else:
            upd = pd.DataFrame({
                "Upc": matched["UPC"].astype(str).str.replace(r"\D","",regex=True).str.zfill(12),
                "cost_qty": matched["PACK"].astype(int),
                "cost_cents": (matched["COST"].astype(float) * 100.0).round().astype(int),
            })
            base = pb.copy()
            base["Upc"] = base["Upc"].astype(str).str.replace(r"\D","",regex=True)
            upd["Upc"] = upd["Upc"].astype(str).str.replace(r"\D","",regex=True)
            pos_slice = base.merge(upd, on="Upc", how="inner", suffixes=("", "_NEW"))
            if not pos_slice.empty:
                pos_slice["cost_qty"] = pos_slice["cost_qty_NEW"].combine_first(pos_slice["cost_qty"])
                pos_slice["cost_cents"] = pos_slice["cost_cents_NEW"].combine_first(pos_slice["cost_cents"])
                pos_slice.drop(columns=[c for c in pos_slice.columns if c.endswith("_NEW")], inplace=True)

        return parsed, pos_slice
