# parsers/jcsales.py
# JC Sales PDF parser
# - Tolerant of optional leading flag (T/C/etc.)
# - ITEM is 3–6 digits (may have leading zeros)
# - Columns in PDF (per text): ITEM DESCRIPTION R-QTY S-QTY UM #/UM UNIT_P UM_P EXT_P
#   Example line (from OCR/text):
#     "T 14158 AXION DISH LIQUID LEMON 900ML 1 1 12PK 2.39 28.68 28.68"
#   or without flag:
#     "118815 TOY DOCTOR PLAY SET ASST COLOR 1 1 24PK 0.85 20.40 20.40"
#
# Output "parsed_<invoice>.xlsx" sheet "parsed" with columns:
#   ["UPC", "DESCRIPTION", "PACK", "COST", "UNIT", "RETAIL", "NOW", "DELTA"]
#
# UPC resolution:
#   - Match ITEM to JC Master (xlsx): pick row with same ITEM.
#   - Choose UPC = UPC1 if (strip-leading-zeros UPC1) appears in Pricebook Upc (also strip-leading-zeros),
#     else UPC2 under same rule; else UPC = f"No Match {ITEM}".
#
# "NOW" = pricebook.cents / 100
# "UNIT" = COST / PACK
# "RETAIL" = UNIT * 2
# "DELTA" = UNIT - (pricebook.cost_cents / pricebook.cost_qty / 100)
#
# POS update CSV (JC Sales tab logic in app.py uses the returned DataFrame):
#   - Keep only matched UPC rows (exclude "No Match ...")
#   - Update cost_qty = PACK, cost_cents = COST*100
#
from __future__ import annotations
import re
from typing import Optional, Tuple

import numpy as np
import pandas as pd

try:
    import pdfplumber
except Exception:
    pdfplumber = None


class JCSalesParser:
    WANT_COLS = ["ITEM", "DESCRIPTION", "PACK", "COST"]

    # -------- utilities --------
    @staticmethod
    def _to_float(x) -> float | None:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return None
        s = str(x).strip().replace("$", "").replace(",", "")
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

    # --- core line regex ---
    # Optional leading flag (one letter) + spaces
    # ITEM = 3–6 digits
    # DESCRIPTION = any text up to R-QTY and S-QTY (two ints)
    # PACK token looks like "12PK" (we capture the number before PK)
    # Followed by UNIT_P, UM_P, EXT_P (floats)
    _LINE_RX = re.compile(
        r"""
        ^
        (?:[A-Z]\s+)?                 # optional leading flag like 'T ' or 'C '
        (?P<item>\d{3,6})\s+          # ITEM number (3–6 digits)
        (?P<desc>.*?)\s+              # DESCRIPTION (greedy, up to numbers)
        (?P<rqty>\d+)\s+              # R-QTY
        (?P<sqty>\d+)\s+              # S-QTY
        (?P<pack>\d+)\s*PK\s+         # '#/UM' as e.g. "12PK"
        (?P<unit_p>\d+\.\d{2})\s+     # UNIT_P
        (?P<um_p>\d+\.\d{2})\s+       # UM_P (case cost)
        (?P<ext_p>\d+\.\d{2})         # EXT_P
        \s*$
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    def _parse_pdf_lines(self, pdf) -> pd.DataFrame:
        rows = []
        for pidx, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            if not text.strip():
                continue
            for li, raw in enumerate(text.splitlines()):
                line = raw.strip()
                if len(line) < 8:
                    continue
                m = self._LINE_RX.match(line)
                if not m:
                    continue

                item = m.group("item")
                desc = re.sub(r"\s{2,}", " ", m.group("desc")).strip()
                rqty = self._to_int(m.group("rqty")) or 0
                sqty = self._to_int(m.group("sqty")) or 0
                pack = self._to_int(m.group("pack")) or 0
                unit_p = self._to_float(m.group("unit_p"))
                um_p = self._to_float(m.group("um_p"))
                # ext_p = self._to_float(m.group("ext_p"))  # not used downstream

                # We require successful shipment (S-QTY>0), PACK>0, and a usable COST
                if sqty <= 0 or pack <= 0 or um_p is None:
                    continue

                rows.append(
                    {
                        "ITEM": item,
                        "DESCRIPTION": desc,
                        "PACK": int(pack),
                        "COST": float(um_p),
                        "_order": len(rows),
                    }
                )

        if not rows:
            return pd.DataFrame(columns=self.WANT_COLS)

        out = pd.DataFrame(rows).sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)
        return out[self.WANT_COLS]

    # --------- public API ----------
    def parse_invoice(
        self,
        invoice_pdf,              # UploadedFile (pdf)
        jc_master_xlsx,           # UploadedFile (xlsx)
        pricebook_csv,            # UploadedFile (csv)
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Returns:
          parsed_df  -> columns: ["UPC","DESCRIPTION","PACK","COST","UNIT","RETAIL","NOW","DELTA","ITEM"]
          pos_slice  -> DF of pricebook rows for invoice UPCs (matched only), with cost_qty/cost_cents updated
        """
        if pdfplumber is None:
            return pd.DataFrame(), pd.DataFrame()

        # --- 1) Parse lines from PDF
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

        # --- 2) Load JC master + pricebook
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

        # normalize Upc for matching (strip leading zeros)
        pb["_Upc_norm"] = pb["Upc"].astype(str).str.replace(r"\D", "", regex=True).apply(self._lz_strip)

        # --- 3) Map ITEM -> (UPC1, UPC2) from master, then pick one that exists in pricebook
        #       If neither exists in PB, use "No Match <ITEM>"
        if not jc_master.empty:
            # Expect columns: ITEM, UPC1, UPC2 (case-insensitive safe)
            mcols = {c.lower(): c for c in jc_master.columns}
            col_item = mcols.get("item")
            col_u1 = mcols.get("upc1")
            col_u2 = mcols.get("upc2")

            if col_item:
                key = jc_master[col_item].astype(str).str.strip()
                u1 = jc_master[col_u1].astype(str) if col_u1 else pd.Series([""] * len(jc_master))
                u2 = jc_master[col_u2].astype(str) if col_u2 else pd.Series([""] * len(jc_master))
                master_map = (
                    pd.DataFrame(
                        {
                            "ITEM": key,
                            "UPC1": u1.fillna(""),
                            "UPC2": u2.fillna(""),
                        }
                    )
                    .drop_duplicates("ITEM")
                    .set_index("ITEM")
                )
            else:
                master_map = pd.DataFrame(columns=["UPC1", "UPC2"])
        else:
            master_map = pd.DataFrame(columns=["UPC1", "UPC2"])

        # helper to pick UPC
        def resolve_upc(item_str: str) -> str:
            if item_str in master_map.index:
                upc1 = str(master_map.loc[item_str, "UPC1"])
                upc2 = str(master_map.loc[item_str, "UPC2"])
            else:
                upc1 = upc2 = ""

            u1n = self._lz_strip(upc1)
            u2n = self._lz_strip(upc2)

            has_u1 = (u1n != "0") and (u1n in set(pb["_Upc_norm"]))
            has_u2 = (u2n != "0") and (u2n in set(pb["_Upc_norm"]))

            if has_u1:
                # return original 12+ digits with leading zeros kept as in master (but ensure 12 if it looks like UPC-A)
                return re.sub(r"\D", "", upc1).zfill(12)
            if has_u2:
                return re.sub(r"\D", "", upc2).zfill(12)
            return f"No Match {item_str}"

        # --- 4) Build parsed output with calcs
        parsed = body.copy()
        parsed["UPC"] = parsed["ITEM"].astype(str).apply(resolve_upc)

        parsed["UNIT"] = parsed["COST"] / parsed["PACK"]
        parsed["RETAIL"] = parsed["UNIT"] * 2

        # NOW from pricebook "cents"
        pb_num = pb.copy()
        pb_num["cents"] = pd.to_numeric(pb_num["cents"], errors="coerce")  # retail cents
        pb_num["_Upc_norm"] = pb_num["_Upc_norm"].fillna("")

        def now_for_upc(u: str) -> float:
            if not u or u.startswith("No Match"):
                return np.nan
            u_norm = self._lz_strip(u)
            m = pb_num.loc[pb_num["_Upc_norm"] == u_norm, "cents"]
            if m.empty or m.isna().all():
                return np.nan
            return float(m.iloc[0]) / 100.0

        parsed["NOW"] = parsed["UPC"].apply(now_for_upc)

        # DELTA = UNIT - (cost_cents / cost_qty / 100)
        pb_num["cost_cents"] = pd.to_numeric(pb_num["cost_cents"], errors="coerce")
        pb_num["cost_qty"] = pd.to_numeric(pb_num["cost_qty"], errors="coerce")

        def delta_for_upc(u: str, unit_price: float) -> float | None:
            if not u or u.startswith("No Match") or unit_price is None or np.isnan(unit_price):
                return np.nan
            u_norm = self._lz_strip(u)
            rows = pb_num.loc[pb_num["_Upc_norm"] == u_norm, ["cost_cents", "cost_qty"]]
            if rows.empty:
                return np.nan
            cc = rows["cost_cents"].iloc[0]
            cq = rows["cost_qty"].iloc[0]
            if pd.isna(cc) or pd.isna(cq) or cq <= 0:
                return np.nan
            return float(unit_price) - float(cc) / float(cq) / 100.0

        parsed["DELTA"] = [
            delta_for_upc(u, unitv) for u, unitv in zip(parsed["UPC"], parsed["UNIT"])
        ]

        # Order + final columns
        out_cols = ["UPC", "DESCRIPTION", "PACK", "COST", "UNIT", "RETAIL", "NOW", "DELTA", "ITEM"]
        parsed = parsed[out_cols]

        # --- 5) POS slice (only matched UPCs)
        matched = parsed[~parsed["UPC"].str.startswith("No Match")].copy()
        if matched.empty:
            pos_slice = pd.DataFrame(columns=pb.columns)
        else:
            # Update cost_qty and cost_cents for the matched UPCs
            upd = pd.DataFrame(
                {
                    "Upc": matched["UPC"].astype(str).str.zfill(12),
                    "cost_qty": matched["PACK"].astype(int),
                    "cost_cents": (matched["COST"].astype(float) * 100.0).round().astype(int),
                }
            )

            # left-join into pricebook, but keep only rows that exist in pricebook
            pb_lz = pb.copy()
            pb_lz["Upc"] = pb_lz["Upc"].astype(str).str.replace(r"\D", "", regex=True)
            upd["Upc"] = upd["Upc"].astype(str).str.replace(r"\D", "", regex=True)

            pos_slice = pb_lz.merge(upd, on="Upc", how="inner", suffixes=("", "_NEW"))
            if not pos_slice.empty:
                pos_slice["cost_qty"] = pos_slice["cost_qty_NEW"].combine_first(pos_slice["cost_qty"])
                pos_slice["cost_cents"] = pos_slice["cost_cents_NEW"].combine_first(pos_slice["cost_cents"])
                pos_slice.drop(columns=[c for c in pos_slice.columns if c.endswith("_NEW")], inplace=True)

        return parsed, pos_slice
