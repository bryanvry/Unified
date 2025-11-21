# parsers/jcsales.py
from __future__ import annotations
import re
import numpy as np
import pandas as pd

try:
    import pdfplumber
except Exception:
    pdfplumber = None


class JCSalesParser:
    """
    Robust JC Sales PDF parser using header-anchored word-grid bucketing.

    Output: (rows_df, invoice_no)
      rows_df columns: ["LINE","ITEM","DESCRIPTION","R_QTY","S_QTY","UM","PACK","UNIT_P","UM_P","EXT_P"]
      invoice_no: like "OSI014135"
    """

    WANT_COLS = ["LINE","ITEM","DESCRIPTION","R_QTY","S_QTY","UM","PACK","UNIT_P","UM_P","EXT_P"]

    # ---------- small utils ----------
    @staticmethod
    def _to_int(x, default=np.nan):
        s = str(x).strip()
        if s == "":
            return default
        try:
            return int(float(s))
        except Exception:
            return default

    @staticmethod
    def _to_float(x, default=np.nan):
        if x is None:
            return default
        if isinstance(x, (int, float, np.number)):
            return float(x)
        s = str(x)
        s = s.replace(",", "").replace("$", "").strip()
        m = re.search(r"-?\d+(?:\.\d+)?", s)
        if not m:
            return default
        try:
            return float(m.group(0))
        except Exception:
            return default

    @staticmethod
    def _canon(s: str) -> str:
        return re.sub(r"[^a-z0-9]", "", s.lower()) if s is not None else ""

    # ---------- header detection ----------
    def _find_header(self, page):
        """
        Find the header line: "LINE # ITEM DESCRIPTION Crv R-QTY S-QTY UM #/UM UNIT_P UM_P EXT_P"
        Return (header_y, x_centers_dict)
        """
        words = page.extract_words(keep_blank_chars=False, use_text_flow=True) or []
        if not words:
            return None

        # group words into rough text lines by Y
        lines = {}
        for w in words:
            key = round(float(w["top"]) / 2.0, 0)
            lines.setdefault(key, []).append(w)

        best_key = None
        best_score = -1
        for key, ws in lines.items():
            txt = " ".join(w["text"] for w in sorted(ws, key=lambda z: z["x0"]))
            c = self._canon(txt)
            # look for several header tokens to score header-ish lines
            score = 0
            for token in ["line", "item", "description", "rq", "sq", "um", "um", "unitp", "ump", "extp"]:
                if token in c:
                    score += 1
            if score > best_score:
                best_score = score
                best_key = key

        if best_key is None or best_score < 4:
            return None

        hdr_words = sorted(lines[best_key], key=lambda x: x["x0"])
        header_y = float(np.mean([w["top"] for w in hdr_words]))

        def nearest_center(*aliases):
            # return x-center of the first alias word found; if not found, None
            for name in aliases:
                tgt = self._canon(name)
                for w in hdr_words:
                    if tgt in self._canon(w["text"]):
                        return (w["x0"] + w["x1"]) / 2.0
            return None

        x_line = nearest_center("LINE")
        x_item = nearest_center("ITEM")
        x_desc = nearest_center("DESCRIPTION", "DESC")
        x_rq   = nearest_center("R-QTY", "RQTY", "R QTY", "R- QTY")
        x_sq   = nearest_center("S-QTY", "SQTY", "S QTY", "S- QTY")
        x_um   = nearest_center("UM")
        x_pack = nearest_center("#/UM", "NUM/UM", "HASH/UM", "# / UM")
        x_unitp= nearest_center("UNIT_P", "UNIT P", "UNITP")
        x_ump  = nearest_center("UM_P", "UM P", "UMP")
        x_extp = nearest_center("EXT_P", "EXT P", "EXTP")

        # If any are None, approximate by using neighbors we did find
        xs = {
            "LINE": x_line,
            "ITEM": x_item,
            "DESCRIPTION": x_desc,
            "R_QTY": x_rq,
            "S_QTY": x_sq,
            "UM": x_um,
            "PACK": x_pack,
            "UNIT_P": x_unitp,
            "UM_P": x_ump,
            "EXT_P": x_extp,
        }
        # If DESCRIPTION missing but ITEM present, set desc between ITEM and R_QTY
        if xs["DESCRIPTION"] is None and xs["ITEM"] is not None and xs["R_QTY"] is not None:
            xs["DESCRIPTION"] = (xs["ITEM"] + xs["R_QTY"]) / 2.0

        # Require at least ITEM, DESCRIPTION, PACK (or UM_P), EXT_P-ish to proceed
        have = sum(v is not None for v in xs.values())
        if have < 5 or xs["ITEM"] is None or xs["DESCRIPTION"] is None:
            return None
        return header_y, xs

    # ---------- bucket a physical line of words into columns ----------
    def _bucket_words(self, words_line, xs):
        buckets = {k: [] for k in ["LINE","ITEM","DESCRIPTION","R_QTY","S_QTY","UM","PACK","UNIT_P","UM_P","EXT_P"]}
        for w in sorted(words_line, key=lambda z: z["x0"]):
            xc = (w["x0"] + w["x1"]) / 2.0
            best = None
            best_dx = 1e9
            for col, xcenter in xs.items():
                if xcenter is None:
                    continue
                dx = abs(xc - xcenter)
                if dx < best_dx:
                    best = col
                    best_dx = dx
            if best is None:
                best = "DESCRIPTION"
            buckets[best].append(w["text"])
        return {k: " ".join(v).strip() for k, v in buckets.items()}

    # ---------- one page to rows ----------
    def _parse_page(self, page) -> list[dict]:
        found = self._find_header(page)
        if not found:
            return []
        header_y, xs = found

        words = page.extract_words(keep_blank_chars=False, use_text_flow=True) or []
        # Take only words below the header
        words = [w for w in words if w["top"] > header_y + 1]

        # group into physical rows by y
        lines = {}
        for w in words:
            key = round(float(w["top"]) / 2.2, 0)  # slight spread to keep wrapped bits together
            lines.setdefault(key, []).append(w)

        rows = []
        for key in sorted(lines.keys()):
            buck = self._bucket_words(lines[key], xs)

            # Normalize fields
            line_no = self._to_int(buck.get("LINE", "").split()[0], default=np.nan)
            item    = buck.get("ITEM", "").strip()
            desc    = buck.get("DESCRIPTION", "").strip()

            rq     = self._to_int(buck.get("R_QTY", ""), default=np.nan)
            sq     = self._to_int(buck.get("S_QTY", ""), default=np.nan)
            um     = buck.get("UM", "").strip() or "PK"
            pack   = self._to_int(buck.get("PACK", ""), default=np.nan)

            unit_p = self._to_float(buck.get("UNIT_P", ""), default=np.nan)
            um_p   = self._to_float(buck.get("UM_P", ""), default=np.nan)
            ext_p  = self._to_float(buck.get("EXT_P", ""), default=np.nan)

            # Heuristics: if UNIT_P empty but UM_P present and PACK present:
            #   Often UNIT_P = UM_P / PACK
            if (np.isnan(unit_p) or unit_p is None) and (not np.isnan(um_p)) and (not np.isnan(pack)) and pack not in (0, np.nan):
                unit_p = um_p / float(pack)

            # If PACK missed into DESCRIPTION tail (e.g., " ... 60"), rescue a trailing integer
            if (np.isnan(pack) or pack is None) and desc:
                m = re.search(r"(\d+)$", desc)
                if m:
                    pack = self._to_int(m.group(1), default=np.nan)
                    # remove trailing number from description to keep clean
                    desc = re.sub(r"\s*\d+$", "", desc).strip()

            # Skip rows that are clearly not data lines
            if not item or not desc:
                continue

            # Require at least PACK and UM_P or EXT_P to consider valid
            # (EXT_P exists on every line on your sample; use it to filter header junk)
            if np.isnan(pack) and np.isnan(um_p) and np.isnan(ext_p):
                continue

            rows.append({
                "LINE": line_no,
                "ITEM": item,
                "DESCRIPTION": desc,
                "R_QTY": rq,
                "S_QTY": sq,
                "UM": um,
                "PACK": pack,
                "UNIT_P": unit_p,
                "UM_P": um_p,
                "EXT_P": ext_p,
            })
        return rows

    # ---------- invoice number ----------
    def _extract_invoice_no(self, page) -> str | None:
        txt = page.extract_text() or ""
        # usually appears as *OSI014135* or "JCSALES OSI014135 ..."
        m = re.search(r"OSI\d{6,}", txt)
        if m:
            return m.group(0)
        m2 = re.search(r"\*([A-Z]{3}\d{6,})\*", txt)
        if m2:
            return m2.group(1)
        return None

    # ---------- public entry ----------
    def parse(self, uploaded_pdf):
        if pdfplumber is None:
            return pd.DataFrame(columns=self.WANT_COLS), None

        try:
            uploaded_pdf.seek(0)
        except Exception:
            pass

        inv_no = None
        all_rows = []
        try:
            with pdfplumber.open(uploaded_pdf) as pdf:
                for i, page in enumerate(pdf.pages):
                    if inv_no is None:
                        inv_no = self._extract_invoice_no(page)
                    rows = self._parse_page(page)
                    if rows:
                        all_rows.extend(rows)
        except Exception:
            return pd.DataFrame(columns=self.WANT_COLS), None

        if not all_rows:
            return pd.DataFrame(columns=self.WANT_COLS), inv_no

        df = pd.DataFrame(all_rows)

        # Clean up and order:
        # - LINE: fill incremental if NaN (some pages miss it)
        if "LINE" in df.columns:
            if df["LINE"].isna().any():
                # fill missing with running index in display order
                df = df.sort_values(by=["LINE"], na_position="last").reset_index(drop=True)
                start = 1
                for i in range(len(df)):
                    if pd.isna(df.at[i, "LINE"]):
                        df.at[i, "LINE"] = start
                    start += 1
                df["LINE"] = df["LINE"].astype(int)
        else:
            df.insert(0, "LINE", range(1, len(df) + 1))

        # Coerce numeric
        for c in ["R_QTY","S_QTY","PACK"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        for c in ["UNIT_P","UM_P","EXT_P"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        # Drop any obvious header/garbage remnants (no ITEM or no DESCRIPTION or PACK missing)
        df = df[df["ITEM"].astype(str).str.strip() != ""]
        df = df[df["DESCRIPTION"].astype(str).str.strip() != ""]
        # PACK is required for parsed sheet math; keep those that have it
        df = df[df["PACK"].notna()]

        # Sort by LINE to preserve invoice order
        df = df.sort_values("LINE").reset_index(drop=True)

        # Keep only expected columns and order
        keep = [c for c in self.WANT_COLS if c in df.columns]
        df = df[keep]

        return df, inv_no
