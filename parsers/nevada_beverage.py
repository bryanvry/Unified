# parsers/nevada_beverage.py
# Nevada Beverage PDF-only parser (strict ITEM#/QTY rule)
#  • Every item line starts with 5–6 digit ITEM# (e.g., "53218 5 BUD LT 18PK CAN 018200532184 15.00 0.00 75.00")
#  • Quantity = the integer immediately following the 5–6 digit ITEM#
#  • If quantity == 0 → treat as OOS and skip
#  • UPC = first 12–13 digit token (normalized to 12 digits)
#  • Cost = first non-zero price AFTER the UPC (this is D.PRICE, not DEP=0.00 or EXT)
#  • Order preserved
#
# Fallbacks:
#  • If strict line parse yields nothing, try table and word-grid strategies.

from typing import List, Optional, Tuple
import re
import numpy as np
import pandas as pd

try:
    import pdfplumber
except Exception:
    pdfplumber = None


class NevadaBeverageParser:
    WANT_COLS = ["UPC", "Item Name", "Cost", "Cases"]
    _MIN_PRICE = 0.01

    # ----------------- helpers -----------------
    @staticmethod
    def _digits_only(s: str) -> str:
        return re.sub(r"\D", "", str(s)) if s is not None else ""

    @classmethod
    def _extract_upc_token(cls, s: str) -> str:
        """
        Find a 12–13 digit token bounded by non-digits.
        - 13 digits starting with '0' => last 12 (UPC-A)
        - 13 digits not starting '0' => first 12
        - 12 digits => use as-is
        - Longer => first 12
        """
        if not s:
            return ""
        text = str(s)

        m = re.search(r"(?<!\d)(\d{12,13})(?!\d)", text)
        if not m:
            m = re.search(r"(\d{10,})", text)
            if not m:
                return ""
            run = m.group(1)
        else:
            run = m.group(1)

        if len(run) == 12:
            return run
        if len(run) == 13:
            return run[1:] if run.startswith("0") else run[:12]
        return run[:12]

    @staticmethod
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

    @staticmethod
    def _to_int(x):
        try:
            return int(round(float(str(x).strip())))
        except Exception:
            return 0

    # ----------------- strict line parser -----------------
    def _parse_text_regex_strict(self, page) -> pd.DataFrame:
        """
        Strict NV rule per user:
          line = ITEM#(5–6 digits) <space> QTY(int) <space> DESCRIPTION ... UPC ... D.PRICE ... DEP(0.00) ... EXT
          - QTY = the integer immediately after the ITEM#
          - If QTY == 0 -> skip
          - UPC = first 12–13 digit token
          - COST = first non-zero price AFTER the UPC
          - DESCRIPTION = text between end-of-QTY and start-of-UPC
        """
        txt = page.extract_text() or ""
        if not txt.strip():
            return pd.DataFrame(columns=self.WANT_COLS)

        rows = []
        for li, raw in enumerate(txt.splitlines()):
            line = raw.strip()
            if len(line) < 8:
                continue

            # 1) Find first 5–6 digit ITEM#
            m_item = re.search(r"\b(\d{5,6})\b", line)
            if not m_item:
                continue

            # 2) Quantity = next integer after ITEM#
            after_item = line[m_item.end():]
            m_qty = re.search(r"\b(\d{1,3})\b", after_item)
            if not m_qty:
                continue
            qty = self._to_int(m_qty.group(1))
            if qty <= 0:
                # Explicitly skip zero-arrival (OOS)
                continue

            qty_end_abs = m_item.end() + m_qty.end()  # absolute index in full line

            # 3) UPC token (first 12–13 digit token) anywhere AFTER qty
            after_qty = line[qty_end_abs:]
            m_upc = re.search(r"(?<!\d)(\d{12,13})(?!\d)", after_qty)
            if not m_upc:
                # allow longer number blobs as last resort
                m_upc = re.search(r"(\d{10,})", after_qty)
                if not m_upc:
                    continue
            upc_span_abs = (qty_end_abs + m_upc.start(), qty_end_abs + m_upc.end())
            upc = self._extract_upc_token(m_upc.group(1))
            if not upc:
                continue

            # 4) Cost (D.PRICE) = first non-zero price AFTER UPC
            after_upc = line[upc_span_abs[1]:]
            prices_after = [self._to_float(mm.group(1)) for mm in re.finditer(r"(\d+\.\d{2})", after_upc)]
            cost = None
            for p in prices_after:
                if p is not None and not np.isnan(p) and p >= self._MIN_PRICE:
                    cost = float(p)
                    break
            if cost is None:
                continue

            # 5) Description = between end-of-QTY and start-of-UPC
            desc = line[qty_end_abs:upc_span_abs[0]].strip()
            desc = re.sub(r"\s{2,}", " ", desc).strip(" -–—|;,")

            rows.append({"UPC": upc, "Item Name": desc, "Cost": cost, "Cases": int(qty), "_order": li})

        if not rows:
            return pd.DataFrame(columns=self.WANT_COLS)

        out = pd.DataFrame(rows).sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)
        return out[self.WANT_COLS]

    # ----------------- table/word-grid fallbacks (unchanged) -----------------
    def _match_header(self, columns: List[str]) -> Optional[dict]:
        def canon(s: str) -> str:
            return re.sub(r"[^a-z0-9]", "", s.lower()) if s is not None else ""
        cmap = {canon(c): c for c in columns}

        def pick(*aliases):
            for a in aliases:
                ca = canon(a)
                if ca in cmap:
                    return cmap[ca]
            for a in aliases:
                ca = canon(a)
                for k, orig in cmap.items():
                    if ca in k:
                        return orig
            return None

        col_qty   = pick("QTY", "Qty", "Quantity")
        col_desc  = pick("DESCRIPTION", "Description", "Item Description", "Desc")
        col_upc   = pick("U.P.C.", "UPC", "U P C", "U.P.C")
        col_price = pick("D.PRICE", "D PRICE", "DPRICE", "UNITPRICE", "UNIT PRICE", "PRICE")
        if all([col_qty, col_desc, col_upc, col_price]):
            return {"QTY": col_qty, "DESCRIPTION": col_desc, "U.P.C.": col_upc, "D.PRICE": col_price}
        return None

    def _parse_pdf_tables(self, pdf: "pdfplumber.PDF") -> pd.DataFrame:
        rows = []
        table_settings_list = [
            {"vertical_strategy": "lines", "horizontal_strategy": "lines", "intersection_tolerance": 5, "snap_tolerance": 3, "join_tolerance": 3, "edge_min_length": 3},
            {"vertical_strategy": "text", "horizontal_strategy": "text"},
        ]
        for page in pdf.pages:
            for settings in table_settings_list:
                try:
                    tables = page.extract_tables(table_settings=settings)
                except Exception:
                    tables = []
                for tbl in tables or []:
                    if not tbl or len(tbl) < 2:
                        continue
                    header = None
                    header_idx = 0
                    for i, row in enumerate(tbl[:6]):
                        if any(str(x or "").strip() for x in row):
                            header = [str(x or "").strip() for x in row]
                            header_idx = i
                            break
                    if header is None:
                        continue
                    colmap = self._match_header(header)
                    if not colmap:
                        continue
                    body = tbl[header_idx + 1 :]
                    if not body:
                        continue
                    df = pd.DataFrame(body, columns=header)
                    for _, r in df.iterrows():
                        upc_raw = r.get(colmap["U.P.C."], "")
                        name    = r.get(colmap["DESCRIPTION"], "")
                        price   = r.get(colmap["D.PRICE"], "")
                        qty     = r.get(colmap["QTY"], "")
                        upc   = self._extract_upc_token(upc_raw)
                        cost  = self._to_float(price)
                        cases = self._to_int(qty)
                        name  = str(name).strip()
                        if upc and cost is not None and not np.isnan(cost) and cost >= self._MIN_PRICE and cases > 0:
                            rows.append({"UPC": upc, "Item Name": name, "Cost": float(cost), "Cases": int(cases)})
        if not rows:
            return pd.DataFrame(columns=self.WANT_COLS)
        out = pd.DataFrame(rows, columns=self.WANT_COLS)
        out.reset_index(drop=True, inplace=True)
        return out

    def _find_header_spans(self, page) -> Optional[Tuple[float, dict]]:
        words = page.extract_words(keep_blank_chars=False, use_text_flow=True)
        if not words:
            return None

        def canon(s: str) -> str:
            return re.sub(r"[^a-z0-9]", "", s.lower()) if s is not None else ""

        lines = {}
        for w in words:
            key = round(float(w["top"]) / 2.0, 0)
            lines.setdefault(key, []).append(w)

        header_key = None
        best_score = 0
        for key, ws in lines.items():
            txt = " ".join([w["text"] for w in sorted(ws, key=lambda x: x["x0"])])
            ctxt = canon(txt)
            score = sum(a in ctxt for a in ["qty", "description", "desc", "upc", "dprice", "price", "unitprice"])
            if score >= 3 and score >= best_score:
                best_score = score
                header_key = key

        if header_key is None:
            return None

        header_words = sorted(lines[header_key], key=lambda x: x["x0"])

        def nearest_xcenter(*aliases):
            for target in aliases:
                tgt = canon(target)
                for w in header_words:
                    if tgt in canon(w["text"]):
                        return (w["x0"] + w["x1"]) / 2.0
            if header_words:
                return float(np.median([(w["x0"] + w["x1"]) / 2.0 for w in header_words]))
            return None

        x_qty   = nearest_xcenter("QTY", "Qty", "Quantity")
        x_desc  = nearest_xcenter("DESCRIPTION", "Description", "Desc")
        x_upc   = nearest_xcenter("U.P.C.", "UPC")
        x_price = nearest_xcenter("D.PRICE", "D PRICE", "UNIT PRICE", "PRICE")

        colmap = {"qty": x_qty, "desc": x_desc, "upc": x_upc, "price": x_price}
        header_y = float(np.mean([w["top"] for w in header_words]))
        if sum(v is not None for v in colmap.values()) < 3:
            return None
        return header_y, colmap

    def _bucket_line(self, words_line, col_x):
        buckets = {"qty": [], "desc": [], "upc": [], "price": []}
        for w in sorted(words_line, key=lambda x: x["x0"]):
            xc = (w["x0"] + w["x1"]) / 2.0
            best = None
            best_dx = 1e9
            for key, x in col_x.items():
                if x is None:
                    continue
                dx = abs(xc - x)
                if dx < best_dx:
                    best = key
                    best_dx = dx
            if best is None:
                best = "desc"
            buckets[best].append(w["text"])
        return {k: " ".join(v).strip() for k, v in buckets.items()}

    def _parse_word_grid(self, page) -> pd.DataFrame:
        words = page.extract_words(keep_blank_chars=False, use_text_flow=True)
        if not words:
            return pd.DataFrame(columns=self.WANT_COLS)

        found = self._find_header_spans(page)
        if not found:
            return pd.DataFrame(columns=self.WANT_COLS)
        header_y, col_x = found

        lines = {}
        for w in words:
            if w["top"] <= header_y + 1:
                continue
            key = round(w["top"] / 2.0, 0)
            lines.setdefault(key, []).append(w)

        rows = []
        for key in sorted(lines.keys()):
            line_words = lines[key]
            buck = self._bucket_line(line_words, col_x)
            qty_txt   = buck.get("qty", "")
            desc_txt  = buck.get("desc", "")
            upc_txt   = buck.get("upc", "")
            price_txt = buck.get("price", "")

            prices_in_bucket = [self._to_float(m) for m in re.findall(r"\d+\.\d{2}", price_txt or "")]
            qty_val = self._to_int(qty_txt)
            if qty_val <= 0:
                continue

            if not upc_txt and re.search(r"\d", desc_txt or ""):
                m = re.search(r"(?<!\d)(\d{12,13})(?!\d)", desc_txt)
                if m:
                    upc_txt = m.group(1)
                else:
                    m2 = re.search(r"(\d{10,})", desc_txt)
                    if m2:
                        upc_txt = m2.group(1)

            upc = self._extract_upc_token(upc_txt)

            unit_price = None
            if prices_in_bucket:
                vals = [p for p in prices_in_bucket if p is not None and not np.isnan(p) and p >= self._MIN_PRICE]
                if vals:
                    unit_price = min(vals)

            if not upc or unit_price is None:
                continue

            rows.append({"UPC": upc, "Item Name": desc_txt.strip(), "Cost": float(unit_price), "Cases": int(qty_val)})

        if not rows:
            return pd.DataFrame(columns=self.WANT_COLS)
        return pd.DataFrame(rows, columns=self.WANT_COLS)

    # ----------------- public API -----------------
    def parse(self, uploaded_file) -> pd.DataFrame:
        if pdfplumber is None:
            return pd.DataFrame(columns=self.WANT_COLS)

        try:
            uploaded_file.seek(0)
        except Exception:
            pass

        name = (getattr(uploaded_file, "name", "") or "").lower()
        if not name.endswith(".pdf"):
            return pd.DataFrame(columns=self.WANT_COLS)

        try:
            with pdfplumber.open(uploaded_file) as pdf:
                # Try STRICT text regex first (based on 5–6 digit ITEM# then QTY)
                items = []
                for page in pdf.pages:
                    df = self._parse_text_regex_strict(page)
                    if not df.empty:
                        items.append(df)
                items = pd.concat(items, ignore_index=True) if items else pd.DataFrame(columns=self.WANT_COLS)

                # Fallback: tables
                if items.empty:
                    items = self._parse_pdf_tables(pdf)

                # Fallback: word-grid
                if items.empty:
                    rows = []
                    for page in pdf.pages:
                        pg = self._parse_word_grid(page)
                        if not pg.empty:
                            rows.append(pg)
                    items = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=self.WANT_COLS)
        except Exception:
            items = pd.DataFrame(columns=self.WANT_COLS)

        if items.empty:
            return pd.DataFrame(columns=self.WANT_COLS)

        # Normalize and filter
        items["UPC"] = items["UPC"].astype(str).str.replace(r"\D", "", regex=True).str[:12].str.zfill(12)
        items["Item Name"] = items["Item Name"].astype(str).str.strip()
        items["Cost"] = pd.to_numeric(items["Cost"], errors="coerce")
        items["Cases"] = pd.to_numeric(items["Cases"], errors="coerce").fillna(0).astype(int)

        items = items[(items["UPC"] != "") & items["Cost"].notna() & (items["Cost"] >= self._MIN_PRICE) & (items["Cases"] > 0)].copy()
        items.reset_index(drop=True, inplace=True)
        return items[self.WANT_COLS]
