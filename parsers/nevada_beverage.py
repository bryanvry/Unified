import re
import pandas as pd
import numpy as np

from .utils import (
    normalize_invoice_upc,
    sanitize_columns,
)

class NevadaBeverageParser:
    """
    Nevada Beverage invoice parser.
    - Accepts PDF, TXT (pasted), XLSX/XLS, CSV
    - Reads item rows after the header that contains 'ITEM#' and 'UPC'/'U.P.C.'
    - Stops when a TOTAL/PAYMENT/SUMMARY section is detected
    - Returns normalized columns:
      ["invoice_date","UPC","Brand","Description","Pack","Size","Cost","+Cost","Case Qty"]
    """
    name = "Nevada Beverage"
    tokens = ["ITEM#", "U.P.C.", "UPC", "QTY", "DESCRIPTION"]

    # --------- helpers ---------
    def _read_lines_from_pdf(self, uploaded_file):
        import pdfplumber
        uploaded_file.seek(0)
        lines = []
        with pdfplumber.open(uploaded_file) as pdf:
            for page in pdf.pages:
                txt = page.extract_text() or ""
                for ln in txt.splitlines():
                    ln = ln.strip()
                    if ln:
                        lines.append(ln)
        return lines

    def _read_lines_from_txt(self, uploaded_file):
        uploaded_file.seek(0)
        txt = uploaded_file.read().decode("utf-8", errors="ignore")
        return [ln.strip() for ln in txt.splitlines() if ln.strip()]

    def _read_lines_from_table(self, uploaded_file):
        name = uploaded_file.name.lower()
        if name.endswith(".csv"):
            df_raw = pd.read_csv(uploaded_file, header=None, dtype=str, keep_default_na=False)
        else:
            df_raw = pd.read_excel(uploaded_file, header=None, dtype=str)

        # locate header with ITEM# and UPC/U.P.C.
        header_row = None
        for i in range(min(120, len(df_raw))):
            row = " ".join([str(x) for x in df_raw.iloc[i].tolist()])
            if "ITEM#" in row.upper() and ("U.P.C." in row.upper() or "UPC" in row.upper()):
                header_row = i
                break
        if header_row is None:
            header_row = 0

        df = df_raw.iloc[header_row + 1 :].fillna("")
        lines = df.apply(
            lambda r: " ".join([str(x) for x in r.tolist() if str(x).strip() != ""]),
            axis=1,
        ).tolist()
        return [ln.strip() for ln in lines if ln.strip()]

    # --------- main parse ---------
    def parse(self, uploaded_file) -> pd.DataFrame:
        name = uploaded_file.name.lower()

        # 1) Load lines
        try:
            if name.endswith(".pdf"):
                lines = self._read_lines_from_pdf(uploaded_file)
            elif name.endswith(".txt"):
                lines = self._read_lines_from_txt(uploaded_file)
            else:
                lines = self._read_lines_from_table(uploaded_file)
        except Exception:
            cols = ["invoice_date","UPC","Brand","Description","Pack","Size","Cost","+Cost","Case Qty"]
            return pd.DataFrame(columns=cols)

        # 2) Iterate lines and extract items
        items = []
        for ln in lines:
            # stop when reaching totals/payment/summary area
            if re.search(r"\b(TOTAL|PAYMENT|SUMMARY)\b", ln, re.I):
                break

            m_upc  = re.search(r"(?:UPC|U\.P\.C\.)[:\s]*([0-9\- ]+)", ln, re.I)
            m_desc = re.search(r"ITEM#\s*\S+\s+(.+)", ln)
            m_cost = re.search(r"\$([0-9\.,]+)", ln)
            m_date = re.search(r"Invoice Date[:\s]*([0-9/\-]+)", ln, re.I)

            if m_upc:
                upc = normalize_invoice_upc(m_upc.group(1))
                desc = (m_desc.group(1).strip() if m_desc else "")
                cost = float(m_cost.group(1).replace(",", "")) if m_cost else np.nan
                items.append({
                    "invoice_date": (m_date.group(1).strip() if m_date else None),
                    "UPC": upc,
                    "Brand": "",
                    "Description": desc,
                    "Pack": np.nan,    # NV process doesn't use a pack value
                    "Size": "",
                    "Cost": cost,
                    "+Cost": cost,     # treat item amount as +Cost
                    "Case Qty": pd.NA, # unknown/not needed for NV export
                })

        # 3) Normalize frame
        out = pd.DataFrame(items)
        if not out.empty:
            out["invoice_date"] = pd.to_datetime(out["invoice_date"], errors="coerce").dt.date
        cols = ["invoice_date","UPC","Brand","Description","Pack","Size","Cost","+Cost","Case Qty"]
        for c in cols:
            if c not in out.columns:
                out[c] = "" if c in ["Brand","Description","Size"] else pd.NA
        out = out[cols]
        return sanitize_columns(out)
