import re
import pandas as pd
import numpy as np

# shared helpers
from .utils import (
    normalize_invoice_upc,
    first_int_from_text,
    sanitize_columns,
)

class SouthernGlazersParser:
    """
    Southern Glazer's invoice parser.
    - Accepts PDF, TXT (pasted), XLSX/XLS, CSV
    - Uses Unit Net Amount as +Cost
    - Pack from 'CS ORD/DLV' (first integer)
    - Returns normalized columns:
      ["invoice_date","UPC","Brand","Description","Pack","Size","Cost","+Cost","Case Qty"]
    """
    name = "Southern Glazer's"
    tokens = ["ITEM#", "UPC", "SIZE:", "Unit Net Amount", "CS ORD/DLV", "Invoice"]

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
        # Fallback for CSV/XLSX/XLS: collapse each row into a single string line
        name = uploaded_file.name.lower()
        if name.endswith(".csv"):
            df_raw = pd.read_csv(uploaded_file, header=None, dtype=str, keep_default_na=False)
        else:
            df_raw = pd.read_excel(uploaded_file, header=None, dtype=str)

        # try to locate header area (row that contains ITEM# and UPC)
        header_row = None
        for i in range(min(100, len(df_raw))):
            row = " ".join([str(x) for x in df_raw.iloc[i].tolist()])
            if "ITEM#" in row.upper() and "UPC" in row.upper():
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

        # 1) Load lines from the source (PDF/TXT vs table files)
        try:
            if name.endswith(".pdf"):
                lines = self._read_lines_from_pdf(uploaded_file)
            elif name.endswith(".txt"):
                lines = self._read_lines_from_txt(uploaded_file)
            else:
                lines = self._read_lines_from_table(uploaded_file)
        except Exception:
            # as a last resort, return empty normalized df
            cols = ["invoice_date","UPC","Brand","Description","Pack","Size","Cost","+Cost","Case Qty"]
            return pd.DataFrame(columns=cols)

        # 2) Scan lines to extract items
        items = []
        current = {}  # holds fields for the current product block

        for ln in lines:
            # new block starts when we see ITEM#
            if "ITEM#" in ln.upper():
                if current.get("UPC"):
                    items.append(current)
                current = {"Size": "", "Brand": "", "Description": ""}

                # crude first-pass description right after ITEM#
                mdesc = re.search(r"ITEM#\s*\S+\s+(.+)", ln)
                if mdesc and not current.get("Description"):
                    current["Description"] = mdesc.group(1).strip()

            # field matches (order-independent)
            m_upc   = re.search(r"\bUPC[:\s]*([0-9\- ]+)", ln, re.I)
            m_size  = re.search(r"\bSIZE[:\s]*([A-Za-z0-9 .]+)", ln, re.I)
            m_unit  = re.search(r"Unit Net Amount[:\s]*\$?([0-9\.,]+)", ln, re.I)
            m_cs    = re.search(r"CS ORD/DLV[:\s]*([0-9]+(?:/[0-9]+)?)", ln, re.I)
            m_date  = re.search(r"Invoice Date[:\s]*([0-9/\-]+)", ln, re.I)

            if m_upc:
                upc_raw = re.sub(r"[^0-9]", "", m_upc.group(1))
                current["UPC"] = normalize_invoice_upc(upc_raw)

            if m_size:
                sz = m_size.group(1).strip()
                # light normalization: " z"→" oz", "Z"→"oz"
                sz = sz.replace(" z", " oz").replace("Z", "oz")
                current["Size"] = sz

            if m_unit:
                try:
                    current["Cost"] = float(m_unit.group(1).replace(",", ""))
                except Exception:
                    current["Cost"] = np.nan

            if m_cs:
                current["Pack"] = first_int_from_text(m_cs.group(1))

            if m_date and "invoice_date" not in current:
                current["invoice_date"] = m_date.group(1).strip()

            # If description still empty, try a looser pass
            if not current.get("Description"):
                mdesc2 = re.search(r"ITEM#.*?\s([A-Za-z0-9].+)", ln)
                if mdesc2:
                    current["Description"] = mdesc2.group(1).strip()

        # flush last block
        if current.get("UPC"):
            items.append(current)

        # 3) Build normalized DataFrame
        out = pd.DataFrame(items)

        # +Cost := top Unit Net Amount (same as Cost above for SG)
        if "Cost" in out.columns:
            out["+Cost"] = out["Cost"]
        else:
            out["+Cost"] = pd.NA

        # parse/normalize fields
        out["invoice_date"] = pd.to_datetime(out.get("invoice_date"), errors="coerce").dt.date
        # Pack defaults to 1 if missing/invalid (avoids divide-by-zero downstream)
        out["Pack"] = pd.to_numeric(out.get("Pack"), errors="coerce")
        out.loc[out["Pack"].isna() | (out["Pack"] <= 0), "Pack"] = 1
        out["Case Qty"] = pd.Series([pd.NA] * len(out), dtype="Int64")  # unknown for SG here
        out["Brand"] = out.get("Brand", "")
        out["Description"] = out.get("Description", "")
        out["Size"] = out.get("Size", "")

        cols = ["invoice_date","UPC","Brand","Description","Pack","Size","Cost","+Cost","Case Qty"]
        for c in cols:
            if c not in out.columns:
                out[c] = "" if c in ["Brand","Description","Size"] else pd.NA

        out = out[cols]
        return sanitize_columns(out)
