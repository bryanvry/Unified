
import pandas as pd
import numpy as np
import re
from .utils import normalize_invoice_upc, sanitize_columns

class NevadaBeverageParser:
    name = "Nevada Beverage"
    tokens = ["ITEM#","U.P.C.","QTY","DESCRIPTION"]

    def parse(self, uploaded_file) -> pd.DataFrame:
        if uploaded_file.name.lower().endswith(".csv"):
            df_raw = pd.read_csv(uploaded_file, header=None, dtype=str, keep_default_na=False)
        else:
            df_raw = pd.read_excel(uploaded_file, header=None, dtype=str)

        header_row = None
        for i in range(min(100, len(df_raw))):
            row = " ".join([str(x) for x in df_raw.iloc[i].tolist()])
            if "ITEM#" in row.upper() and ("U.P.C." in row.upper() or "UPC" in row.upper()):
                header_row = i
                break
        if header_row is None:
            header_row = 0

        df = df_raw.iloc[header_row+1:].fillna("")
        lines = df.apply(lambda r: " ".join([str(x) for x in r.tolist() if str(x).strip()!=""]), axis=1).tolist()

        items = []
        for ln in lines:
            if re.search(r"TOTAL|PAYMENT|SUMMARY", ln, re.I):
                break
            m_upc = re.search(r"(?:UPC|U\.P\.C\.)[:\s]*([0-9\- ]+)", ln, re.I)
            m_desc = re.search(r"ITEM#\s*\S+\s+(.+)", ln)
            m_cost = re.search(r"\$([0-9\.,]+)", ln)
            m_date = re.search(r"Invoice Date[:\s]*([0-9/\\-]+)", ln, re.I)
            if m_upc:
                upc = normalize_invoice_upc(m_upc.group(1))
                desc = m_desc.group(1).strip() if m_desc else ""
                cost = float(m_cost.group(1).replace(",","")) if m_cost else np.nan
                items.append({
                    "invoice_date": m_date.group(1) if m_date else None,
                    "UPC": upc, "Brand":"", "Description": desc,
                    "Pack": np.nan, "Size":"",
                    "Cost": cost, "+Cost": cost,
                    "Case Qty": pd.NA
                })

        out = pd.DataFrame(items)
        out["invoice_date"] = pd.to_datetime(out["invoice_date"], errors="coerce").dt.date
        cols = ["invoice_date","UPC","Brand","Description","Pack","Size","Cost","+Cost","Case Qty"]
        for c in cols:
            if c not in out.columns:
                out[c] = "" if c in ["Brand","Description","Size"] else pd.NA
        return sanitize_columns(out[cols])
