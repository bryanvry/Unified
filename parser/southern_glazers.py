
import pandas as pd
import numpy as np
import re
from .utils import find_col, first_int_from_text, to_float, normalize_invoice_upc, sanitize_columns, digits_only

class SouthernGlazersParser:
    name = "Southern Glazer's"
    tokens = ["ITEM#","UPC","SIZE:","Unit Net Amount","CS ORD/DLV","Invoice"]

    def parse(self, uploaded_file) -> pd.DataFrame:
        if uploaded_file.name.lower().endswith(".csv"):
            df_raw = pd.read_csv(uploaded_file, header=None, dtype=str, keep_default_na=False)
        else:
            df_raw = pd.read_excel(uploaded_file, header=None, dtype=str)

        header_row = None
        for i in range(min(80, len(df_raw))):
            row = " ".join([str(x) for x in df_raw.iloc[i].tolist()])
            if "ITEM#" in row.upper() and "UPC" in row.upper():
                header_row = i
                break
        if header_row is None:
            header_row = 0

        df = df_raw.iloc[header_row+1:].fillna("")
        lines = df.apply(lambda r: " ".join([str(x) for x in r.tolist() if str(x).strip()!=""]), axis=1).tolist()

        items = []
        current = {}
        for ln in lines:
            upc_match = re.search(r"\bUPC[:\s]*([0-9\- ]+)", ln, re.I)
            size_match = re.search(r"\bSIZE[:\s]*([A-Za-z0-9 ]+)", ln, re.I)
            unit_net_match = re.search(r"Unit Net Amount[:\s]*\$?([0-9\.,]+)", ln, re.I)
            cs_match = re.search(r"CS ORD/DLV[:\s]*([0-9]+(?:/[0-9]+)?)", ln, re.I)
            date_match = re.search(r"Invoice Date[:\s]*([0-9/\\-]+)", ln, re.I)

            if "ITEM#" in ln.upper():
                if current.get("UPC"):
                    items.append(current)
                current = {"Size":"", "Brand":"", "Description":""}

            if upc_match:
                upc_raw = re.sub(r"[^0-9]", "", upc_match.group(1))
                current["UPC"] = normalize_invoice_upc(upc_raw)
            if size_match:
                sz = size_match.group(1).strip().replace(" z", " oz").replace("Z", "oz")
                current["Size"] = sz
            if unit_net_match:
                try: current["Cost"] = float(unit_net_match.group(1).replace(",",""))
                except: current["Cost"] = np.nan
            if cs_match:
                current["Pack"] = first_int_from_text(cs_match.group(1))
            if date_match and "invoice_date" not in current:
                current["invoice_date"] = date_match.group(1)

            if not current.get("Description"):
                mdesc = re.search(r"ITEM#.*?\s([A-Za-z0-9].+)", ln)
                if mdesc: current["Description"] = mdesc.group(1).strip()

        if current.get("UPC"):
            items.append(current)

        out = pd.DataFrame(items)
        out["+Cost"] = out["Cost"]
        out["invoice_date"] = pd.to_datetime(out.get("invoice_date"), errors="coerce").dt.date
        out["Case Qty"] = pd.Series([pd.NA]*len(out), dtype="Int64")
        out["Brand"] = out.get("Brand","")
        cols = ["invoice_date","UPC","Brand","Description","Pack","Size","Cost","+Cost","Case Qty"]
        for c in cols:
            if c not in out.columns:
                out[c] = "" if c in ["Brand","Description","Size"] else pd.NA
        return sanitize_columns(out[cols])
