
import pandas as pd
import numpy as np
import re
from .utils import find_col, first_int_from_text, to_float, normalize_invoice_upc, sanitize_columns

class UnifiedParser:
    name = "Unified (SVMERCH)"
    tokens = ["Item UPC","Net Case Cost","Case Qty","Invoice Date","Brand","Description","Pack","Size","Cost"]

    def parse(self, uploaded_file) -> pd.DataFrame:
        name = uploaded_file.name.lower()
        if name.endswith(".csv"):
            df_raw = pd.read_csv(uploaded_file, header=None, dtype=str, keep_default_na=False)
        else:
            df_raw = pd.read_excel(uploaded_file, header=None, dtype=str)

        header_tokens = self.tokens
        best_row_idx, best_hits = None, 0
        for i in range(min(200, len(df_raw))):
            vals = [str(x) if pd.notna(x) else "" for x in df_raw.iloc[i].tolist()]
            hits = sum(1 for v in vals for t in header_tokens if t.lower() in v.strip().lower())
            if hits > best_hits:
                best_hits, best_row_idx = hits, i
        header_row = best_row_idx if best_row_idx is not None else 0

        raw_header = df_raw.iloc[header_row].tolist()
        clean_header, seen = [], {}
        for i, h in enumerate(raw_header):
            nm = (str(h) if pd.notna(h) else "").strip() or f"Unnamed_{i}"
            nm = " ".join(nm.split())
            if nm in seen:
                seen[nm] += 1
                nm = f"{nm}_{seen[nm]}"
            else:
                seen[nm] = 0
            clean_header.append(nm)

        inv_df = df_raw.iloc[header_row+1:].copy()
        inv_df.columns = clean_header
        inv_df = inv_df.dropna(how="all")
        cols = list(inv_df.columns)

        col_item_upc = find_col(cols, ["Item UPC","UPC"])
        col_brand    = find_col(cols, ["Brand"])
        col_desc     = find_col(cols, ["Description","Item Description"])
        col_pack     = find_col(cols, ["Pack","Case Pack","Qty per case"])
        col_size     = find_col(cols, ["Size"])
        col_cost     = find_col(cols, ["Cost"])
        col_netcost  = find_col(cols, ["Net Case Cost"])
        col_caseqty  = find_col(cols, ["Case Qty","Case Quantity","Cases","Qty"])
        col_invdate  = find_col(cols, ["Invoice Date","Inv Date","Date"])

        inv_df = inv_df[inv_df[col_item_upc].astype(str).apply(lambda x: len(re.sub(r"\D","", str(x))) >= 8)]
        case_qty_num = pd.to_numeric(inv_df[col_caseqty].apply(first_int_from_text) if col_caseqty else np.nan, errors="coerce")
        inv_df = inv_df[case_qty_num.fillna(0) > 0]

        if col_invdate:
            inv_df["_invoice_date_parsed"] = pd.to_datetime(inv_df[col_invdate], errors="coerce")
        else:
            inv_df["_invoice_date_parsed"] = pd.NaT
        inv_df["_invoice_date"] = inv_df["_invoice_date_parsed"].dt.date

        out = pd.DataFrame()
        out["invoice_date"] = inv_df["_invoice_date"]
        out["UPC"]          = inv_df[col_item_upc].astype(str).apply(normalize_invoice_upc)
        out["Brand"]        = inv_df[col_brand].astype(str) if col_brand else ""
        out["Description"]  = inv_df[col_desc].astype(str) if col_desc else ""
        out["Pack"]         = inv_df[col_pack].apply(first_int_from_text) if col_pack else np.nan
        out["Size"]         = inv_df[col_size].astype(str) if col_size else ""
        out["Cost"]         = inv_df[col_cost].apply(to_float) if col_cost else np.nan
        out["+Cost"]        = inv_df[col_netcost].apply(to_float) if col_netcost else out["Cost"]
        out["Case Qty"]     = case_qty_num.loc[inv_df.index].astype("Int64")
        return sanitize_columns(out)
