# app.py
import streamlit as st
import pandas as pd
from io import BytesIO
from parsers import SouthernGlazersParser, NevadaBeverageParser

st.set_page_config(page_title="Unified Invoice Processor", layout="wide")

# ---------- helpers ----------
def _to_int_safe(x):
    try:
        return int(round(float(str(x).replace(",", "").strip())))
    except Exception:
        return 0

def _to_float_safe(x):
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return 0.0

def _update_master_from_invoice(master_xlsx, invoice_df):
    master_df = pd.read_excel(master_xlsx, dtype=str).fillna("")
    for col in ["Cases","Total","Cost $","Cost ¢","Pack","Invoice UPC","Full Barcode","Name"]:
        if col not in master_df.columns:
            master_df[col] = ""

    # numeric normalization
    master_df["Pack"]   = master_df["Pack"].apply(_to_int_safe)
    master_df["Cases"]  = master_df["Cases"].apply(_to_int_safe)
    master_df["Total"]  = master_df["Total"].apply(_to_float_safe)
    master_df["Cost $"] = master_df["Cost $"].apply(_to_float_safe)
    master_df["Cost ¢"] = master_df["Cost ¢"].apply(_to_int_safe)

    # dedupe invoice by UPC for updating master/pricebook; sum Cases, keep last Cost/Name
    if invoice_df.empty:
        invoice_unique = invoice_df.copy()
    else:
        invoice_unique = (invoice_df
            .assign(Cases=pd.to_numeric(invoice_df["Cases"], errors="coerce").fillna(0).astype(int),
                    Cost=pd.to_numeric(invoice_df["Cost"], errors="coerce"))
            .groupby("UPC", as_index=False)
            .agg({"Item Name": "last", "Cost": "last", "Cases": "sum"}))

    inv_map = invoice_unique.set_index("UPC")[["Item Name","Cost","Cases"]].to_dict(orient="index")
    inv_upcs = set(invoice_unique["UPC"])

    changed_cost_rows, not_in_master_rows, bad_pack_rows = [], [], []
    updated = master_df.copy()

    # normalize master invoice upc function
    def norm_upc(u):
        s = str(u).replace("-", "").replace(" ", "")
        if len(s) == 13 and s.startswith("0"):
            s = s[1:]
        if len(s) > 12:
            s = s[-12:]
        if len(s) < 12:
            s = s.zfill(12)
        return s

    # apply updates
    for idx, row in updated.iterrows():
        inv_upc = norm_upc(row.get("Invoice UPC", ""))
        if inv_upc in inv_map:
            inv_rec = inv_map[inv_upc]
            cases = int(inv_rec["Cases"])
            old_cost = float(updated.at[idx, "Cost $"])
            new_cost = float(inv_rec["Cost"])
            updated.at[idx, "Cases"]  = cases
            # ***** critical: Total = Pack × Cases *****
            pack_val = int(row.get("Pack", 0))
            updated.at[idx, "Total"]  = float(pack_val * cases)
            updated.at[idx, "Cost $"] = new_cost
            updated.at[idx, "Cost ¢"] = int(round(new_cost * 100))

            if abs(old_cost - new_cost) > 1e-6:
                changed_cost_rows.append({
                    "Invoice UPC": inv_upc,
                    "Name": row.get("Name", ""),
                    "Old Cost $": old_cost,
                    "New Cost $": new_cost
                })

            if pack_val <= 0:
                bad_pack_rows.append({
                    "Invoice UPC": inv_upc,
                    "Name": row.get("Name", ""),
                    "Pack": pack_val
                })

    # missing in master
    master_inv = set(norm_upc(x) for x in updated["Invoice UPC"].fillna(""))
    for u in sorted(inv_upcs - master_inv):
        rec = inv_map.get(u, {})
        not_in_master_rows.append({
            "Invoice UPC": u,
            "Item Name": rec.get("Item Name",""),
            "Cost": rec.get("Cost",""),
            "Cases": rec.get("Cases",""),
        })

    return updated, pd.DataFrame(changed_cost_rows), pd.DataFrame(not_in_master_rows), pd.DataFrame(bad_pack_rows), invoice_unique

def _build_pricebook_update(pricebook_csv, updated_master_df):
    # keep only items on the invoice (Total > 0), update addstock & cost_cents
    pb = pd.read_csv(pricebook_csv, dtype=str).fillna("")
    for c in ["Upc","addstock","cost_cents"]:
        if c not in pb.columns:
            pb[c] = ""

    m = updated_master_df.copy()
    m["Full Barcode"] = m["Full Barcode"].astype(str)
    m["TotalNum"] = m["Total"].apply(_to_float_safe)
    m["CostCents"] = m["Cost ¢"].apply(_to_int_safe)

    m = m[m["TotalNum"] > 0]
    master_map = m.set_index("Full Barcode")[["TotalNum","CostCents"]].to_dict(orient="index")

    keep_mask = pb["Upc"].isin(m["Full Barcode"])
    pos_update = pb[keep_mask].copy()
    pos_update["addstock"]   = pos_update["Upc"].map(lambda u: int(master_map[str(u)]["TotalNum"]) if str(u) in master_map else 0)
    pos_update["cost_cents"] = pos_update["Upc"].map(lambda u: int(master_map[str(u)]["CostCents"]) if str(u) in master_map else 0)

    missing = None
    if len(pos_update) != len(m):
        miss = [{"Full Barcode (Master)": fb} for fb in set(m["Full Barcode"]) - set(pos_update["Upc"])]
        missing = pd.DataFrame(miss)

    return pos_update, missing

# ---------- UI ----------
st.title("Unified — Multi-Vendor Invoice Processor")

tabs = st.tabs(["Southern Glazer's", "Nevada Beverage"])

# ----- Southern Glazer's -----
with tabs[0]:
    st.subheader("Southern Glazer's")
    inv_files = st.file_uploader("Upload SG invoice PDF(s) or CSV/XLSX", type=["pdf","csv","xlsx","xls"], accept_multiple_files=True, key="sg_inv")
    master_xlsx = st.file_uploader("Upload Master workbook (.xlsx)", type=["xlsx"], key="sg_master")
    pricebook_csv = st.file_uploader("Upload pricebook CSV (optional for POS update)", type=["csv"], key="sg_pb")

    if st.button("Process SG"):
        if not inv_files or not master_xlsx:
            st.error("Please upload at least one SG invoice and the Master workbook.")
        else:
            # Parse invoices (in order)
            sg_parser = SouthernGlazersParser()
            parts = []
            for f in inv_files:
                f.seek(0)
                df = sg_parser.parse(f)
                if not df.empty:
                    parts.append(df)
            invoice_items_df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["UPC","Item Name","Cost","Cases"])

            # Download: invoice CSV in invoice order, keep UPC leading zeros
            inv_bytes = invoice_items_df.to_csv(index=False).encode("utf-8")
            st.download_button("Download Invoice Items (CSV)", inv_bytes, "sg_invoice_items.csv")

            # Update master
            updated_master, cost_changes, not_in_master, bad_pack, invoice_unique = _update_master_from_invoice(master_xlsx, invoice_items_df)

            # Download updated master xlsx
            bio_master = BytesIO()
            with pd.ExcelWriter(bio_master, engine="openpyxl") as w:
                updated_master.to_excel(w, index=False, sheet_name="Master")
            st.download_button("Download Updated Master (XLSX)", bio_master.getvalue(), "Master_UPDATED.xlsx")

            # Audits
            if not cost_changes.empty:
                st.caption("Cost changes")
                st.dataframe(cost_changes, use_container_width=True)
                st.download_button("Download Cost Changes (CSV)", cost_changes.to_csv(index=False).encode("utf-8"), "sg_cost_changes.csv")

            if not not_in_master.empty:
                st.caption("Invoice UPCs not in Master")
                st.dataframe(not_in_master, use_container_width=True)
                st.download_button("Download Not-in-Master (CSV)", not_in_master.to_csv(index=False).encode("utf-8"), "sg_not_in_master.csv")

            if not bad_pack.empty:
                st.caption("Master rows with Pack ≤ 0")
                st.dataframe(bad_pack, use_container_width=True)
                st.download_button("Download Bad Pack Rows (CSV)", bad_pack.to_csv(index=False).encode("utf-8"), "sg_bad_pack.csv")

            # Optional: POS update
            if pricebook_csv is not None:
                pos_update, pb_missing = _build_pricebook_update(pricebook_csv, updated_master)
                st.caption("POS update preview")
                st.dataframe(pos_update.head(100), use_container_width=True)
                bio_pos = BytesIO()
                with pd.ExcelWriter(bio_pos, engine="openpyxl") as w:
                    pos_update.to_excel(w, index=False, sheet_name="POS_Update")
                st.download_button("Download POS Update (XLSX)", bio_pos.getvalue(), "POS_Update.xlsx")

                if pb_missing is not None and not pb_missing.empty:
                    st.warning("Some invoice items were not found in the pricebook.")
                    st.dataframe(pb_missing, use_container_width=True)
                    st.download_button("Download Pricebook Missing (CSV)", pb_missing.to_csv(index=False).encode("utf-8"), "pricebook_missing.csv")

# ----- Nevada Beverage -----
with tabs[1]:
    st.subheader("Nevada Beverage")
    inv_files_nv = st.file_uploader("Upload NV invoice PDF(s) or CSV/XLSX", type=["pdf","csv","xlsx","xls"], accept_multiple_files=True, key="nv_inv")
    master_xlsx_nv = st.file_uploader("Upload Master workbook (.xlsx)", type=["xlsx"], key="nv_master")
    pricebook_csv_nv = st.file_uploader("Upload pricebook CSV (optional for POS update)", type=["csv"], key="nv_pb")

    if st.button("Process NV"):
        if not inv_files_nv or not master_xlsx_nv:
            st.error("Please upload at least one NV invoice and the Master workbook.")
        else:
            nv_parser = NevadaBeverageParser()
            parts_nv = []
            for f in inv_files_nv:
                f.seek(0)
                df = nv_parser.parse(f)
                if not df.empty:
                    parts_nv.append(df)
            invoice_items_nv = pd.concat(parts_nv, ignore_index=True) if parts_nv else pd.DataFrame(columns=["UPC","Item Name","Cost","Cases"])

            inv_bytes_nv = invoice_items_nv.to_csv(index=False).encode("utf-8")
            st.download_button("Download Invoice Items (CSV)", inv_bytes_nv, "nv_invoice_items.csv")

            updated_master_nv, cost_changes_nv, not_in_master_nv, bad_pack_nv, invoice_unique_nv = _update_master_from_invoice(master_xlsx_nv, invoice_items_nv)

            bio_master_nv = BytesIO()
            with pd.ExcelWriter(bio_master_nv, engine="openpyxl") as w:
                updated_master_nv.to_excel(w, index=False, sheet_name="Master")
            st.download_button("Download Updated Master (XLSX)", bio_master_nv.getvalue(), "Master_UPDATED_NV.xlsx")

            if not cost_changes_nv.empty:
                st.caption("Cost changes")
                st.dataframe(cost_changes_nv, use_container_width=True)
                st.download_button("Download Cost Changes (CSV)", cost_changes_nv.to_csv(index=False).encode("utf-8"), "nv_cost_changes.csv")

            if not not_in_master_nv.empty:
                st.caption("Invoice UPCs not in Master")
                st.dataframe(not_in_master_nv, use_container_width=True)
                st.download_button("Download Not-in-Master (CSV)", not_in_master_nv.to_csv(index=False).encode("utf-8"), "nv_not_in_master.csv")

            if not bad_pack_nv.empty:
                st.caption("Master rows with Pack ≤ 0")
                st.dataframe(bad_pack_nv, use_container_width=True)
                st.download_button("Download Bad Pack Rows (CSV)", bad_pack_nv.to_csv(index=False).encode("utf-8"), "nv_bad_pack.csv")

            if pricebook_csv_nv is not None:
                pos_update_nv, pb_missing_nv = _build_pricebook_update(pricebook_csv_nv, updated_master_nv)
                st.caption("POS update preview")
                st.dataframe(pos_update_nv.head(100), use_container_width=True)
                bio_pos_nv = BytesIO()
                with pd.ExcelWriter(bio_pos_nv, engine="openpyxl") as w:
                    pos_update_nv.to_excel(w, index=False, sheet_name="POS_Update")
                st.download_button("Download POS Update (XLSX)", bio_pos_nv.getvalue(), "POS_Update_NV.xlsx")

                if pb_missing_nv is not None and not pb_missing_nv.empty:
                    st.warning("Some invoice items were not found in the pricebook.")
                    st.dataframe(pb_missing_nv, use_container_width=True)
                    st.download_button("Download Pricebook Missing (CSV)", pb_missing_nv.to_csv(index=False).encode("utf-8"), "pricebook_missing_nv.csv")
