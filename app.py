import streamlit as st
import pandas as pd
from io import BytesIO

# ===== Imports for vendor parsers =====
from parsers import SouthernGlazersParser, NevadaBeverageParser

st.set_page_config(page_title="Unified Invoice Processor", layout="wide")

# ===================== Utilities =====================
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

def _norm_upc_12(u: str) -> str:
    s = str(u or "").replace("-", "").replace(" ", "")
    if len(s) == 13 and s.startswith("0"):
        s = s[1:]
    if len(s) > 12:
        s = s[-12:]
    if len(s) < 12:
        s = s.zfill(12)
    return s

def _needs_cols(df: pd.DataFrame, cols):
    return all(c in df.columns for c in cols)

def _ensure_invoice_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Make invoice df robust to header variations; return normalized df with UPC, Item Name, Cost, Cases."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["UPC", "Item Name", "Cost", "Cases"])
    # Try case-insensitive mapping
    colmap = {}
    for want in ["UPC","Item Name","Cost","Cases"]:
        found = None
        for c in df.columns:
            if str(c).strip().lower() == want.lower():
                found = c
                break
        colmap[want] = found
    out = pd.DataFrame()
    for want in ["UPC","Item Name","Cost","Cases"]:
        src = colmap.get(want)
        if src is None:
            out[want] = []  # will become empty DF below
        else:
            out[want] = df[src]
    # if any required not present, return empty shell with headers
    if not _needs_cols(out, ["UPC","Item Name","Cost","Cases"]):
        return pd.DataFrame(columns=["UPC","Item Name","Cost","Cases"])
    # normalize types & UPCs (preserve leading zeros)
    out["UPC"] = out["UPC"].map(_norm_upc_12)
    out["Item Name"] = out["Item Name"].astype(str)
    out["Cost"] = pd.to_numeric(out["Cost"], errors="coerce")
    out["Cases"] = pd.to_numeric(out["Cases"], errors="coerce").fillna(0).astype(int)
    # drop rows missing UPC or Cost
    out = out[(out["UPC"].astype(str) != "") & out["Cost"].notna()].copy()
    return out

def _update_master_from_invoice(master_xlsx, invoice_df: pd.DataFrame):
    """Apply SG/NV invoice updates to Master: Cases, Total=Pack*Cases, Cost$, Cost¢. Returns (updated, cost_changes, not_in_master, bad_pack, invoice_unique)"""
    # guard invoice_df
    invoice_df = _ensure_invoice_cols(invoice_df)
    if invoice_df.empty:
        return (None, None, None, None, None)

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

    # dedupe invoice by UPC for updating master/pricebook
    invoice_unique = (invoice_df
        .groupby("UPC", as_index=False)
        .agg({"Item Name":"last","Cost":"last","Cases":"sum"}))

    inv_map = invoice_unique.set_index("UPC")[["Item Name","Cost","Cases"]].to_dict(orient="index")
    inv_upcs = set(invoice_unique["UPC"])

    changed_cost_rows, not_in_master_rows, bad_pack_rows = [], [], []
    updated = master_df.copy()

    # normalize master invoice UPC and apply updates
    for idx, row in updated.iterrows():
        inv_upc = _norm_upc_12(row.get("Invoice UPC", ""))
        if inv_upc in inv_map:
            inv_rec = inv_map[inv_upc]
            cases = int(inv_rec["Cases"])
            old_cost = float(updated.at[idx, "Cost $"])
            new_cost = float(inv_rec["Cost"])

            updated.at[idx, "Cases"]  = cases
            pack_val = int(row.get("Pack", 0))
            updated.at[idx, "Total"]  = float(pack_val * cases)   # *** Total = Pack × Cases ***
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
    master_inv = set(_norm_upc_12(x) for x in updated["Invoice UPC"].fillna(""))
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

def _download_xlsx(df: pd.DataFrame, filename: str, sheet="Sheet1"):
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name=sheet)
    st.download_button(f"Download {filename}", bio.getvalue(), filename)

# ===================== UI =====================
st.title("Unified — Multi-Vendor Invoice Processor")

tabs = st.tabs(["Unified (SVMERCH)", "Southern Glazer's", "Nevada Beverage"])

# ---------- Unified (SVMERCH) ----------
with tabs[0]:
    st.subheader("Unified (SVMERCH)")
    st.caption("Upload your Unified invoice(s) and POS (pricebook). Keeps your original workflow.")
    pos_csv = st.file_uploader("Upload POS / pricebook CSV", type=["csv"], key="un_pos")
    inv_files = st.file_uploader("Upload Unified invoice file(s) (.xlsx/.xls)", type=["xlsx","xls"], accept_multiple_files=True, key="un_inv")

    if st.button("Process Unified"):
        if not pos_csv or not inv_files:
            st.error("Please upload at least one Unified invoice and the POS CSV.")
        else:
            # —— Minimal integrated Unified processor ——
            #  • Reads invoices, normalizes “Item UPC” to 12-digit reference for joins
            #  • Ignores rows where case qty == 0
            #  • De-dupes by UPC using latest invoice date that actually arrived
            #  • Goal Sheet 1 and Goal Sheet 2 per your previous spec

            # read POS
            pos_df = pd.read_csv(pos_csv, dtype=str).fillna("")
            # enforce $Now derived from cents column (to dollars)
            if "cents" in pos_df.columns and "$Now" not in pos_df.columns:
                pos_df["$Now"] = (pd.to_numeric(pos_df["cents"], errors="coerce").fillna(0) / 100).round(2)

            # collect invoice items
            inv_parts = []
            for f in inv_files:
                x = pd.read_excel(f, dtype=str).fillna("")
                # expected columns include "Item UPC", "Brand", "Description", "Pack", "Size", "Net Case Cost", "Case Qty", "Invoice Date", "+Cost" possibly, etc.
                # normalize column names (lowercase)
                x.columns = [c.strip() for c in x.columns]
                # harmonize typical column labels
                colmap = {
                    "Item UPC": "Item UPC",
                    "Brand": "Brand",
                    "Description": "Description",
                    "Pack": "Pack",
                    "Size": "Size",
                    "Net Case Cost": "Net Case Cost",
                    "Case Qty": "Case Qty",
                    "+Cost": "Net Case Cost",
                    "Invoice Date": "Invoice Date",
                    "Date": "Invoice Date",
                }
                for k,v in list(colmap.items()):
                    if k not in x.columns:
                        # try case-insensitive fallback
                        alts = [c for c in x.columns if c.lower()==k.lower()]
                        if alts:
                            x[v] = x[alts[0]]
                        else:
                            if v not in x.columns:
                                x[v] = ""
                # numeric
                x["Case Qty"] = pd.to_numeric(x["Case Qty"], errors="coerce").fillna(0).astype(float)
                x["Net Case Cost"] = pd.to_numeric(x["Net Case Cost"], errors="coerce")
                # ignore case qty == 0 (did not arrive)
                x = x[x["Case Qty"] > 0]
                # normalize UPC from Unified quirks (extra leading zeros, last digit chopped)
                def norm_un(u):
                    s = str(u or "").replace("-", "").replace(" ", "")
                    s = "".join(ch for ch in s if ch.isdigit())
                    # Unified sometimes has extra leading zeros and chops last digit:
                    # take rightmost 12 and zfill.
                    if len(s) > 12:
                        s = s[-12:]
                    if len(s) < 12:
                        s = s.zfill(12)
                    return s
                x["Item UPC"] = x["Item UPC"].map(norm_un)
                inv_parts.append(x)

            if not inv_parts:
                st.error("No usable Unified rows found in the uploaded files.")
            else:
                inv_all = pd.concat(inv_parts, ignore_index=True)

                # If multiple rows per UPC, keep the latest invoice date that arrived
                inv_all["Invoice Date"] = pd.to_datetime(inv_all["Invoice Date"], errors="coerce")
                inv_all = inv_all.sort_values(["Item UPC", "Invoice Date"], ascending=[True, True])
                inv_latest = inv_all.groupby("Item UPC", as_index=False).tail(1)

                # ==== Goal Sheet 1 ====
                # Columns: POS.UPC, Brand, Description, Pack, Size, Cost (Net Case Cost), +Cost (Net Case Cost),
                # Unit = +Cost / Pack, D40% = Unit / 0.6, 40% = (Cost / Pack) / 0.6, $Now from POS (cents -> $)
                gs1 = inv_latest[["Item UPC","Brand","Description","Pack","Size","Net Case Cost"]].copy()
                gs1.rename(columns={"Item UPC":"UPC","Net Case Cost":"+Cost"}, inplace=True)
                gs1["Pack"] = pd.to_numeric(gs1["Pack"], errors="coerce").fillna(0)
                gs1["+Cost"] = pd.to_numeric(gs1["+Cost"], errors="coerce").fillna(0.0)
                gs1["Unit"] = (gs1["+Cost"] / gs1["Pack"]).replace([float("inf")], 0).fillna(0.0)
                gs1["D40%"] = (gs1["Unit"] / 0.6).round(2)
                gs1["Cost"] = gs1["+Cost"]  # alias for clarity
                gs1["40%"] = ((gs1["Cost"] / gs1["Pack"]) / 0.6).replace([float("inf")], 0).fillna(0.0).round(2)

                # bring $Now from POS (cents->dollars)
                pos_now = pos_df[["UPC","cents"]].copy() if "UPC" in pos_df.columns else pd.DataFrame(columns=["UPC","cents"])
                if not pos_now.empty:
                    pos_now["$Now"] = (pd.to_numeric(pos_now["cents"], errors="coerce").fillna(0) / 100).round(2)
                    pos_now = pos_now[["UPC","$Now"]]
                    gs1 = gs1.merge(pos_now, on="UPC", how="left")
                else:
                    gs1["$Now"] = 0.0

                # ==== Goal Sheet 2 (POS upload) ====
                # same as POS sheet but only items found in Unified; cost_qty = Pack; cost_cents = +Cost * 100
                if "UPC" in pos_df.columns:
                    keep_upcs = set(gs1["UPC"])
                    pos2 = pos_df[pos_df["UPC"].isin(keep_upcs)].copy()
                    # ensure fields
                    if "cost_qty" not in pos2.columns:
                        pos2["cost_qty"] = ""
                    if "cost_cents" not in pos2.columns:
                        pos2["cost_cents"] = ""
                    # join pack/+Cost
                    join = gs1[["UPC","Pack","+Cost"]].copy()
                    join["cost_qty"] = join["Pack"].astype(int)
                    join["cost_cents"] = (join["+Cost"].round(2) * 100).astype(int)
                    join = join[["UPC","cost_qty","cost_cents"]]
                    pos2 = pos2.drop(columns=[c for c in ["cost_qty","cost_cents"] if c in pos2.columns]).merge(join, on="UPC", how="left")
                else:
                    pos2 = pd.DataFrame()

                # outputs
                st.success("Unified processing complete.")
                st.dataframe(gs1.head(100), use_container_width=True)
                _download_xlsx(gs1, "Goal_Sheet_1.xlsx", sheet="Goal Sheet 1")
                if not pos2.empty:
                    st.dataframe(pos2.head(100), use_container_width=True)
                    _download_xlsx(pos2, "Goal_Sheet_2.xlsx", sheet="POS Update")
                else:
                    st.info("POS upload sheet not generated (pricebook missing a 'UPC' column or no matches).")

# ---------- Southern Glazer's ----------
with tabs[1]:
    st.subheader("Southern Glazer's")
    inv_files = st.file_uploader("Upload SG invoice PDF(s) or CSV/XLSX", type=["pdf","csv","xlsx","xls"], accept_multiple_files=True, key="sg_inv")
    master_xlsx = st.file_uploader("Upload Master workbook (.xlsx)", type=["xlsx"], key="sg_master")
    pricebook_csv = st.file_uploader("Upload pricebook CSV (optional for POS update)", type=["csv"], key="sg_pb")

    if st.button("Process SG"):
        if not inv_files or not master_xlsx:
            st.error("Please upload at least one SG invoice and the Master workbook.")
        else:
            sg_parser = SouthernGlazersParser()
            parts = []
            for f in inv_files:
                f.seek(0)
                df = sg_parser.parse(f)
                if not df.empty:
                    parts.append(df)
            invoice_items_df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["UPC","Item Name","Cost","Cases"])

            # guard: ensure required headers before any downstream work
            invoice_items_df = _ensure_invoice_cols(invoice_items_df)
            if invoice_items_df.empty:
                st.error("Could not parse any SG items (no UPC/Item Name/Cost/Cases). Please check the PDF quality or try another file.")
            else:
                # invoice CSV (invoice order preserved by parser)
                inv_bytes = invoice_items_df.to_csv(index=False).encode("utf-8")
                st.download_button("Download Invoice Items (CSV)", inv_bytes, "sg_invoice_items.csv")

                updated_master, cost_changes, not_in_master, bad_pack, invoice_unique = _update_master_from_invoice(master_xlsx, invoice_items_df)
                if updated_master is None:
                    st.error("No valid items to merge into Master.")
                else:
                    bio_master = BytesIO()
                    with pd.ExcelWriter(bio_master, engine="openpyxl") as w:
                        updated_master.to_excel(w, index=False, sheet_name="Master")
                    st.download_button("Download Updated Master (XLSX)", bio_master.getvalue(), "Master_UPDATED.xlsx")

                    if cost_changes is not None and not cost_changes.empty:
                        st.caption("Cost changes")
                        st.dataframe(cost_changes, use_container_width=True)
                        st.download_button("Download Cost Changes (CSV)", cost_changes.to_csv(index=False).encode("utf-8"), "sg_cost_changes.csv")

                    if not_in_master is not None and not not_in_master.empty:
                        st.caption("Invoice UPCs not in Master")
                        st.dataframe(not_in_master, use_container_width=True)
                        st.download_button("Download Not-in-Master (CSV)", not_in_master.to_csv(index=False).encode("utf-8"), "sg_not_in_master.csv")

                    if bad_pack is not None and not bad_pack.empty:
                        st.caption("Master rows with Pack ≤ 0")
                        st.dataframe(bad_pack, use_container_width=True)
                        st.download_button("Download Bad Pack Rows (CSV)", bad_pack.to_csv(index=False).encode("utf-8"), "sg_bad_pack.csv")

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

# ---------- Nevada Beverage ----------
with tabs[2]:
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
            invoice_items_nv = _ensure_invoice_cols(invoice_items_nv)
            if invoice_items_nv.empty:
                st.error("Could not parse any NV items (no UPC/Item Name/Cost/Cases).")
            else:
                inv_bytes_nv = invoice_items_nv.to_csv(index=False).encode("utf-8")
                st.download_button("Download Invoice Items (CSV)", inv_bytes_nv, "nv_invoice_items.csv")

                updated_master_nv, cost_changes_nv, not_in_master_nv, bad_pack_nv, invoice_unique_nv = _update_master_from_invoice(master_xlsx_nv, invoice_items_nv)
                if updated_master_nv is None:
                    st.error("No valid items to merge into Master.")
                else:
                    bio_master_nv = BytesIO()
                    with pd.ExcelWriter(bio_master_nv, engine="openpyxl") as w:
                        updated_master_nv.to_excel(w, index=False, sheet_name="Master")
                    st.download_button("Download Updated Master (XLSX)", bio_master_nv.getvalue(), "Master_UPDATED_NV.xlsx")

                    if cost_changes_nv is not None and not cost_changes_nv.empty:
                        st.caption("Cost changes")
                        st.dataframe(cost_changes_nv, use_container_width=True)
                        st.download_button("Download Cost Changes (CSV)", cost_changes_nv.to_csv(index=False).encode("utf-8"), "nv_cost_changes.csv")

                    if not_in_master_nv is not None and not not_in_master_nv.empty:
                        st.caption("Invoice UPCs not in Master")
                        st.dataframe(not_in_master_nv, use_container_width=True)
                        st.download_button("Download Not-in-Master (CSV)", not_in_master_nv.to_csv(index=False).encode("utf-8"), "nv_not_in_master.csv")

                    if bad_pack_nv is not None and not bad_pack_nv.empty:
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
