# app.py
import streamlit as st
import pandas as pd
from io import BytesIO

# ===== Imports for vendor parsers (these files should already exist as you pasted earlier) =====
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
    """Normalize invoice frame to UPC, Item Name, Cost, Cases (strings preserved, zeros kept)."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["UPC", "Item Name", "Cost", "Cases"])
    want = ["UPC","Item Name","Cost","Cases"]
    out = {}
    for w in want:
        picked = None
        for c in df.columns:
            if str(c).strip().lower() == w.lower():
                picked = c
                break
        out[w] = df[picked] if picked is not None else None
    if any(v is None for v in out.values()):
        return pd.DataFrame(columns=want)
    res = pd.DataFrame({k: out[k] for k in want}).copy()
    res["UPC"] = res["UPC"].map(_norm_upc_12).astype(str)
    res["Item Name"] = res["Item Name"].astype(str)
    res["Cost"] = pd.to_numeric(res["Cost"], errors="coerce")
    res["Cases"] = pd.to_numeric(res["Cases"], errors="coerce").fillna(0).astype(int)
    res = res[(res["UPC"] != "") & res["Cost"].notna()].copy()
    return res

def _resolve_col(df: pd.DataFrame, candidates, default_name):
    """Find first column in candidates; if none, create default_name and return it."""
    for cand in candidates:
        if cand in df.columns:
            return cand
    if default_name not in df.columns:
        df[default_name] = ""
    return default_name

def _download_xlsx(df: pd.DataFrame, filename: str, sheet="Sheet1"):
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name=sheet)
    st.download_button(f"Download {filename}", bio.getvalue(), filename)

def _download_csv_with_upc_text(df: pd.DataFrame, filename: str, upc_col="UPC"):
    """Force Excel to keep leading zeros by exporting UPC as =\"001234...\" """
    df2 = df.copy()
    if upc_col in df2.columns:
        df2[upc_col] = df2[upc_col].astype(str).map(lambda x: f'="{x}"')
    st.download_button(f"Download {filename}", df2.to_csv(index=False).encode("utf-8"), filename)

# ===================== Master update & POS build =====================
def _update_master_from_invoice(master_xlsx, invoice_df: pd.DataFrame):
    """
    Apply SG/NV invoice updates to Master:
      - Cases (from invoice aggregation)
      - Total = Pack × Cases
      - Cost $ and Cost ¢ from invoice Cost
    Returns:
      updated_df, cost_changes_df, not_in_master_df, pack_missing_on_added_df, invoice_unique_df
    """
    invoice_df = _ensure_invoice_cols(invoice_df)
    if invoice_df.empty:
        return (None, None, None, None, None)

    master = pd.read_excel(master_xlsx, dtype=str).fillna("")

    # Resolve column names actually present in the Master
    name_col         = _resolve_col(master, ["Name","NAME","name"], "Name")
    pack_col         = _resolve_col(master, ["Pack","PACK","pack"], "Pack")
    cases_col        = _resolve_col(master, ["Cases","CASES","cases"], "Cases")
    total_col        = _resolve_col(master, ["Total","TOTAL","total"], "Total")
    cost_dollar_col  = _resolve_col(master, ["Cost $","Cost$","COST $","cost $"], "Cost $")
    cost_cent_col    = _resolve_col(master, ["Cost ¢","Cost cents","Cost c","COST ¢"], "Cost ¢")
    inv_upc_col      = _resolve_col(master, ["Invoice UPC","InvoiceUPC","INV UPC","Invoice upc"], "Invoice UPC")
    full_barcode_col = _resolve_col(master, ["Full Barcode","FullBarcode","FULL BARCODE"], "Full Barcode")

    # Numeric normalization on the resolved names
    master[pack_col]        = master[pack_col].apply(_to_int_safe)
    master[cases_col]       = master[cases_col].apply(_to_int_safe)
    master[total_col]       = master[total_col].apply(_to_float_safe)
    master[cost_dollar_col] = master[cost_dollar_col].apply(_to_float_safe)
    master[cost_cent_col]   = master[cost_cent_col].apply(_to_int_safe)

    # Aggregate invoice by UPC: sum Cases; keep last Item Name and Cost
    invoice_unique = (invoice_df
        .groupby("UPC", as_index=False)
        .agg({"Item Name":"last","Cost":"last","Cases":"sum"}))

    inv_map = invoice_unique.set_index("UPC")[["Item Name","Cost","Cases"]].to_dict(orient="index")
    inv_upcs = set(invoice_unique["UPC"])

    changed_cost_rows = []
    not_in_master_rows = []
    pack_missing_on_added_rows = []  # ONLY rows where we set Cases>0 AND pack==0

    updated = master.copy()

    for idx, row in updated.iterrows():
        inv_upc = _norm_upc_12(row.get(inv_upc_col, ""))
        if inv_upc in inv_map:
            inv_rec   = inv_map[inv_upc]
            new_cases = int(inv_rec["Cases"])
            old_cost  = float(updated.at[idx, cost_dollar_col])
            new_cost  = float(inv_rec["Cost"])

            updated.at[idx, cases_col]       = new_cases
            pack_val = int(row.get(pack_col, 0))
            updated.at[idx, total_col]       = float(pack_val * new_cases)  # Total = Pack × Cases
            updated.at[idx, cost_dollar_col] = new_cost
            updated.at[idx, cost_cent_col]   = int(round(new_cost * 100))

            if abs(old_cost - new_cost) > 1e-6:
                changed_cost_rows.append({
                    inv_upc_col: inv_upc,
                    name_col: row.get(name_col, ""),
                    "Old Cost $": old_cost,
                    "New Cost $": new_cost
                })

            # List: items added (Cases>0) but Pack == 0
            if new_cases > 0 and pack_val == 0:
                pack_missing_on_added_rows.append({
                    inv_upc_col: inv_upc,
                    name_col: row.get(name_col, ""),
                    "Cases": new_cases,
                    "Pack": pack_val
                })

    # Which invoice UPCs not in master?
    master_inv = set(_norm_upc_12(x) for x in updated[inv_upc_col].fillna(""))
    for u in sorted(inv_upcs - master_inv):
        rec = inv_map.get(u, {})
        not_in_master_rows.append({
            inv_upc_col: u,
            "Item Name": rec.get("Item Name",""),
            "Cost": rec.get("Cost",""),
            "Cases": rec.get("Cases",""),
        })

    return (
        updated,
        pd.DataFrame(changed_cost_rows),
        pd.DataFrame(not_in_master_rows),
        pd.DataFrame(pack_missing_on_added_rows),
        invoice_unique
    )

def _build_pricebook_update(pricebook_csv, updated_master_df):
    pb = pd.read_csv(pricebook_csv, dtype=str).fillna("")

    # Make sure columns exist
    for c in ["Upc","addstock","cost_cents"]:
        if c not in pb.columns:
            pb[c] = ""

    # Resolve the master column names again (same logic)
    master = updated_master_df.copy()
    full_barcode_col = _resolve_col(master, ["Full Barcode","FullBarcode","FULL BARCODE"], "Full Barcode")
    total_col        = _resolve_col(master, ["Total","TOTAL","total"], "Total")
    cost_cent_col    = _resolve_col(master, ["Cost ¢","Cost cents","Cost c","COST ¢"], "Cost ¢")

    master[full_barcode_col] = master[full_barcode_col].astype(str)
    master["__TotalNum"]  = master[total_col].apply(_to_float_safe)
    master["__CostCents"] = master[cost_cent_col].apply(_to_int_safe)

    # Keep ONLY invoice items (Total > 0)
    m = master[master["__TotalNum"] > 0].copy()
    if m.empty:
        return pb.iloc[0:0].copy(), pd.DataFrame([{"note":"No items with Total > 0 in Master after update."}])

    master_map = m.set_index(full_barcode_col)[["__TotalNum","__CostCents"]].to_dict(orient="index")

    keep_mask = pb["Upc"].isin(m[full_barcode_col])
    pos_update = pb[keep_mask].copy()

    pos_update["addstock"]   = pos_update["Upc"].map(lambda u: int(master_map[str(u)]["__TotalNum"]) if str(u) in master_map else 0)
    pos_update["cost_cents"] = pos_update["Upc"].map(lambda u: int(master_map[str(u)]["__CostCents"]) if str(u) in master_map else 0)

    missing = None
    if len(pos_update) != len(m):
        miss = [{"Full Barcode (Master)": fb} for fb in set(m[full_barcode_col]) - set(pos_update["Upc"])]
        missing = pd.DataFrame(miss)

    return pos_update, missing

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
            # Read POS
            pos_df = pd.read_csv(pos_csv, dtype=str).fillna("")
            if "cents" in pos_df.columns and "$Now" not in pos_df.columns:
                pos_df["$Now"] = (pd.to_numeric(pos_df["cents"], errors="coerce").fillna(0) / 100).round(2)

            inv_parts = []
            for f in inv_files:
                x = pd.read_excel(f, dtype=str).fillna("")
                x.columns = [c.strip() for c in x.columns]
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
                        alts = [c for c in x.columns if c.lower()==k.lower()]
                        if alts:
                            x[v] = x[alts[0]]
                        else:
                            if v not in x.columns:
                                x[v] = ""
                x["Case Qty"] = pd.to_numeric(x["Case Qty"], errors="coerce").fillna(0).astype(float)
                x["Net Case Cost"] = pd.to_numeric(x["Net Case Cost"], errors="coerce")
                x = x[x["Case Qty"] > 0]

                def norm_un(u):
                    s = str(u or "").replace("-", "").replace(" ", "")
                    s = "".join(ch for ch in s if ch.isdigit())
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
                inv_all["Invoice Date"] = pd.to_datetime(inv_all["Invoice Date"], errors="coerce")
                inv_all = inv_all.sort_values(["Item UPC", "Invoice Date"], ascending=[True, True])
                inv_latest = inv_all.groupby("Item UPC", as_index=False).tail(1)

                # Goal Sheet 1
                gs1 = inv_latest[["Item UPC","Brand","Description","Pack","Size","Net Case Cost"]].copy()
                gs1.rename(columns={"Item UPC":"UPC","Net Case Cost":"+Cost"}, inplace=True)
                gs1["Pack"] = pd.to_numeric(gs1["Pack"], errors="coerce").fillna(0)
                gs1["+Cost"] = pd.to_numeric(gs1["+Cost"], errors="coerce").fillna(0.0)
                gs1["Unit"] = (gs1["+Cost"] / gs1["Pack"]).replace([float("inf")], 0).fillna(0.0)
                gs1["D40%"] = (gs1["Unit"] / 0.6).round(2)
                gs1["Cost"] = gs1["+Cost"]
                gs1["40%"] = ((gs1["Cost"] / gs1["Pack"]) / 0.6).replace([float("inf")], 0).fillna(0.0).round(2)

                if "UPC" in pos_df.columns:
                    pos_now = pos_df[["UPC","cents"]].copy()
                    pos_now["$Now"] = (pd.to_numeric(pos_now["cents"], errors="coerce").fillna(0) / 100).round(2)
                    pos_now = pos_now[["UPC","$Now"]]
                    gs1 = gs1.merge(pos_now, on="UPC", how="left")
                else:
                    gs1["$Now"] = 0.0

                # Goal Sheet 2
                if "UPC" in pos_df.columns:
                    keep_upcs = set(gs1["UPC"])
                    pos2 = pos_df[pos_df["UPC"].isin(keep_upcs)].copy()
                    if "cost_qty" not in pos2.columns:
                        pos2["cost_qty"] = ""
                    if "cost_cents" not in pos2.columns:
                        pos2["cost_cents"] = ""
                    join = gs1[["UPC","Pack","+Cost"]].copy()
                    join["cost_qty"] = join["Pack"].astype(int)
                    join["cost_cents"] = (join["+Cost"].round(2) * 100).astype(int)
                    join = join[["UPC","cost_qty","cost_cents"]]
                    # drop old then merge
                    pos2 = pos2.drop(columns=[c for c in ["cost_qty","cost_cents"] if c in pos2.columns]).merge(join, on="UPC", how="left")
                else:
                    pos2 = pd.DataFrame()

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
            invoice_items_df = _ensure_invoice_cols(invoice_items_df)
            if invoice_items_df.empty:
                st.error("Could not parse any SG items (no UPC/Item Name/Cost/Cases). Please check the PDF.")
            else:
                # Invoice CSV (invoice order preserved by parser) — keep leading zeros for Excel
                _download_csv_with_upc_text(invoice_items_df, "sg_invoice_items.csv", upc_col="UPC")

                updated_master, cost_changes, not_in_master, pack_missing, invoice_unique = _update_master_from_invoice(master_xlsx, invoice_items_df)
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

                    if pack_missing is not None and not pack_missing.empty:
                        st.caption("Added items with Pack == 0")
                        st.dataframe(pack_missing, use_container_width=True)
                        st.download_button("Download Added-with-Pack-0 (CSV)", pack_missing.to_csv(index=False).encode("utf-8"), "sg_added_pack_zero.csv")

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
                _download_csv_with_upc_text(invoice_items_nv, "nv_invoice_items.csv", upc_col="UPC")

                updated_master_nv, cost_changes_nv, not_in_master_nv, pack_missing_nv, invoice_unique_nv = _update_master_from_invoice(master_xlsx_nv, invoice_items_nv)
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

                    if pack_missing_nv is not None and not pack_missing_nv.empty:
                        st.caption("Added items with Pack == 0")
                        st.dataframe(pack_missing_nv, use_container_width=True)
                        st.download_button("Download Added-with-Pack-0 (CSV)", pack_missing_nv.to_csv(index=False).encode("utf-8"), "nv_added_pack_zero.csv")

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
