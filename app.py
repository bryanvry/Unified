import streamlit as st
import pandas as pd
import numpy as np
import re
import sys
import os
import importlib.util
from io import BytesIO
from datetime import datetime

# ===== vendor parsers =====
from parsers import SouthernGlazersParser, NevadaBeverageParser, BreakthruParser

# --- ROBUST IMPORT FOR JC SALES ---
def get_jcsales_parser():
    """Try to import JCSalesParser, falling back to manual file load if needed."""
    # 1. Try standard import
    try:
        from parsers import JCSalesParser
        return JCSalesParser
    except ImportError:
        pass

    # 2. Try importing from local file in 'parsers' folder manually
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_dir, "parsers", "jcsales.py")
        
        if os.path.exists(file_path):
            spec = importlib.util.spec_from_file_location("JCSalesParserModule", file_path)
            module = importlib.util.module_from_spec(spec)
            sys.modules["JCSalesParserModule"] = module
            spec.loader.exec_module(module)
            return module.JCSalesParser
    except Exception as e:
        print(f"Manual import failed: {e}")
    
    return None

JCSalesParser = get_jcsales_parser()

st.set_page_config(page_title="Unified — Multi-Vendor Invoice Processor", page_icon="🧾", layout="wide")

# ---------------- shared helpers ----------------
UNIFIED_IGNORE_UPCS = set(["000000000000", "003760010302", "023700052551"])

# --- UTILS ported from utils.py for self-containment ---
def digits_only(s):
    return re.sub(r"\D", "", str(s)) if pd.notna(s) else ""

def upc_check_digit(core11: str) -> str:
    core11 = re.sub(r"\D","",core11).zfill(11)[:11]
    if len(core11) != 11:
        return "0"
    d = [int(x) for x in core11]
    return str((10 - ((sum(d[0::2])*3 + sum(d[1::2])) % 10)) % 10)

def normalize_pos_upc(raw: str) -> str:
    d = digits_only(raw)
    if len(d) == 12: return d
    if len(d) == 11: return d + upc_check_digit(d)
    if len(d) > 12: d = d[-12:]
    return d.zfill(12)

def normalize_invoice_upc(raw: str) -> str:
    d = digits_only(raw)
    core11 = d[-11:] if len(d) >= 11 else d.zfill(11)
    return core11 + upc_check_digit(core11)

def first_int_from_text(s):
    m = re.search(r"\d+", str(s) if pd.notna(s) else "")
    return int(m.group(0)) if m else 1

def find_col(cols, candidates):
    for c in cols:
        if any(x.lower() == c.lower() for x in candidates):
            return c
    # fuzzy match
    for c in cols:
        if any(x.lower() in c.lower() for x in candidates):
            return c
    return None


def _build_pricebook_update(pricebook_csv_file, updated_master_df):
    """
    Build POS update sheet from updated Master.
    - Join on Master['Full Barcode'] ↔ Pricebook['Upc'] (both normalized to 12 digits).
    - Only include items with Total > 0 in Master.
    - Update only addstock and cost_cents in the pricebook rows that match.
    - Return (pos_update_df, pricebook_missing_df).
    """
    if updated_master_df is None or len(updated_master_df) == 0:
        return (pd.DataFrame(), pd.DataFrame())

    # Read pricebook
    try:
        if isinstance(pricebook_csv_file, pd.DataFrame):
             pb = pricebook_csv_file
        else:
             pb = pd.read_csv(pricebook_csv_file, dtype=str, keep_default_na=False, na_values=[])
    except Exception:
        return (pd.DataFrame(), pd.DataFrame())
        
    if pb.empty:
        return (pd.DataFrame(), pd.DataFrame())

    # Resolve key columns
    upc_col = "Upc" if "Upc" in pb.columns else "UPC"
    if upc_col not in pb.columns:
        return (pd.DataFrame(), pd.DataFrame())

    # Normalize PB UPCs
    pb["_norm_upc"] = pb[upc_col].astype(str).apply(normalize_pos_upc)

    # Filter master for items that have invoice quantity
    if "Total" not in updated_master_df.columns:
         return (pd.DataFrame(), pd.DataFrame())

    invoice_items = updated_master_df[pd.to_numeric(updated_master_df["Total"], errors='coerce').fillna(0) > 0].copy()

    if invoice_items.empty:
        return (pd.DataFrame(), pd.DataFrame())

    invoice_items["_norm_upc"] = invoice_items["Full Barcode"].astype(str).apply(normalize_pos_upc)

    # Merge
    merged = pd.merge(
        pb,
        invoice_items[["_norm_upc", "Total", "Cost"]],
        on="_norm_upc",
        how="left",
        indicator=True
    )

    # Items in Invoice but NOT in Pricebook
    # We need to check which invoice_items did not find a match in pb
    matched_upcs = set(merged[merged["_merge"] == "both"]["_norm_upc"])
    missing_in_pb = invoice_items[~invoice_items["_norm_upc"].isin(matched_upcs)].copy()

    # Filter PB rows that matched (where we have updates)
    pos_update = merged[merged["_merge"] == "both"].copy()
    
    # Update columns
    pos_update["addstock"] = pd.to_numeric(pos_update["Total"], errors='coerce').fillna(0).astype(int)
    
    def cost_to_cents(val):
        try:
            return int(round(float(val) * 100))
        except:
            return 0
            
    pos_update["cost_cents"] = pos_update["Cost"].apply(cost_to_cents)

    # Drop temp columns
    pos_update = pos_update.drop(columns=["_norm_upc", "Total", "Cost", "_merge"])
    
    return pos_update, missing_in_pb

def _ensure_invoice_cols(df):
    required = ["UPC", "Item Name", "Cost", "Cases"]
    for c in required:
        if c not in df.columns:
            df[c] = ""
    return df[required]

def _update_master_from_invoice(master_file, invoice_df):
    """
    Updates Master file with Invoice data.
    Returns: (updated_master, cost_changes, not_in_master, pack_missing, invoice_unique)
    """
    # Load Master
    try:
        mst = pd.read_excel(master_file, dtype=str)
    except:
        return None, None, None, None, None

    # Normalize Master UPCs (Full Barcode)
    if "Full Barcode" not in mst.columns:
        return None, None, None, None, None

    mst["_norm_upc"] = mst["Full Barcode"].apply(normalize_pos_upc)
    invoice_df["_norm_upc"] = invoice_df["UPC"].apply(normalize_pos_upc)

    # 1. Identify Items NOT in Master
    master_upcs = set(mst["_norm_upc"])
    not_in_master = invoice_df[~invoice_df["_norm_upc"].isin(master_upcs)].copy()

    # 2. Merge Invoice into Master
    # Aggregate invoice items (duplicate UPCs? sum cases, avg cost?)
    inv_grp = invoice_df.groupby("_norm_upc").agg({
        "Cost": "max", # Assume highest cost is new cost
        "Cases": "sum"
    }).reset_index()
    
    # Merge
    merged = pd.merge(mst, inv_grp, on="_norm_upc", how="left", suffixes=("", "_new"))
    
    # 3. Detect Cost Changes
    merged["Cost"] = pd.to_numeric(merged["Cost"], errors='coerce').fillna(0)
    merged["Cost_new"] = pd.to_numeric(merged["Cost_new"], errors='coerce').fillna(0)
    
    # Identify changes (where Cost_new > 0 and different from Cost)
    cost_changes = merged[
        (merged["Cost_new"] > 0) & 
        (abs(merged["Cost"] - merged["Cost_new"]) > 0.009)
    ].copy()
    
    cost_changes["Old Cost"] = cost_changes["Cost"]
    cost_changes["New Cost"] = cost_changes["Cost_new"]
    
    # 4. Update Master Cost
    merged.loc[merged["Cost_new"] > 0, "Cost"] = merged["Cost_new"]

    # 5. Calculate Total = Pack * Cases (from invoice)
    merged["Pack"] = pd.to_numeric(merged["Pack"], errors='coerce').fillna(0)
    merged["Cases"] = pd.to_numeric(merged["Cases"], errors='coerce').fillna(0)
    
    merged["Total"] = merged["Pack"] * merged["Cases"]
    
    # 6. Missing Pack (Items on invoice where Master Pack is 0 or NaN)
    pack_missing = merged[
        (merged["Cases"] > 0) & 
        (merged["Pack"] <= 0)
    ].copy()

    # Cleanup
    final_master = merged.drop(columns=["_norm_upc", "Cost_new", "Cases"]) 
    
    return final_master, cost_changes, not_in_master, pack_missing, inv_grp

def df_to_csv_bytes(df):
    return df.to_csv(index=False).encode('utf-8')

# --- UNIFIED HELPERS ---
def process_unified(pos_csv_file, unified_files):
    # Load POS
    pos_df = pd.read_csv(pos_csv_file, dtype=str, keep_default_na=False)
    
    # Normalize POS UPCs
    upc_header = "Upc" if "Upc" in pos_df.columns else "UPC"
    pos_df["_norm_upc"] = pos_df[upc_header].apply(normalize_pos_upc)
    
    # Load Unified Parsed Data
    from parsers import UnifiedParser as UPClass
    parser = UPClass()
    
    all_inv_dfs = []
    for f in unified_files:
        try:
            f.seek(0)
            df = parser.parse(f)
            all_inv_dfs.append(df)
        except Exception as e:
            st.error(f"Error parsing {f.name}: {e}")
            
    if not all_inv_dfs:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        
    inv_all = pd.concat(all_inv_dfs, ignore_index=True)
    
    # Logic: Keep latest invoice date per UPC
    inv_all["invoice_date"] = pd.to_datetime(inv_all["invoice_date"], errors='coerce')
    inv_all = inv_all.sort_values("invoice_date", ascending=True)
    inv_all = inv_all.drop_duplicates(subset=["UPC"], keep="last") # Keep latest
    
    # Filter Ignore List
    inv_all = inv_all[~inv_all["UPC"].isin(UNIFIED_IGNORE_UPCS)]
    
    # Join Invoice -> POS
    merged = pd.merge(
        inv_all,
        pos_df,
        left_on="UPC",
        right_on="_norm_upc",
        how="left",
        indicator=True
    )
    
    # Matched items
    matched = merged[merged["_merge"] == "both"].copy()
    
    # 1. FULL EXPORT (Merged data)
    full_export_df = matched.copy()
    
    # 2. POS UPDATE (Changes)
    matched["+Cost"] = pd.to_numeric(matched["+Cost"], errors='coerce').fillna(0)
    matched["Pack"] = pd.to_numeric(matched["Pack"], errors='coerce').fillna(1)
    
    matched["cost_cents_num"] = pd.to_numeric(matched["cost_cents"], errors='coerce').fillna(0)
    matched["cost_qty_num"] = pd.to_numeric(matched["cost_qty"], errors='coerce').fillna(1)
    
    matched["new_cost_cents"] = (matched["+Cost"] * 100).astype(int)
    
    changes = matched[
        (matched["new_cost_cents"] != matched["cost_cents_num"]) |
        (matched["Pack"] != matched["cost_qty_num"])
    ].copy()
    
    changes["cost_cents"] = changes["new_cost_cents"]
    changes["cost_qty"] = changes["Pack"]
    
    pos_cols = [c for c in pos_df.columns if c != "_norm_upc"]
    pos_update_df = changes[pos_cols].copy()
    
    # 3. Goal Sheet 1 (Audit)
    gs1 = matched.copy()
    gs1["+Cost"] = pd.to_numeric(gs1["+Cost"], errors="coerce")
    gs1["Cost"]  = pd.to_numeric(gs1["Cost"], errors="coerce") # Unit Cost from Inv
    gs1["Pack"]  = pd.to_numeric(gs1["Pack"], errors="coerce")
    gs1.loc[gs1["Pack"].isna() | (gs1["Pack"]<=0), "Pack"] = 1
    
    gs1["Unit"]  = gs1["+Cost"] / gs1["Pack"]
    
    gs1["D40%"]  = gs1["Unit"] / 0.6
    gs1["40%"]   = (gs1["Cost"] / gs1["Pack"]) / 0.6 
    
    cents_col = "cents" if "cents" in pos_df.columns else "Price" 
    if cents_col not in pos_df.columns: cents_col = None
    
    def cents_to_dollars(v):
        try:
            return float(str(v))/100.0
        except:
            return np.nan
    
    if cents_col:
        gs1["$Now"] = gs1[cents_col].apply(cents_to_dollars)
    else:
        gs1["$Now"] = 0.0

    pos_unit_cost = gs1["cost_cents_num"] / 100.0
    with np.errstate(divide='ignore', invalid='ignore'):
        pos_unit = pos_unit_cost / gs1["cost_qty_num"]
        pos_d40  = pos_unit / 0.6
        
    delta = gs1["D40%"] - pos_d40
    tol = 0.005
    gs1["Delta"] = delta.apply(lambda x: "=" if pd.notna(x) and abs(x)<tol else (round(float(x),2) if pd.notna(x) else np.nan))

    gs1_out = gs1[["UPC","Brand","Description","Pack","Size","Cost","+Cost","Unit","D40%","40%","$Now","Delta"]].copy()
    gs1_out["UPC"] = gs1_out["UPC"].astype(str).str.zfill(12)
    gs1_out = gs1_out.dropna(subset=["+Cost"]).sort_values("UPC").reset_index(drop=True)

    # 4. Unmatched
    unmatched = inv_all[~inv_all["UPC"].isin(matched["UPC"])][
        ["UPC","Brand","Description","Pack","+Cost","Case Qty","invoice_date"]
    ].copy() if not inv_all.empty else pd.DataFrame()

    full_export_df = full_export_df.loc[:, ~full_export_df.columns.duplicated()].copy()
    pos_update_df  = pos_update_df.loc[:,  ~pos_update_df.columns.duplicated()].copy()

    return full_export_df, pos_update_df, gs1_out, unmatched

# --- HELPER: Define the missing function ---
def load_dataframe(uploaded_file):
    """Helper to load CSV or Excel from Streamlit uploader."""
    if uploaded_file is None:
        return None
    try:
        name = uploaded_file.name.lower()
        if name.endswith(('.xls', '.xlsx')):
            return pd.read_excel(uploaded_file, dtype=str)
        else:
            return pd.read_csv(uploaded_file, dtype=str)
    except Exception as e:
        st.error(f"Error loading {uploaded_file.name}: {e}")
        return None
```

### **Part 2: The User Interface (Append to the end of `app.py`)**

This code starts with `# ---------------- UI ----------------`. Paste it after the code block above.

```python
# ---------------- UI ----------------

# DROPDOWN MENU CONFIGURATION
VENDOR_OPTIONS = [
    "Unified (SVMERCH)", 
    "Southern Glazer's", 
    "Nevada Beverage", 
    "Breakthru", 
    "JC Sales"
]

selected_vendor = st.selectbox("Select Vendor Source", VENDOR_OPTIONS)
st.divider()

# ===== Unified tab =====
if selected_vendor == "Unified (SVMERCH)":
    st.title("🧾 Unified → POS Processor")
    st.caption("Upload Unified invoice(s) + POS CSV to get POS updates, full export, and an audit workbook with Goal Sheet 1.")

    pos_file = st.file_uploader("Upload POS pricebook CSV", type=["csv"], accept_multiple_files=False, key="un_pos")
    inv_files = st.file_uploader("Upload Unified invoice file(s) (XLSX/XLS/CSV)", type=["xlsx","xls","csv"], accept_multiple_files=True, key="un_inv")

    if st.button("Process Unified", type="primary"):
        if not pos_file or not inv_files:
            st.warning("Upload a POS CSV and at least one Unified invoice file.")
        else:
            with st.spinner("Processing Unified…"):
                full_export_df, pos_update_df, gs1_out, unmatched = process_unified(pos_file, inv_files)

            st.session_state["full_export_df"] = full_export_df
            st.session_state["pos_update_df"]  = pos_update_df
            st.session_state["gs1_df"]         = gs1_out
            st.session_state["unmatched_df"]   = unmatched
            st.session_state["ts"]             = datetime.now().strftime("%Y%m%d_%H%M%S")

            st.success(f"Done! FULL rows: {len(full_export_df)}  |  Only-changed: {len(pos_update_df)}  |  Unmatched: {len(unmatched)}")

    if "full_export_df" in st.session_state and st.session_state["full_export_df"] is not None:
        ts = st.session_state["ts"]

        col1, col2, col3 = st.columns(3)
        with col1:
            st.download_button(
                "⬇️ POS Update (only changed) — CSV",
                data=df_to_csv_bytes(st.session_state["pos_update_df"]),
                file_name=f"POS_Update_OnlyChanged_{ts}.csv",
                mime="text/csv",
                key="dl_changed_csv",
            )
        with col2:
            st.download_button(
                "⬇️ FULL Export — CSV",
                data=df_to_csv_bytes(st.session_state["full_export_df"]),
                file_name=f"FULL_Unified_Export_{ts}.csv",
                mime="text/csv",
                key="dl_full_csv",
            )
        with col3:
            st.download_button(
                "⬇️ Goal Sheet 1 (Preview) — CSV",
                data=df_to_csv_bytes(st.session_state["gs1_df"]),
                file_name=f"Goal_Sheet_1_{ts}.csv",
                mime="text/csv",
                key="dl_gs1_csv"
            )
        
        st.subheader("Preview — FULL Export (first 200)")
        st.dataframe(st.session_state["full_export_df"].head(200), use_container_width=True)
        
        st.subheader("Preview — Goal Sheet 1 (first 100)")
        st.dataframe(st.session_state["gs1_df"].head(100), use_container_width=True)

        st.subheader("Unmatched (first 200)")
        st.dataframe(st.session_state["unmatched_df"].head(200), use_container_width=True)


# ===== Southern Glazer's tab =====
if selected_vendor == "Southern Glazer's":
    st.title("Southern Glazer's Processor")
    inv_files = st.file_uploader("Upload SG invoice PDF(s) or CSV/XLSX", type=["pdf","csv","xlsx","xls"], accept_multiple_files=True, key="sg_inv")
    master_xlsx = st.file_uploader("Upload Master workbook (.xlsx)", type=["xlsx"], key="sg_master")
    pricebook_csv = st.file_uploader("Upload pricebook CSV (optional for POS update)", type=["csv"], key="sg_pb")

    if st.button("Process SG", type="primary"):
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
                st.error("Could not parse any SG items (no UPC/Item Name/Cost/Cases). Please check the file.")
            else:
                updated_master, cost_changes, not_in_master, pack_missing, invoice_unique = _update_master_from_invoice(master_xlsx, invoice_items_df)
                
                pos_update = None
                pb_missing = None
                if pricebook_csv is not None and updated_master is not None:
                    pricebook_csv.seek(0)
                    pos_update, pb_missing = _build_pricebook_update(pricebook_csv, updated_master)

                st.session_state["sg_invoice_items_df"] = invoice_items_df
                st.session_state["sg_updated_master"]   = updated_master
                st.session_state["sg_cost_changes"]     = cost_changes
                st.session_state["sg_not_in_master"]    = not_in_master
                st.session_state["sg_pack_missing"]     = pack_missing
                st.session_state["sg_pos_update"]       = pos_update
                st.session_state["sg_pb_missing"]       = pb_missing
                st.session_state["sg_ts"]               = datetime.now().strftime("%Y%m%d_%H%M%S")

                st.success("Southern Glazer's — processing complete.")

    if "sg_invoice_items_df" in st.session_state and st.session_state["sg_invoice_items_df"] is not None:
        sg_ts = st.session_state["sg_ts"] or datetime.now().strftime("%Y%m%d_%H%M%S")

        st.subheader("Invoice Items (parsed, in-invoice order)")
        st.dataframe(st.session_state["sg_invoice_items_df"].head(100), use_container_width=True)

        inv_items_df = st.session_state["sg_invoice_items_df"].copy()
        if "UPC" in inv_items_df.columns:
            inv_items_df["UPC"] = inv_items_df["UPC"].astype(str).map(lambda x: f'="{x}"')
        st.download_button(
            "⬇️ Invoice Items (CSV)",
            data=df_to_csv_bytes(inv_items_df),
            file_name=f"sg_invoice_items_{sg_ts}.csv",
            mime="text/csv",
            key="sg_dl_inv"
        )

        st.subheader("Updated Master (preview)")
        st.dataframe(st.session_state["sg_updated_master"].head(100), use_container_width=True)
        st.download_button(
            "⬇️ Updated Master (CSV)",
            data=df_to_csv_bytes(st.session_state["sg_updated_master"]),
            file_name=f"sg_updated_master_{sg_ts}.csv",
            mime="text/csv",
            key="sg_dl_mst"
        )
        
        st.subheader("POS Update (preview)")
        if st.session_state["sg_pos_update"] is not None and not st.session_state["sg_pos_update"].empty:
            st.dataframe(st.session_state["sg_pos_update"], use_container_width=True)
            st.download_button(
                "⬇️ POS Update (CSV)",
                data=df_to_csv_bytes(st.session_state["sg_pos_update"]),
                file_name=f"sg_pos_update_{sg_ts}.csv",
                mime="text/csv",
                key="sg_dl_pos"
            )
        else:
            st.info("No POS updates generated (pricebook missing or no matches).")

        if st.session_state["sg_cost_changes"] is not None and not st.session_state["sg_cost_changes"].empty:
            st.write("---")
            st.subheader("Cost Changes (Diff > 0.009)")
            st.dataframe(st.session_state["sg_cost_changes"], use_container_width=True)

        if st.session_state["sg_not_in_master"] is not None and not st.session_state["sg_not_in_master"].empty:
            st.write("---")
            st.subheader("Items NOT in Master")
            st.dataframe(st.session_state["sg_not_in_master"], use_container_width=True)

        if st.session_state["sg_pb_missing"] is not None and not st.session_state["sg_pb_missing"].empty:
            st.write("---")
            st.subheader("Items in Invoice but NOT in Pricebook")
            st.dataframe(st.session_state["sg_pb_missing"], use_container_width=True)
            st.download_button(
                "⬇️ Pricebook Missing (CSV)",
                data=df_to_csv_bytes(st.session_state["sg_pb_missing"]),
                file_name=f"pricebook_missing_sg_{sg_ts}.csv",
                key="sg_dl_pb_missing"
            )


# ===== Nevada Beverage tab =====
if selected_vendor == "Nevada Beverage":
    st.title("Nevada Beverage Processor")
    inv_files = st.file_uploader("Upload Nevada invoice PDF(s)", type=["pdf"], accept_multiple_files=True, key="nv_inv")
    master_xlsx = st.file_uploader("Upload Master workbook (.xlsx)", type=["xlsx"], key="nv_master")
    pricebook_csv = st.file_uploader("Upload pricebook CSV (optional for POS update)", type=["csv"], key="nv_pb")

    if st.button("Process Nevada", type="primary"):
        if not inv_files or not master_xlsx:
            st.error("Please upload at least one Nevada invoice and the Master workbook.")
        else:
            nv_parser = NevadaBeverageParser()
            parts = []
            for f in inv_files:
                f.seek(0)
                df = nv_parser.parse(f)
                if not df.empty:
                    parts.append(df)
            invoice_items_df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["UPC","Item Name","Cost","Cases"])
            invoice_items_df = _ensure_invoice_cols(invoice_items_df)

            if invoice_items_df.empty:
                st.error("Could not parse any Nevada items (no UPC/Item Name/Cost/Cases). Please check the file.")
            else:
                updated_master, cost_changes, not_in_master, pack_missing, invoice_unique = _update_master_from_invoice(master_xlsx, invoice_items_df)
                
                pos_update = None
                pb_missing = None
                if pricebook_csv is not None and updated_master is not None:
                    pricebook_csv.seek(0)
                    pos_update, pb_missing = _build_pricebook_update(pricebook_csv, updated_master)

                st.session_state["nv_invoice_items_df"] = invoice_items_df
                st.session_state["nv_updated_master"]   = updated_master
                st.session_state["nv_cost_changes"]     = cost_changes
                st.session_state["nv_not_in_master"]    = not_in_master
                st.session_state["nv_pack_missing"]     = pack_missing
                st.session_state["nv_pos_update"]       = pos_update
                st.session_state["nv_pb_missing"]       = pb_missing
                st.session_state["nv_ts"]               = datetime.now().strftime("%Y%m%d_%H%M%S")

                st.success("Nevada Beverage — processing complete.")

    if "nv_invoice_items_df" in st.session_state and st.session_state["nv_invoice_items_df"] is not None:
        nv_ts = st.session_state["nv_ts"] or datetime.now().strftime("%Y%m%d_%H%M%S")

        st.subheader("Invoice Items (parsed, in-invoice order)")
        st.dataframe(st.session_state["nv_invoice_items_df"].head(100), use_container_width=True)
        st.download_button(
            "⬇️ Invoice Items (CSV)",
            data=df_to_csv_bytes(st.session_state["nv_invoice_items_df"]),
            file_name=f"nv_invoice_items_{nv_ts}.csv",
            key="nv_dl_items"
        )

        st.subheader("Updated Master (preview)")
        if st.session_state["nv_updated_master"] is not None:
            st.dataframe(st.session_state["nv_updated_master"].head(100), use_container_width=True)
            st.download_button(
                "⬇️ Updated Master (CSV)",
                data=df_to_csv_bytes(st.session_state["nv_updated_master"]),
                file_name=f"nv_updated_master_{nv_ts}.csv",
                key="nv_dl_master"
            )
        
        st.subheader("POS Update (preview)")
        if st.session_state["nv_pos_update"] is not None and not st.session_state["nv_pos_update"].empty:
            st.dataframe(st.session_state["nv_pos_update"], use_container_width=True)
            st.download_button(
                "⬇️ POS Update (CSV)",
                data=df_to_csv_bytes(st.session_state["nv_pos_update"]),
                file_name=f"nv_pos_update_{nv_ts}.csv",
                key="nv_dl_pos"
            )
        else:
            st.info("No POS updates generated.")

        if st.session_state["nv_cost_changes"] is not None and not st.session_state["nv_cost_changes"].empty:
            st.write("---")
            st.subheader("Cost Changes (Diff > 0.009)")
            st.dataframe(st.session_state["nv_cost_changes"], use_container_width=True)

        if st.session_state["nv_not_in_master"] is not None and not st.session_state["nv_not_in_master"].empty:
            st.write("---")
            st.subheader("Items NOT in Master")
            st.dataframe(st.session_state["nv_not_in_master"], use_container_width=True)

        if st.session_state["nv_pb_missing"] is not None and not st.session_state["nv_pb_missing"].empty:
            st.write("---")
            st.subheader("Items in Invoice but NOT in Pricebook")
            st.dataframe(st.session_state["nv_pb_missing"], use_container_width=True)
            st.download_button(
                "⬇️ Pricebook Missing (CSV)",
                data=df_to_csv_bytes(st.session_state["nv_pb_missing"]),
                file_name=f"pricebook_missing_nv_{nv_ts}.csv",
                key="nv_dl_pb_missing"
            )


# ===== Breakthru =====
if selected_vendor == "Breakthru":
    st.title("Breakthru Processor")
    inv_files = st.file_uploader("Upload Breakthru invoice PDF(s) or Excel/CSV", type=["pdf", "xlsx", "xls", "csv"], accept_multiple_files=True, key="bt_inv")
    master_xlsx = st.file_uploader("Upload Master workbook (.xlsx)", type=["xlsx"], key="bt_master")
    pricebook_csv = st.file_uploader("Upload pricebook CSV (optional for POS update)", type=["csv"], key="bt_pb")

    if st.button("Process Breakthru", type="primary"):
        if not inv_files or not master_xlsx:
            st.error("Please upload at least one Breakthru invoice and the Master workbook.")
        else:
            bt_parser = BreakthruParser()
            parts = []
            for f in inv_files:
                f.seek(0)
                df = bt_parser.parse(f)
                if not df.empty:
                    parts.append(df)
            invoice_items_df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["UPC","Item Name","Cost","Cases"])
            invoice_items_df = _ensure_invoice_cols(invoice_items_df)

            if invoice_items_df.empty:
                st.error("Could not parse any Breakthru items. Check the file.")
            else:
                # Logic Fix: Reverted to using your existing helper
                updated_master, cost_changes, not_in_master, pack_missing, invoice_unique = _update_master_from_invoice(master_xlsx, invoice_items_df)
                
                pos_update = None
                pb_missing = None
                if pricebook_csv is not None and updated_master is not None:
                    # Logic Fix: Pass the FILE OBJECT
                    pricebook_csv.seek(0)
                    pos_update, pb_missing = _build_pricebook_update(pricebook_csv, updated_master)

                st.session_state["bt_invoice_items_df"] = invoice_items_df
                st.session_state["bt_updated_master"]   = updated_master
                st.session_state["bt_cost_changes"]     = cost_changes
                st.session_state["bt_not_in_master"]    = not_in_master
                st.session_state["bt_pack_missing"]     = pack_missing
                st.session_state["bt_pos_update"]       = pos_update
                st.session_state["bt_pb_missing"]       = pb_missing
                st.session_state["bt_ts"]               = datetime.now().strftime("%Y%m%d_%H%M%S")

                st.success("Breakthru — processing complete.")

    if "bt_invoice_items_df" in st.session_state and st.session_state["bt_invoice_items_df"] is not None:
        bt_ts = st.session_state["bt_ts"] or datetime.now().strftime("%Y%m%d_%H%M%S")

        st.subheader("Invoice Items (parsed, in-invoice order)")
        st.dataframe(st.session_state["bt_invoice_items_df"].head(100), use_container_width=True)
        st.download_button(
            "⬇️ Invoice Items (CSV)",
            data=df_to_csv_bytes(st.session_state["bt_invoice_items_df"]),
            file_name=f"bt_invoice_items_{bt_ts}.csv",
            key="bt_dl_items"
        )

        st.subheader("Updated Master (preview)")
        if st.session_state["bt_updated_master"] is not None:
            st.dataframe(st.session_state["bt_updated_master"].head(100), use_container_width=True)
            st.download_button(
                "⬇️ Updated Master (CSV)",
                data=df_to_csv_bytes(st.session_state["bt_updated_master"]),
                file_name=f"bt_updated_master_{bt_ts}.csv",
                key="bt_dl_master"
            )
        
        st.subheader("POS Update (preview)")
        if st.session_state["bt_pos_update"] is not None and not st.session_state["bt_pos_update"].empty:
            st.dataframe(st.session_state["bt_pos_update"], use_container_width=True)
            st.download_button(
                "⬇️ POS Update (CSV)",
                data=df_to_csv_bytes(st.session_state["bt_pos_update"]),
                file_name=f"bt_pos_update_{bt_ts}.csv",
                key="bt_dl_pos"
            )
        else:
            st.info("No POS updates generated.")

        if st.session_state["bt_cost_changes"] is not None and not st.session_state["bt_cost_changes"].empty:
            st.write("---")
            st.subheader("Cost Changes (Diff > 0.009)")
            st.dataframe(st.session_state["bt_cost_changes"], use_container_width=True)

        if st.session_state["bt_not_in_master"] is not None and not st.session_state["bt_not_in_master"].empty:
            st.write("---")
            st.subheader("Items NOT in Master")
            st.dataframe(st.session_state["bt_not_in_master"], use_container_width=True)

        if st.session_state["bt_pb_missing"] is not None and not st.session_state["bt_pb_missing"].empty:
            st.write("---")
            st.subheader("Items in Invoice but NOT in Pricebook")
            st.dataframe(st.session_state["bt_pb_missing"], use_container_width=True)
            st.download_button(
                "⬇️ Pricebook Missing (CSV)",
                data=df_to_csv_bytes(st.session_state["bt_pb_missing"]),
                file_name=f"pricebook_missing_bthru_{bt_ts}.csv",
                key="bt_dl_pb_missing"
            )


# ===== JC SALES =====
if selected_vendor == "JC Sales":
    st.title("🛒 JC Sales Parser")
    st.caption("Process JC Sales PDF Invoice against Master & Pricebook.")

    if JCSalesParser is None:
        st.error("⚠️ The JC Sales Parser module could not be loaded.")
        st.info("Debug: Please ensure 'parsers/jcsales.py' exists.")
    else:
        jc_col1, jc_col2, jc_col3 = st.columns(3)
        with jc_col1:
            jc_invoice = st.file_uploader("JC Sales Invoice (PDF)", type=["pdf"], key="jc_inv")
        with jc_col2:
            jc_master = st.file_uploader("JC Sales Master (XLSX/CSV)", type=["xlsx", "xls", "csv"], key="jc_mst")
        with jc_col3:
            jc_pb = st.file_uploader("POS Pricebook (CSV)", type=["csv"], key="jc_pb")

        if st.button("Process JC Sales", type="primary"):
            if not jc_invoice or not jc_master or not jc_pb:
                st.warning("Please upload Invoice, Master File, and Pricebook.")
            else:
                with st.spinner("Parsing JC Sales..."):
                    try:
                        # Instantiate the manually loaded class
                        parser = JCSalesParser()
                        
                        # Reset file pointers
                        jc_invoice.seek(0)
                        jc_master.seek(0)
                        jc_pb.seek(0)

                        # Run Logic
                        parsed_df, pos_update_df = parser.parse(jc_invoice, jc_master, jc_pb)
                        
                        st.session_state["jc_parsed"] = parsed_df
                        st.session_state["jc_pos_update"] = pos_update_df
                        
                        inv_name = jc_invoice.name
                        match = re.search(r"(OSI\d+)", inv_name, re.IGNORECASE)
                        inv_num = match.group(1) if match else "Parsed_Invoice"
                        st.session_state["jc_inv_num"] = inv_num
                        
                        st.success("Processing Complete!")
                    except Exception as e:
                        st.error(f"An error occurred: {e}")
                        # Print full stack trace to console for debugging
                        import traceback
                        traceback.print_exc()

        # Display Results
        if "jc_parsed" in st.session_state and st.session_state["jc_parsed"] is not None:
            inv_num = st.session_state.get("jc_inv_num", "Invoice")
            
            st.divider()
            
            c1, c2 = st.columns(2)
            
            with c1:
                st.subheader("Parsed Invoice (Goal Sheet)")
                st.dataframe(st.session_state["jc_parsed"], use_container_width=True)
                
                # Create Excel file in memory
                excel_buffer = BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                    st.session_state["jc_parsed"].to_excel(writer, index=False, sheet_name="Parsed")
                
                st.download_button(
                    label=f"⬇️ Download parsed_{inv_num}.xlsx",
                    data=excel_buffer.getvalue(),
                    file_name=f"parsed_{inv_num}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            with c2:
                st.subheader("POS Update File")
                st.dataframe(st.session_state["jc_pos_update"], use_container_width=True)
                
                # Download POS Update as CSV
                csv_data = st.session_state["jc_pos_update"].to_csv(index=False).encode('utf-8')
                st.download_button(
                    label=f"⬇️ Download POS_update_{inv_num}.csv",
                    data=csv_data,
                    file_name=f"POS_update_{inv_num}.csv",
                    mime="text/csv"
                )
