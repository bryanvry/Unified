import streamlit as st
import pandas as pd
import numpy as np
import re
from io import BytesIO
from datetime import datetime

# ===== vendor parsers =====
from parsers import SouthernGlazersParser, NevadaBeverageParser, BreakthruParser, JCSalesParser, CostcoParser

st.set_page_config(page_title="Unified — Multi-Vendor Invoice Processor", page_icon="🧾", layout="wide")
# --- NEW: Force Sidebar to be smaller ---
st.markdown(
    """
    <style>
        section[data-testid="stSidebar"] {
            width: 100px !important; # Set the width to your liking
        }
    </style>
    """,
    unsafe_allow_html=True,
)
# ---------------- shared helpers ----------------
UNIFIED_IGNORE_UPCS = set(["000000000000", "003760010302", "023700052551"])
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
    pb = pd.read_csv(pricebook_csv_file, dtype=str, keep_default_na=False, na_values=[])
    if pb.empty:
        return (pd.DataFrame(), pd.DataFrame())

    # Resolve key columns
    upc_col = "Upc" if "Upc" in pb.columns else ("UPC" if "UPC" in pb.columns else pb.columns[0])

    # Normalize UPCs in pricebook and master
    pb = pb.copy()
    pb["__pb_upc_norm"] = pb[upc_col].astype(str).map(normalize_pos_upc)

    mast = updated_master_df.copy()
    fb_col = _resolve_col(mast, ["Full Barcode","FullBarcode","FULL BARCODE"], "Full Barcode")
    total_col = _resolve_col(mast, ["Total","TOTAL","total"], "Total")
    cents_col = _resolve_col(mast, ["Cost ¢","Cost cents","Cost c","COST ¢"], "Cost ¢")

    # Numeric Total; keep only >0
    mast["__Total_num"] = mast[total_col].apply(_to_float_safe)
    mast["__Cost_cents"] = mast[cents_col].apply(_to_int_safe)
    mast["__fb_norm"] = mast[fb_col].astype(str).map(normalize_pos_upc)

    mast_used = mast[mast["__Total_num"] > 0].copy()

    # Which master rows aren’t present in the pricebook
    pb_upcs = set(pb["__pb_upc_norm"])
    missing_mask = ~mast_used["__fb_norm"].isin(pb_upcs)
    pricebook_missing = (
        mast_used.loc[missing_mask, [fb_col, total_col, cents_col]]
        .rename(columns={fb_col: "Full Barcode", total_col: "Total", cents_col: "Cost ¢"})
        .reset_index(drop=True)
    )

    # Keep only rows we can actually update (present in pricebook)
    mast_used = mast_used[~missing_mask].copy()
    if mast_used.empty:
        return (pd.DataFrame(), pricebook_missing)

    # Build mapping Full Barcode → (Total, Cost ¢)
    map_total = dict(zip(mast_used["__fb_norm"], mast_used["__Total_num"]))
    map_cents = dict(zip(mast_used["__fb_norm"], mast_used["__Cost_cents"]))

    # Apply updates to a copy of the pricebook
    out = pb.copy()
    out["_new_addstock"] = out["__pb_upc_norm"].map(map_total).fillna(0)
    out["_new_cost_cents"] = out["__pb_upc_norm"].map(map_cents).fillna(0).astype(int)

    # Only keep rows that were actually in the invoice (Total>0 mapping exists)
    updated_rows = out[out["_new_addstock"] > 0].copy()
    if updated_rows.empty:
        return (pd.DataFrame(), pricebook_missing)

    # Write into the canonical columns (create if missing)
    if "addstock" not in updated_rows.columns:
        updated_rows["addstock"] = 0
    if "cost_cents" not in updated_rows.columns:
        updated_rows["cost_cents"] = 0

    updated_rows["addstock"] = updated_rows["_new_addstock"]
    updated_rows["cost_cents"] = updated_rows["_new_cost_cents"]

    # Drop helper cols
    updated_rows = updated_rows.drop(columns=["_new_addstock","_new_cost_cents"], errors="ignore")

    # Return just the updated subset (POS upload file) + the missing list
    # Keep original order of columns
    updated_rows = updated_rows[pb.columns] if set(pb.columns).issubset(set(updated_rows.columns)) else updated_rows
    return (updated_rows.reset_index(drop=True), pricebook_missing.reset_index(drop=True))

def digits_only(s):
    return re.sub(r"\D", "", str(s)) if pd.notna(s) else ""

def upc_check_digit(core11: str) -> str:
    core11 = re.sub(r"\D","",core11).zfill(11)[:11]
    if len(core11) != 11:
        return "0"
    d = [int(x) for x in core11]
    return str((10 - ((sum(d[0::2])*3 + sum(d[1::2])) % 10)) % 10)

def normalize_invoice_upc(raw: str) -> str:
    d = digits_only(raw)
    core11 = d[-11:] if len(d) >= 11 else d.zfill(11)
    return core11 + upc_check_digit(core11)

def normalize_pos_upc(raw: str) -> str:
    d = digits_only(raw)
    if len(d) == 12:
        return d
    if len(d) == 11:
        return d + upc_check_digit(d)
    if len(d) > 12:
        d = d[-12:]
    return d.zfill(12)

def first_int_from_text(s):
    m = re.search(r"\d+", str(s) if pd.notna(s) else "")
    return int(m.group(0)) if m else np.nan

def to_float(x):
    if pd.isna(x):
        return np.nan
    if isinstance(x,(int,float,np.number)):
        return float(x)
    s = str(x).replace("$","").replace(",","").strip()
    try:
        return float(s)
    except:
        return np.nan

def find_col(cols, candidates):
    low = [c.lower() for c in cols]
    for cand in candidates:
        if cand.lower() in low:
            return cols[low.index(cand.lower())]
    for cand in candidates:
        for i,c in enumerate(low):
            if cand.lower() in c:
                return cols[i]
    return None

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")

def dfs_to_xlsx_bytes(dfs: dict) -> bytes:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        for name, d in dfs.items():
            d.to_excel(writer, sheet_name=name[:31], index=False)
    bio.seek(0)
    return bio.getvalue()

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
    s = str(u or "").strip().replace("-", "").replace(" ", "")
    s = "".join(ch for ch in s if ch.isdigit())
    if len(s) == 13 and s.startswith("0"):
        s = s[1:]
    if len(s) > 12:
        s = s[-12:]
    if len(s) < 12:
        s = s.zfill(12)
    return s

def _resolve_col(df: pd.DataFrame, candidates, default_name):
    for cand in candidates:
        if cand in df.columns:
            return cand
    if default_name not in df.columns:
        df[default_name] = ""
    return default_name

# -------- SG/NV/Breakthru shared helpers --------
def _ensure_invoice_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure columns: UPC, Item Name, Cost, Cases. Normalize UPC to 12-digit numeric string."""
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

def _update_master_from_invoice(master_xlsx, invoice_df: pd.DataFrame):
    """Update Master using invoice_df (must have UPC, Item Name, Cost, Cases). Match on Master['Invoice UPC']."""
    invoice_df = _ensure_invoice_cols(invoice_df)
    if invoice_df.empty:
        return (None, None, None, None, None)

    master = pd.read_excel(master_xlsx, dtype=str).fillna("")

    name_col         = _resolve_col(master, ["Name","NAME","name"], "Name")
    pack_col         = _resolve_col(master, ["Pack","PACK","pack"], "Pack")
    cases_col        = _resolve_col(master, ["Cases","CASES","cases"], "Cases")
    total_col        = _resolve_col(master, ["Total","TOTAL","total"], "Total")
    cost_dollar_col  = _resolve_col(master, ["Cost $","Cost$","COST $","cost $"], "Cost $")
    cost_cent_col    = _resolve_col(master, ["Cost ¢","Cost cents","Cost c","COST ¢"], "Cost ¢")
    inv_upc_col      = _resolve_col(master, ["Invoice UPC","InvoiceUPC","INV UPC","Invoice upc"], "Invoice UPC")

    master[pack_col]        = master[pack_col].apply(_to_int_safe)
    master[cases_col]       = master[cases_col].apply(_to_int_safe)
    master[total_col]       = master[total_col].apply(_to_float_safe)
    master[cost_dollar_col] = master[cost_dollar_col].apply(_to_float_safe)
    master[cost_cent_col]   = master[cost_cent_col].apply(_to_int_safe)

    invoice_unique = (
        invoice_df
        .groupby("UPC", as_index=False)
        .agg({"Item Name":"last","Cost":"last","Cases":"sum"})
    )

    inv_map = invoice_unique.set_index("UPC")[["Item Name","Cost","Cases"]].to_dict(orient="index")
    inv_upcs = set(invoice_unique["UPC"])

    changed_cost_rows = []
    not_in_master_rows = []
    pack_missing_on_added_rows = []

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

            if new_cases > 0 and pack_val == 0:
                pack_missing_on_added_rows.append({
                    inv_upc_col: inv_upc,
                    name_col: row.get(name_col, ""),
                    "Cases": new_cases,
                    "Pack": pack_val
                })

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

def _update_master_from_invoice_bt(master_xlsx, invoice_df: pd.DataFrame):
    """
    Breakthru variant:
    - Normalizes both UPC and Item Number from the invoice.
    - Groups by (UPC, Item Number), preserving first-appearance order.
    - Matches against Master to update costs/cases.
    - Returns an extra value: 'ordered_fbs', a list of matched Master Full Barcodes in invoice order.
    """
    if invoice_df is None or invoice_df.empty:
        return (None, None, None, None, None, [])

    df = invoice_df.copy()

    # ---- Pick invoice columns (case-insensitive helpers) ----
    cols = list(df.columns)

    def _pick_col(candidates, default):
        low = [str(c).lower() for c in cols]
        for cand in candidates:
            if cand.lower() in low:
                return cols[low.index(cand.lower())]
        return default if default in df.columns else None

    upc_col      = _pick_col(["UPC"], "UPC")
    name_col_inv = _pick_col(["Item Name", "Description", "Item Description"], "Item Name")
    cost_col     = _pick_col(["Cost"], "Cost")
    cases_col    = _pick_col(["Cases", "Qty", "Quantity"], "Cases")
    itemnum_col  = _pick_col(["Item Number", "ItemNumber", "Item No", "Item #", "Item"], "Item Number")

    if not upc_col or not name_col_inv or not cost_col or not cases_col:
        return (None, None, None, None, None, [])

    # ---- Normalize invoice numeric fields ----
    df[upc_col] = df[upc_col].astype(str).fillna("")
    if itemnum_col and itemnum_col in df.columns:
        df[itemnum_col] = df[itemnum_col].astype(str).fillna("")
    else:
        itemnum_col = None

    df[cost_col] = pd.to_numeric(df[cost_col], errors="coerce")
    df[cases_col] = pd.to_numeric(df[cases_col], errors="coerce").fillna(0).astype(int)

    # ---- Prepare Keys for Lookup ----
    df["__inv_upc_norm"] = df[upc_col].astype(str).map(
        lambda x: _norm_upc_12(x) if str(x).strip() else ""
    )
    if itemnum_col:
        df["__itemnorm"] = df[itemnum_col].astype(str).map(
            lambda x: _norm_upc_12(x) if str(x).strip() else ""
        )
    else:
        df["__itemnorm"] = ""

    df_valid = df[
        ((df["__inv_upc_norm"].ne("")) | (df["__itemnorm"].ne("")))
        & df[cost_col].notna()
        & df[cases_col].gt(0)
    ].copy()

    if df_valid.empty:
        return (None, None, None, None, None, [])

    # ---- Aggregate by BOTH keys, preserving Order ----
    # 1. Add an index to track original appearance
    df_valid["_sort_idx"] = range(len(df_valid))

    # 2. Group and take min(_sort_idx) to find first appearance
    invoice_unique = (
        df_valid
        .groupby(["__inv_upc_norm", "__itemnorm"], as_index=False)
        .agg({
            name_col_inv: "last",
            cost_col: "last",
            cases_col: "sum",
            "_sort_idx": "min" 
        })
        .sort_values("_sort_idx") # Restore invoice order
        .rename(columns={
            name_col_inv: "Item Name",
            cost_col: "Cost",
            cases_col: "Cases",
        })
    )

    inv_records = invoice_unique.to_dict(orient="records")

    # ---- Load and normalize Master ----
    master = pd.read_excel(master_xlsx, dtype=str).fillna("")

    name_col         = _resolve_col(master, ["Name","NAME","name"], "Name")
    pack_col         = _resolve_col(master, ["Pack","PACK","pack"], "Pack")
    cases_col_m      = _resolve_col(master, ["Cases","CASES","cases"], "Cases")
    total_col        = _resolve_col(master, ["Total","TOTAL","total"], "Total")
    cost_dollar_col  = _resolve_col(master, ["Cost $","Cost$","COST $","cost $"], "Cost $")
    cost_cent_col    = _resolve_col(master, ["Cost ¢","Cost cents","Cost c","COST ¢"], "Cost ¢")
    inv_upc_col      = _resolve_col(master, ["Invoice UPC","InvoiceUPC","INV UPC","Invoice upc"], "Invoice UPC")
    full_barcode_col = _resolve_col(master, ["Full Barcode","FullBarcode","FULL BARCODE"], "Full Barcode")

    master[pack_col]        = master[pack_col].apply(_to_int_safe)
    master[cases_col_m]     = master[cases_col_m].apply(_to_int_safe)
    master[total_col]       = master[total_col].apply(_to_float_safe)
    master[cost_dollar_col] = master[cost_dollar_col].apply(_to_float_safe)
    master[cost_cent_col]   = master[cost_cent_col].apply(_to_int_safe)

    master["__inv_norm"] = master[inv_upc_col].map(_norm_upc_12)
    master["__fb_norm"]  = master[full_barcode_col].map(_norm_upc_12)

    by_inv = {}
    by_fb  = {}
    for i, r in master.iterrows():
        if r["__inv_norm"]:
            by_inv.setdefault(r["__inv_norm"], []).append(i)
        if r["__fb_norm"]:
            by_fb.setdefault(r["__fb_norm"], []).append(i)

    changed_cost_rows = []
    not_in_master_rows = []
    pack_missing_on_added_rows = []
    ordered_fbs = [] # To store matched Full Barcodes in invoice order

    updated = master.copy()

    for rec in inv_records:
        u_upc   = rec["__inv_upc_norm"]
        u_item  = rec["__itemnorm"]
        new_cost  = float(rec["Cost"])
        new_cases = int(rec["Cases"])
        item_name = rec["Item Name"]

        matched_indices = []

        # PRIORITY 1: Match Invoice UPC -> Master Invoice UPC
        if u_upc and u_upc in by_inv:
            matched_indices = by_inv[u_upc]
        
        # PRIORITY 2: Match Invoice Item Number -> Master Invoice UPC
        if not matched_indices and u_item and u_item in by_inv:
            matched_indices = by_inv[u_item]
        
        # PRIORITY 3: Match Invoice UPC -> Master Full Barcode
        if not matched_indices and u_upc and u_upc in by_fb:
            matched_indices = by_fb[u_upc]

        if matched_indices:
            for idx in matched_indices:
                old_cost  = float(updated.at[idx, cost_dollar_col])
                pack_val  = int(updated.at[idx, pack_col])

                updated.at[idx, cases_col_m]     = new_cases
                updated.at[idx, total_col]       = float(pack_val * new_cases)
                updated.at[idx, cost_dollar_col] = new_cost
                updated.at[idx, cost_cent_col]   = int(round(new_cost * 100))
                
                # Capture the Full Barcode for sorting POS update later
                fb_val = str(updated.at[idx, full_barcode_col])
                ordered_fbs.append(_norm_upc_12(fb_val))

                if abs(old_cost - new_cost) > 1e-6:
                    changed_cost_rows.append({
                        inv_upc_col: updated.at[idx, inv_upc_col],
                        name_col:    updated.at[idx, name_col],
                        "Old Cost $": old_cost,
                        "New Cost $": new_cost,
                    })
                if new_cases > 0 and pack_val == 0:
                    pack_missing_on_added_rows.append({
                        inv_upc_col: updated.at[idx, inv_upc_col],
                        name_col:    updated.at[idx, name_col],
                        "Cases":     new_cases,
                        "Pack":      pack_val,
                    })
        else:
            display_key = u_upc if u_upc else u_item
            not_in_master_rows.append({
                "Lookup UPC": display_key,
                "Item Name":  item_name,
                "Cost":       new_cost,
                "Cases":      new_cases,
            })

    return (
        updated.drop(columns=["__inv_norm", "__fb_norm"], errors="ignore"),
        pd.DataFrame(changed_cost_rows),
        pd.DataFrame(not_in_master_rows),
        pd.DataFrame(pack_missing_on_added_rows),
        invoice_unique,
        ordered_fbs 
    )
# ---------------- Unified functions ----------------
def parse_unified(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        df_raw = pd.read_csv(uploaded_file, header=None, dtype=str, keep_default_na=False)
    else:
        df_raw = pd.read_excel(uploaded_file, header=None, dtype=str)

    header_tokens = [
        "Item UPC","UPC","Brand","Description","Pack","Size","Cost",
        "Net Case Cost","Case Qty","Invoice Date","Qty"
    ]
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

    col_item_upc   = find_col(cols, ["Item UPC","UPC"])
    col_brand      = find_col(cols, ["Brand"])
    col_desc       = find_col(cols, ["Description","Item Description"])
    col_pack       = find_col(cols, ["Pack","Case Pack","Qty per case"])
    col_size       = find_col(cols, ["Size"])
    col_cost       = find_col(cols, ["Cost"])
    col_net_cost   = find_col(cols, ["Net Case Cost"])
    col_case_qty   = find_col(cols, ["Case Qty","Case Quantity","Cases","Qty"])
    col_inv_date   = find_col(cols, ["Invoice Date","Inv Date","Date"])

    inv_df = inv_df[inv_df[col_item_upc].astype(str).apply(lambda x: len(re.sub(r"\D","", str(x))) >= 8)]
    case_qty_num = pd.to_numeric(inv_df[col_case_qty].apply(first_int_from_text) if col_case_qty else np.nan, errors="coerce")
    inv_df = inv_df[case_qty_num.fillna(0) > 0]

    if col_inv_date:
        inv_df["_invoice_date_parsed"] = pd.to_datetime(inv_df[col_inv_date], errors="coerce")
    else:
        inv_df["_invoice_date_parsed"] = pd.NaT
    inv_df["_invoice_date"] = inv_df["_invoice_date_parsed"].dt.date

    inv_tidy = pd.DataFrame()
    inv_tidy["invoice_date"] = inv_df["_invoice_date"]
    inv_tidy["inv_upc_raw"]  = inv_df[col_item_upc].astype(str)
    inv_tidy["UPC"]          = inv_tidy["inv_upc_raw"].apply(normalize_invoice_upc)
    inv_tidy["Brand"]        = inv_df[col_brand].astype(str) if col_brand else ""
    inv_tidy["Description"]  = inv_df[col_desc].astype(str) if col_desc else ""
    inv_tidy["Pack"]         = inv_df[col_pack].apply(first_int_from_text) if col_pack else np.nan
    inv_tidy["Size"]         = inv_df[col_size].astype(str) if col_size else ""
    inv_tidy["Cost"]         = inv_df[col_cost].apply(to_float) if col_cost else np.nan
    inv_tidy["+Cost"]        = inv_df[col_net_cost].apply(to_float) if col_net_cost else inv_tidy["Cost"]
    inv_tidy["Case Qty"]     = case_qty_num.loc[inv_df.index].astype("Int64")

    inv_all = inv_tidy[~inv_tidy["UPC"].isin(UNIFIED_IGNORE_UPCS)].copy()
    inv_all = inv_all.sort_values(["UPC","invoice_date"]).drop_duplicates(subset=["UPC"], keep="last")
    return inv_all

def process_unified(pos_csv_file, unified_files):
    pos_df = pd.read_csv(pos_csv_file, dtype=str, keep_default_na=False, na_values=[])
    pos_upc_col = "Upc" if "Upc" in pos_df.columns else ("UPC" if "UPC" in pos_df.columns else pos_df.columns[0])
    pos_df["UPC_norm"] = pos_df[pos_upc_col].astype(str).apply(normalize_pos_upc)
    pos_df["cost_qty_num"]   = pd.to_numeric(pos_df.get("cost_qty", np.nan), errors="coerce")
    pos_df["cost_cents_num"] = pd.to_numeric(pos_df.get("cost_cents", np.nan), errors="coerce")
    cents_col = "cents" if "cents" in pos_df.columns else next((c for c in pos_df.columns if "cent" in c.lower() and c.lower()!="cost_cents"), None)

    frames = [parse_unified(f) for f in unified_files]
    inv_all = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["UPC"])
    if not inv_all.empty:
        inv_all = inv_all.sort_values(["UPC","invoice_date"]).drop_duplicates(subset=["UPC"], keep="last")

    merged = pos_df.merge(
        inv_all[["UPC","Pack","+Cost","invoice_date","Brand","Description","Size","Cost"]],
        left_on="UPC_norm", right_on="UPC", how="left"
    )
    matched = merged[~merged["UPC"].isna()].copy()

    matched["new_cost_qty"]   = pd.to_numeric(matched["Pack"], errors="coerce")
    matched.loc[matched["new_cost_qty"].isna() | (matched["new_cost_qty"]<=0), "new_cost_qty"] = 1
    matched["new_cost_cents"] = (pd.to_numeric(matched["+Cost"], errors="coerce") * 100).round().astype("Int64")

    original_pos_cols = [
        c for c in pos_df.columns
        if c not in ["UPC_norm","cost_qty_num","cost_cents_num","cost_qty","cost_cents"]
    ]
    out = matched.copy()
    for col in original_pos_cols:
        if col not in out.columns:
            out[col] = ""

    out["cost_qty"]   = matched["new_cost_qty"].astype(pd.Int64Dtype())
    out["cost_cents"] = matched["new_cost_cents"].astype(pd.Int64Dtype())

    full_export_df = out[original_pos_cols + ["cost_qty","cost_cents"]].copy()

    qty_changed   = (matched["new_cost_qty"].astype("float64") != matched["cost_qty_num"].astype("float64"))
    cents_changed = (matched["new_cost_cents"].astype("float64") != matched["cost_cents_num"].astype("float64"))
    changed = matched[qty_changed | cents_changed].copy()
    pos_update_df = full_export_df.loc[changed.index].copy()

    gs1 = matched.copy()
    gs1["+Cost"] = pd.to_numeric(gs1["+Cost"], errors="coerce")
    gs1["Cost"]  = pd.to_numeric(gs1["Cost"], errors="coerce")
    gs1["Pack"]  = pd.to_numeric(gs1["Pack"], errors="coerce")
    gs1.loc[gs1["Pack"].isna() | (gs1["Pack"]<=0), "Pack"] = 1
    gs1["Unit"]  = gs1["+Cost"] / gs1["Pack"]
    gs1["D40%"]  = gs1["Unit"] / 0.6
    gs1["40%"]   = (gs1["Cost"] / gs1["Pack"]) / 0.6

    def cents_to_dollars(v):
        try:
            return float(str(v))/100.0
        except:
            return np.nan
    gs1["$Now"] = gs1[cents_col].apply(cents_to_dollars) if cents_col else np.nan

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

    unmatched = inv_all[~inv_all["UPC"].isin(matched["UPC"])][
        ["UPC","Brand","Description","Pack","+Cost","Case Qty","invoice_date"]
    ].copy() if not inv_all.empty else pd.DataFrame()

    full_export_df = full_export_df.loc[:, ~full_export_df.columns.duplicated()].copy()
    pos_update_df  = pos_update_df.loc[:,  ~pos_update_df.columns.duplicated()].copy()

    return full_export_df, pos_update_df, gs1_out, unmatched

# ---------------- session state ----------------
for k in ["full_export_df", "pos_update_df", "gs1_df", "unmatched_df", "ts"]:
    if k not in st.session_state:
        st.session_state[k] = None

for k in [
    "sg_invoice_items_df", "sg_updated_master", "sg_cost_changes",
    "sg_not_in_master", "sg_pack_missing", "sg_pos_update", "sg_pb_missing", "sg_ts"
]:
    if k not in st.session_state:
        st.session_state[k] = None

for k in [
    "nv_invoice_items_df", "nv_updated_master", "nv_cost_changes",
    "nv_not_in_master", "nv_pack_missing", "nv_pos_update", "nv_pb_missing", "nv_ts"
]:
    if k not in st.session_state:
        st.session_state[k] = None

for k in [
    "bt_invoice_items_df", "bt_invoice_items_dl_df", "bt_updated_master", "bt_cost_changes",
    "bt_not_in_master", "bt_pack_missing", "bt_pos_update", "bt_pb_missing", "bt_ts"
]:
    if k not in st.session_state:
        st.session_state[k] = None

# New session state for Costco
for k in ["costco_parsed_df", "costco_changed_df", "costco_not_found_df", "costco_master_updated", "costco_ts"]:
    if k not in st.session_state:
        st.session_state[k] = None

# ---------------- UI ----------------

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

with st.sidebar:
    st.header("Navigation")
    selected_vendor = st.radio("Select Vendor Source", [
        "Unified (SVMERCH)", 
        "Southern Glazer's", 
        "Nevada Beverage", 
        "Breakthru", 
        "JC Sales",
        "Costco"  # <--- Added
    ])
    st.divider()

# ===== Unified tab =====
if selected_vendor == "Unified (SVMERCH)":
    st.title("Unified Processor")
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

    if st.session_state["full_export_df"] is not None:
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
            # Export Goal Sheet 1 as XLSX so UPC leading zeros are preserved
            gs1_bytes = dfs_to_xlsx_bytes({"GoalSheet1": st.session_state["gs1_df"]})
            st.download_button(
                "⬇️ Goal Sheet 1 — XLSX",
                data=gs1_bytes,
                file_name=f"Goal_Sheet_1_{ts}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_gs1_xlsx",
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

    if st.session_state["sg_invoice_items_df"] is not None:
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
        if st.session_state["sg_updated_master"] is not None:
            st.dataframe(st.session_state["sg_updated_master"].head(100), use_container_width=True)

            updated_master_bytes = dfs_to_xlsx_bytes({
                "UpdatedMaster": st.session_state["sg_updated_master"]
            })

            st.download_button(
                "⬇️ Updated Master (XLSX)",
                data=updated_master_bytes,
                file_name=f"sg_updated_master_{sg_ts}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="sg_dl_mst_xlsx"
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

    if st.session_state["nv_invoice_items_df"] is not None:
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
    inv_files = st.file_uploader(
        "Upload Breakthru invoice PDF(s) or Excel/CSV",
        type=["pdf", "xlsx", "xls", "csv"],
        accept_multiple_files=True,
        key="bt_inv",
    )
    master_xlsx = st.file_uploader(
        "Upload Master workbook (.xlsx)",
        type=["xlsx"],
        key="bt_master",
    )
    pricebook_csv = st.file_uploader(
        "Upload pricebook CSV (optional for POS update)",
        type=["csv"],
        key="bt_pb",
    )

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

            invoice_items_raw = (
                pd.concat(parts, ignore_index=True)
                if parts
                else pd.DataFrame(columns=["UPC", "Item Name", "Cost", "Cases", "Item Number"])
            )
            if invoice_items_raw.empty:
                st.error("Could not parse any Breakthru items. Check the file.")
            else:
                # Capture the ordered_fbs (Full Barcodes in invoice order)
                updated_master, cost_changes, not_in_master, pack_missing, invoice_unique, ordered_fbs = _update_master_from_invoice_bt(
                    master_xlsx,
                    invoice_items_raw,
                )

                inv_display = invoice_items_raw.copy()
                if "UPC" in inv_display.columns and "Item Number" in inv_display.columns:
                    upc_str = inv_display["UPC"].astype(str).str.strip()
                    item_str = inv_display["Item Number"].astype(str).str.strip()
                    mask_blank_upc = upc_str.eq("") & item_str.ne("")
                    inv_display.loc[mask_blank_upc, "UPC"] = inv_display.loc[mask_blank_upc, "Item Number"]

                pos_update = None
                pb_missing = None
                if pricebook_csv is not None and updated_master is not None:
                    pricebook_csv.seek(0)
                    pos_update, pb_missing = _build_pricebook_update(pricebook_csv, updated_master)
                    
                    # --- NEW: Re-sort POS Update to match Invoice Order ---
                    if pos_update is not None and not pos_update.empty and ordered_fbs:
                        # 1. Identify Pricebook UPC column
                        pu_col = "Upc" if "Upc" in pos_update.columns else ("UPC" if "UPC" in pos_update.columns else pos_update.columns[0])
                        
                        # 2. Create a rank map from the ordered barcodes (first appearance gets lower rank)
                        fb_rank = {}
                        for i, fb in enumerate(ordered_fbs):
                            if fb not in fb_rank:
                                fb_rank[fb] = i
                        
                        # 3. Apply rank and sort
                        pos_update["__norm"] = pos_update[pu_col].astype(str).map(_norm_upc_12)
                        pos_update["__rank"] = pos_update["__norm"].map(fb_rank)
                        
                        # Sort by rank, then drop helpers, then RESET INDEX so numbers are 0,1,2...
                        pos_update = (
                            pos_update.sort_values("__rank")
                            .drop(columns=["__norm", "__rank"])
                            .reset_index(drop=True)
                        )

                st.session_state["bt_invoice_items_df"] = inv_display
                st.session_state["bt_invoice_items_dl_df"] = invoice_items_raw
                st.session_state["bt_updated_master"]   = updated_master
                st.session_state["bt_cost_changes"]     = cost_changes
                st.session_state["bt_not_in_master"]    = not_in_master
                st.session_state["bt_pack_missing"]     = pack_missing
                st.session_state["bt_pos_update"]       = pos_update
                st.session_state["bt_pb_missing"]       = pb_missing
                st.session_state["bt_ts"]               = datetime.now().strftime("%Y%m%d_%H%M%S")

                st.success("Breakthru — processing complete.")

    if st.session_state["bt_invoice_items_df"] is not None:
        bt_ts = st.session_state["bt_ts"] or datetime.now().strftime("%Y%m%d_%H%M%S")

        st.subheader("Invoice Items (parsed, in-invoice order)")
        st.dataframe(st.session_state["bt_invoice_items_df"].head(100), use_container_width=True)
        st.download_button(
            "⬇️ Invoice Items (CSV)",
            data=df_to_csv_bytes(st.session_state["bt_invoice_items_df"]),
            file_name=f"bt_invoice_items_{bt_ts}.csv",
            key="bt_dl_items",
        )

        st.subheader("Updated Master (preview)")
        if st.session_state["bt_updated_master"] is not None:
            st.dataframe(st.session_state["bt_updated_master"].head(100), use_container_width=True)

            bt_updated_master_bytes = dfs_to_xlsx_bytes(
                {"UpdatedMaster": st.session_state["bt_updated_master"]}
            )

            st.download_button(
                "⬇️ Updated Master (XLSX)",
                data=bt_updated_master_bytes,
                file_name=f"bt_updated_master_{bt_ts}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="bt_dl_master_xlsx",
            )

        st.subheader("POS Update (preview - Invoice Order)")
        if st.session_state["bt_pos_update"] is not None and not st.session_state["bt_pos_update"].empty:
            st.dataframe(st.session_state["bt_pos_update"], use_container_width=True)
            st.download_button(
                "⬇️ POS Update (CSV)",
                data=df_to_csv_bytes(st.session_state["bt_pos_update"]),
                file_name=f"bt_pos_update_{bt_ts}.csv",
                key="bt_dl_pos",
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
                key="bt_dl_pb_missing",
            )
# ==== JC SALES ===============================================================
if selected_vendor == "JC Sales":
    st.title("JC Sales Processor (Text Paste)")

    inv_text = st.text_area(
        "Paste JC Sales Invoice Text (Select All in PDF -> Copy -> Paste)", 
        height=300, 
        key="jc_text",
        help="Paste the full text or just the item rows here."
    )
    
    colA, colB = st.columns(2)
    with colA:
        pricebook_csv = st.file_uploader("Upload POS pricebook (CSV)", type=["csv"], key="jc_pb")
    with colB:
        master_xlsx = st.file_uploader("Upload JC Sales Master (XLSX)", type=["xlsx"], key="jc_master")

    if st.button("Process JC Sales", type="primary", key="jc_go"):
        if not inv_text or not pricebook_csv or not master_xlsx:
            st.error("Please paste the invoice text and upload pricebook/master files.")
        else:
            parser = JCSalesParser()
            rows, _ = parser.parse(inv_text)
            
            if (rows is None) or (not isinstance(rows, pd.DataFrame)) or rows.empty:
                st.error("Could not parse any JC Sales lines from the text.")
            else:
                try:
                    master = pd.read_excel(master_xlsx, dtype=str).fillna("")
                except Exception as e:
                    st.error(f"Master read error: {e}")
                    master = pd.DataFrame()

                try:
                    pb = pd.read_csv(pricebook_csv, dtype=str, keep_default_na=False, na_values=[])
                except Exception as e:
                    st.error(f"Pricebook read error: {e}")
                    pb = pd.DataFrame()

                if master.empty or pb.empty:
                    st.error("Master or Pricebook is empty/unreadable.")
                else:
                    def pick(df, names, default):
                        for n in names:
                            if n in df.columns:
                                return n
                        if default not in df.columns:
                            df[default] = ""
                        return default

                    m_item = pick(master, ["ITEM","Item","item"], "ITEM")
                    m_upc1 = pick(master, ["UPC1","Upc1","upc1"], "UPC1")
                    m_upc2 = pick(master, ["UPC2","Upc2","upc2"], "UPC2")

                    pb_upc        = "Upc" if "Upc" in pb.columns else ("UPC" if "UPC" in pb.columns else pb.columns[0])
                    pb_cents      = "cents" if "cents" in pb.columns else next((c for c in pb.columns if c.lower()=="cents"), None)
                    pb_cost_qty   = "cost_qty" if "cost_qty" in pb.columns else next((c for c in pb.columns if c.lower()=="cost_qty"), None)
                    pb_cost_cents = "cost_cents" if "cost_cents" in pb.columns else next((c for c in pb.columns if c.lower()=="cost_cents"), None)

                    pb = pb.copy()
                    pb["__pb_upc_norm"] = pb[pb_upc].astype(str).map(_norm_upc_12)

                    m = master.copy()
                    m["__UPC1_norm"] = m[m_upc1].astype(str).map(_norm_upc_12)
                    m["__UPC2_norm"] = m[m_upc2].astype(str).map(_norm_upc_12)
                    item_to_upcs = dict(zip(m[m_item].astype(str), zip(m["__UPC1_norm"], m["__UPC2_norm"])))
                    pb_set = set(pb["__pb_upc_norm"])

                    parsed = rows.copy()
                    parsed["ITEM"] = parsed["ITEM"].astype(str)
                    parsed["PACK"] = pd.to_numeric(parsed["PACK"], errors="coerce").fillna(0).astype(int)
                    parsed["COST"] = pd.to_numeric(parsed["COST"], errors="coerce")
                    parsed["UNIT"] = pd.to_numeric(parsed["UNIT"], errors="coerce")

                    def resolve_upc(item):
                        u1, u2 = item_to_upcs.get(str(item), ("",""))
                        if u1 and u1 in pb_set:
                            return u1
                        if u2 and u2 in pb_set:
                            return u2
                        return f"No Match {item}"

                    parsed["UPC"] = parsed["ITEM"].map(resolve_upc)
                    parsed["RETAIL"] = parsed["UNIT"] * 2

                    pb_now_map = dict(zip(pb["__pb_upc_norm"], pd.to_numeric(pb.get(pb_cents, 0), errors="coerce").fillna(0) / 100.0))
                    parsed["NOW"] = parsed["UPC"].map(pb_now_map)
                    # Fix: Use startswith to catch "No Match 12345"
                    parsed.loc[parsed["UPC"].astype(str).str.startswith("No Match"), "NOW"] = np.nan

                    pb_cc = pd.to_numeric(pb.get(pb_cost_cents, 0), errors="coerce").fillna(0.0)
                    pb_cq = pd.to_numeric(pb.get(pb_cost_qty, 0), errors="coerce").fillna(0.0)
                    pb_unit_map = {}
                    for u, cc, cq in zip(pb["__pb_upc_norm"], pb_cc, pb_cq):
                        pb_unit_map[u] = (cc/100.0)/cq if cq and cq>0 else np.nan
                    parsed["DELTA"] = parsed["UNIT"] - parsed["UPC"].map(pb_unit_map)

                    parsed_out = parsed[["UPC","DESCRIPTION","PACK","COST","UNIT","RETAIL","NOW","DELTA"]].copy()

                    # --- NEW LOGIC: 1. "No Match" List (Filtered) ---
                    # Filter: ONLY items where the UPC is "No Match..."
                    mask_no_match = parsed["UPC"].astype(str).str.startswith("No Match")
                    jc_nomatch = parsed[mask_no_match].copy()

                    def format_item_with_star(val):
                        s_val = str(val).strip()
                        if s_val not in item_to_upcs: # Check against Master map keys
                            return f"{s_val}*"
                        return s_val

                    # Create "Item Number" column with * if missing from Master
                    jc_nomatch["Item Number"] = jc_nomatch["ITEM"].apply(format_item_with_star)
                    
                    # Columns requested: "Item Number" + [everything else in parsed table except UPC]
                    jc_nomatch_cols = ["Item Number", "DESCRIPTION", "PACK", "COST", "UNIT", "RETAIL", "NOW", "DELTA"]
                    jc_no_pos_match_df = jc_nomatch[jc_nomatch_cols].copy()

                    # --- NEW LOGIC: 2. "Not in Master" Table (Copy/Paste) ---
                    # Filter: Only items NOT in Master (referencing ITEM column)
                    mask_not_in_master = ~parsed["ITEM"].isin(item_to_upcs.keys())
                    jc_not_in_master = parsed[mask_not_in_master].copy()
                    
                    # Columns requested: ITEM, UPC1(blank), UPC2(blank), DESCRIPTION, PACK, COST
                    jc_not_in_master["UPC1"] = ""
                    jc_not_in_master["UPC2"] = ""
                    jc_not_in_master_final = jc_not_in_master[["ITEM", "UPC1", "UPC2", "DESCRIPTION", "PACK", "COST"]].copy()

                    matched = parsed_out[~parsed_out["UPC"].astype(str).str.startswith("No Match")].copy()
                    if not matched.empty:
                        matched = matched.rename(columns={"UPC": "__norm"})
                        pb2 = pb.copy()
                        pb2 = pb2.rename(columns={"__pb_upc_norm": "__norm"})
                        join = pb2.merge(matched[["__norm","PACK","COST"]], on="__norm", how="inner")
                        join["cost_qty"] = pd.to_numeric(join["PACK"], errors="coerce").fillna(0).astype(int)
                        join["cost_cents"] = (pd.to_numeric(join["COST"], errors="coerce").fillna(0.0) * 100).round().astype(int)
                        out_cols = list(pb.columns)
                        if "cost_qty" not in out_cols: out_cols.append("cost_qty")
                        if "cost_cents" not in out_cols: out_cols.append("cost_cents")
                        pos_update = join.reindex(columns=out_cols)
                    else:
                        pos_update = pd.DataFrame()

                    current_date = datetime.now().strftime("%Y-%m-%d")
                    parsed_xlsx_name = f"jcsales_parsed_{current_date}.xlsx"

                    st.session_state["jc_parsed_df"] = parsed_out
                    st.session_state["jc_pos_update_df"] = pos_update
                    # Save the new tables to session state
                    st.session_state["jc_no_pos_match_df"] = jc_no_pos_match_df
                    st.session_state["jc_not_in_master_df"] = jc_not_in_master_final
                    st.session_state["jc_parsed_name"] = parsed_xlsx_name
                    
                    st.success(f"Processing Complete! File: {parsed_xlsx_name}")
                    m1, m2, m3 = st.columns(3)
                    m1.metric("Rows Parsed", len(parsed_out))
                    m2.metric("POS Updates", len(pos_update))
                    m3.metric("Unmatched Items", len(parsed_out) - len(pos_update) if pos_update is not None else 0)

    if st.session_state.get("jc_parsed_df") is not None:
        parsed_out = st.session_state["jc_parsed_df"]
        pos_update = st.session_state.get("jc_pos_update_df")
        parsed_name = st.session_state.get("jc_parsed_name") or "jcsales_parsed.xlsx"

        # 1. Parsed Workbook (Standard)
        parsed_bytes = dfs_to_xlsx_bytes({"parsed": parsed_out})
        st.download_button(
            "⬇️ Download parsed workbook (XLSX)",
            data=parsed_bytes,
            file_name=parsed_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="jc_dl_parsed_xlsx",
        )

        # 2. POS Update
        if pos_update is not None and not pos_update.empty:
            st.download_button(
                "⬇️ Download POS_update (CSV)",
                data=df_to_csv_bytes(pos_update),
                file_name=f"POS_update_JCSales_{parsed_name.replace('jcsales_parsed_', '').replace('.xlsx', '')}.csv",
                mime="text/csv",
                key="jc_dl_pos_csv",
            )
        else:
            st.info("No POS updates generated (no matches).")

        # 3. No Match / Item # View (New Request 1)
        jc_nomatch_df = st.session_state.get("jc_no_pos_match_df")
        if jc_nomatch_df is not None and not jc_nomatch_df.empty:
            st.download_button(
                "⬇️ Download Item # View / No Match List (XLSX)",
                data=dfs_to_xlsx_bytes({"ItemView": jc_nomatch_df}),
                file_name=f"JCSales_ItemNumberList_{parsed_name.replace('jcsales_parsed_', '').replace('.xlsx', '')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="jc_dl_nomatch_xlsx",
            )
            
        # 4. Not In Master Copy/Paste (New Request 2)
        jc_missing_df = st.session_state.get("jc_not_in_master_df")
        if jc_missing_df is not None and not jc_missing_df.empty:
            st.download_button(
                "⬇️ Download Missing from Master (Copy-Paste) (XLSX)",
                data=dfs_to_xlsx_bytes({"NotInMaster": jc_missing_df}),
                file_name=f"JCSales_MissingFromMaster_{parsed_name.replace('jcsales_parsed_', '').replace('.xlsx', '')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="jc_dl_missing_xlsx",
            )

        with st.expander("Preview Parsed Data (First 100 Rows)", expanded=True):
            st.dataframe(parsed_out.head(100), use_container_width=True)

        if pos_update is not None and not pos_update.empty:
            with st.expander("Preview POS Updates", expanded=False):
                st.dataframe(pos_update.head(100), use_container_width=True)

        # --- ADDED PREVIEW HERE ---
        if jc_nomatch_df is not None and not jc_nomatch_df.empty:
            with st.expander("Preview Item # View / No Match List", expanded=False):
                st.dataframe(jc_nomatch_df.head(100), use_container_width=True)
                
        if jc_missing_df is not None and not jc_missing_df.empty:
            with st.expander("Preview Items Missing from Master", expanded=False):
                st.dataframe(jc_missing_df.head(100), use_container_width=True)
# ==== COSTCO =================================================================
if selected_vendor == "Costco":
    st.title("Costco Processor")
    st.markdown("""
    **Step 1:** Upload your Master List and paste the receipt text.
    **Step 2:** Click 'Parse Receipt'.
    **Step 3:** Enter quantities for items where the price didn't match the master list.
    **Step 4:** Click 'Calculate & Update'.
    """)

    costco_master_file = st.file_uploader("Upload Costco Master List (XLSX)", type=["xlsx"], key="costco_master")
    costco_receipt_text = st.text_area("Paste Costco Receipt Text", height=200, key="costco_text")

    # --- Step 1: Parse ---
    if st.button("Step 1: Parse Receipt", type="primary"):
        if not costco_master_file or not costco_receipt_text:
            st.error("Please upload the Master List and paste receipt text.")
        else:
            parser = CostcoParser()
            parsed_df = parser.parse(costco_receipt_text)
            
            if parsed_df.empty:
                st.error("No items found in receipt text. Please check format.")
            else:
                st.session_state["costco_parsed_df"] = parsed_df
                # Reset downstream states
                st.session_state["costco_changed_df"] = None
                st.session_state["costco_not_found_df"] = None
                st.session_state["costco_new_items_df"] = None
                st.session_state["costco_master_updated"] = None
                
                # Clear old quantity states
                keys_to_clear = [k for k in st.session_state.keys() if k.startswith("qty_")]
                for k in keys_to_clear:
                    del st.session_state[k]

                st.success(f"Found {len(parsed_df)} items from receipt.")

    # --- Step 2 & 3: Quantity Input & Process ---
    if st.session_state["costco_parsed_df"] is not None:
        
        if costco_master_file:
            try:
                # 1. Prepare Master Data
                costco_master_file.seek(0)
                master_df = pd.read_excel(costco_master_file, dtype=str)
                
                def pick_col(df, candidates, default):
                    for c in candidates:
                        if c in df.columns: return c
                    return default

                m_item_num = pick_col(master_df, ["Item Number", "Item #"], "Item Number")
                m_cost     = pick_col(master_df, ["Cost"], "Cost")
                
                # Clean master columns for lookup
                master_df["__item_str"] = master_df[m_item_num].astype(str).str.strip()
                master_df["__cost_float"] = pd.to_numeric(master_df[m_cost], errors="coerce").fillna(0.0)
                
                # Map Item -> Cost
                item_cost_map = dict(zip(master_df["__item_str"], master_df["__cost_float"]))
                valid_items = set(master_df["__item_str"])
                
                # 2. Prepare Parsed Data
                full_df = st.session_state["costco_parsed_df"].copy()
                full_df["Item Number"] = full_df["Item Number"].astype(str).str.strip()
                
                # Split Found vs Not Found
                is_found = full_df["Item Number"].isin(valid_items)
                found_df = full_df[is_found].copy()
                not_found_df = full_df[~is_found].copy()
                
                # 3. Intelligent Split: Auto-Calc vs Manual Input
                auto_processed = []
                manual_input_rows = []

                for _, row in found_df.iterrows():
                    item_num = str(row["Item Number"])
                    receipt_price = float(row["Receipt Price"])
                    
                    master_cost = item_cost_map.get(item_num, 0.0)
                    
                    # Logic: If divisible, auto-calc. 
                    # Use a small tolerance for floating point math
                    is_divisible = False
                    calc_qty = 1
                    
                    if master_cost > 0:
                        ratio = receipt_price / master_cost
                        rounded_ratio = round(ratio)
                        if abs(ratio - rounded_ratio) < 0.02: # 2% tolerance for float drift
                            is_divisible = True
                            calc_qty = int(rounded_ratio)
                            if calc_qty == 0: calc_qty = 1 # Safety
                    
                    if is_divisible:
                        auto_processed.append({
                            "Item Number": item_num,
                            "Item Name": row["Item Name"],
                            "Receipt Price": receipt_price,
                            "Quantity": calc_qty
                        })
                    else:
                        manual_input_rows.append(row)
                
                manual_df = pd.DataFrame(manual_input_rows) if manual_input_rows else pd.DataFrame()

            except Exception as e:
                st.error(f"Error reading Master file: {e}")
                st.stop()
                
            st.divider()
            st.subheader("Step 2: Enter Quantities")
            
            # --- INFO MESSAGE ---
            if auto_processed:
                st.info(f"✨ **{len(auto_processed)} items** matched the master cost perfectly. Their quantities were auto-calculated and hidden.")
                with st.expander("View Auto-Processed Items"):
                    st.dataframe(pd.DataFrame(auto_processed)[["Item Number", "Item Name", "Receipt Price", "Quantity"]])

            # --- RENDER ROW-BY-ROW INPUTS FOR MANUAL ITEMS ---
            input_data_manual = [] 

            if not manual_df.empty:
                st.caption("Please enter quantities for these items (Cost changed or new multiple):")
                # Header
                h1, h2, h3 = st.columns([3, 1.5, 1.5])
                h1.caption("**Item**")
                h2.caption("**Receipt Price**")
                h3.caption("**Qty Purchased**")

                for _, row in manual_df.iterrows():
                    item_num = str(row["Item Number"])
                    item_name = str(row["Item Name"])
                    price = float(row["Receipt Price"])
                    
                    qty_key = f"qty_{item_num}"
                    if qty_key not in st.session_state:
                        st.session_state[qty_key] = 1

                    c1, c2, c3 = st.columns([3, 1.5, 1.5])
                    with c1: st.text(f"{item_num}\n{item_name}")
                    with c2: st.text(f"${price:.2f}")
                    with c3:
                        st.number_input("Qty", min_value=1, step=1, key=qty_key, label_visibility="collapsed")

                    current_qty = st.session_state[qty_key]
                    input_data_manual.append({
                        "Item Number": item_num, 
                        "Item Name": item_name,
                        "Receipt Price": price,
                        "Quantity": current_qty
                    })
            elif not auto_processed:
                st.warning("No receipt items matched the Master List.")
            else:
                st.success("All found items were auto-matched! Click 'Calculate' to proceed.")
            
            st.divider()

            # --- Step 3: Calculate ---
            if st.button("Step 3: Calculate & Update", type="primary"):
                # Re-map master columns
                m_pack = pick_col(master_df, ["Pack"], "Pack")
                m_msrp = pick_col(master_df, ["MSRP"], "MSRP")
                m_upc  = pick_col(master_df, ["UPC"], "UPC")
                m_now  = pick_col(master_df, ["Now"], "Now")
                m_name = pick_col(master_df, ["Item Name", "Description", "Name"], "Item Name")

                # Conversions (m_cost already converted to __cost_float but we need to update the original DF)
                master_df[m_pack] = pd.to_numeric(master_df[m_pack], errors="coerce").fillna(1)
                master_df[m_cost] = pd.to_numeric(master_df[m_cost], errors="coerce").fillna(0.0)

                # Index Map
                item_map = {row["__item_str"]: idx for idx, row in master_df.iterrows()}
                
                changed_items = []
                updated_master = master_df.copy()
                
                # Combine Auto and Manual lists
                all_inputs = auto_processed + input_data_manual

                # 1. Process All Items
                for row_data in all_inputs:
                    r_item = row_data["Item Number"]
                    r_qty = float(row_data["Quantity"])
                    r_price = float(row_data["Receipt Price"])
                    
                    if r_qty <= 0: r_qty = 1.0
                    new_unit_cost = r_price / r_qty
                    
                    if r_item in item_map:
                        idx = item_map[r_item]
                        old_cost = float(updated_master.at[idx, m_cost])
                        pack_size = float(updated_master.at[idx, m_pack])
                        
                        updated_master.at[idx, m_cost] = new_unit_cost
                        
                        if pack_size > 0:
                            new_msrp = round((new_unit_cost / pack_size) / 0.6, 2)
                            updated_master.at[idx, m_msrp] = new_msrp
                            
                        # Tolerance check (approx 1 cent)
                        if abs(new_unit_cost - old_cost) > 0.009:
                            changed_items.append({
                                "UPC": updated_master.at[idx, m_upc],
                                "Item Name": updated_master.at[idx, m_name],
                                "Old Cost": old_cost,
                                "New Cost": new_unit_cost,
                                "Now": updated_master.at[idx, m_now],
                                "MSRP": updated_master.at[idx, m_msrp]
                            })
                
                # Drop temp columns
                updated_master.drop(columns=["__item_str", "__cost_float"], inplace=True, errors="ignore")

                # 2. Process Not Found Items (New Items Sheet)
                if not not_found_df.empty:
                    new_rows = []
                    for _, row in not_found_df.iterrows():
                        new_row = {}
                        new_row[m_item_num] = row["Item Number"]
                        new_row[m_name] = row["Item Name"]
                        new_row[m_cost] = row["Receipt Price"] 
                        new_rows.append(new_row)
                    
                    new_items_df = pd.DataFrame(new_rows)
                    new_items_df = new_items_df.reindex(columns=master_df.columns, fill_value="")
                    # Clean up temp cols from reindex if master had them (unlikely but safe)
                    new_items_df.drop(columns=["__item_str", "__cost_float"], inplace=True, errors="ignore")
                else:
                    new_items_df = pd.DataFrame()

                # Save to State
                st.session_state["costco_master_updated"] = updated_master
                st.session_state["costco_changed_df"] = pd.DataFrame(changed_items)
                st.session_state["costco_not_found_df"] = not_found_df
                st.session_state["costco_new_items_df"] = new_items_df
                st.session_state["costco_ts"] = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                st.success("Calculations Complete!")
        else:
             st.warning("Please upload the Master List to match items.")

    # --- Step 4: Results ---
    if st.session_state["costco_master_updated"] is not None:
        ts = st.session_state["costco_ts"]
        
        st.subheader("1. Updated Master File")
        master_bytes = dfs_to_xlsx_bytes({"Sheet1": st.session_state["costco_master_updated"]})
        st.download_button(
            "⬇️ Download Updated Master (XLSX)",
            data=master_bytes,
            file_name=f"Costco_Updated_Master_{ts}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_costco_master"
        )
        
        st.subheader("2. Changed Items")
        changed_df = st.session_state["costco_changed_df"]
        if not changed_df.empty:
            st.dataframe(
                changed_df.style.format({
                    "Old Cost": "${:.2f}",
                    "New Cost": "${:.2f}",
                    "MSRP": "${:.2f}"
                }), 
                use_container_width=True
            )
        else:
            st.info("No cost changes detected.")
            
        st.subheader("3. Items Not Found in Master")
        
        new_items_df = st.session_state.get("costco_new_items_df")
        if new_items_df is not None and not new_items_df.empty:
            new_items_bytes = dfs_to_xlsx_bytes({"NewItems": new_items_df})
            st.download_button(
                "⬇️ Download New Items to Add (XLSX)",
                data=new_items_bytes,
                file_name=f"Costco_New_Items_{ts}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_costco_new_items"
            )

        not_found_df = st.session_state["costco_not_found_df"]
        if not_found_df is not None and not not_found_df.empty:
            st.dataframe(not_found_df, use_container_width=True)
        else:
            st.success("All receipt items were found in the Master list!")
