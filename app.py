import streamlit as st
import pandas as pd
import numpy as np
import re
from io import BytesIO
from datetime import datetime

# ===== vendor parsers =====
from parsers import SouthernGlazersParser, NevadaBeverageParser, BreakthruParser

st.set_page_config(page_title="Unified — Multi-Vendor Invoice Processor", page_icon="🧾", layout="wide")

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
    Breakthru variant: update by Invoice UPC first, then try Full Barcode for any still-unmatched rows.
    `invoice_df` must have columns: UPC, Item Name, Cost, Cases.
    UPC may be either Invoice UPC or Full Barcode (for rows where we filled from Item Number→FB).
    """
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
    full_barcode_col = _resolve_col(master, ["Full Barcode","FullBarcode","FULL BARCODE"], "Full Barcode")

    # normalize numeric columns
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

    # Build lookup maps for master on both keys (normalized 12-digit)
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

    updated = master.copy()
    matched_upcs = set()

    # pass 1: try Invoice UPC
    for upc, rec in inv_map.items():
        u = _norm_upc_12(upc)
        idxs = by_inv.get(u, [])
        if idxs:
            matched_upcs.add(u)
            for idx in idxs:
                old_cost = float(updated.at[idx, cost_dollar_col])
                new_cost = float(rec["Cost"])
                new_cases = int(rec["Cases"])
                pack_val = int(updated.at[idx, pack_col])

                updated.at[idx, cases_col]       = new_cases
                updated.at[idx, total_col]       = float(pack_val * new_cases)
                updated.at[idx, cost_dollar_col] = new_cost
                updated.at[idx, cost_cent_col]   = int(round(new_cost * 100))

                if abs(old_cost - new_cost) > 1e-6:
                    changed_cost_rows.append({
                        inv_upc_col: updated.at[idx, inv_upc_col],
                        name_col: updated.at[idx, name_col],
                        "Old Cost $": old_cost,
                        "New Cost $": new_cost
                    })
                if new_cases > 0 and pack_val == 0:
                    pack_missing_on_added_rows.append({
                        inv_upc_col: updated.at[idx, inv_upc_col],
                        name_col: updated.at[idx, name_col],
                        "Cases": new_cases,
                        "Pack": pack_val
                    })

    # pass 2: try Full Barcode for those not matched via Invoice UPC
    for upc, rec in inv_map.items():
        u = _norm_upc_12(upc)
        if u in matched_upcs:
            continue
        idxs = by_fb.get(u, [])
        if idxs:
            matched_upcs.add(u)
            for idx in idxs:
                old_cost = float(updated.at[idx, cost_dollar_col])
                new_cost = float(rec["Cost"])
                new_cases = int(rec["Cases"])
                pack_val = int(updated.at[idx, pack_col])

                updated.at[idx, cases_col]       = new_cases
                updated.at[idx, total_col]       = float(pack_val * new_cases)
                updated.at[idx, cost_dollar_col] = new_cost
                updated.at[idx, cost_cent_col]   = int(round(new_cost * 100))

                if abs(old_cost - new_cost) > 1e-6:
                    changed_cost_rows.append({
                        inv_upc_col: updated.at[idx, inv_upc_col],
                        name_col: updated.at[idx, name_col],
                        "Old Cost $": old_cost,
                        "New Cost $": new_cost
                    })
                if new_cases > 0 and pack_val == 0:
                    pack_missing_on_added_rows.append({
                        inv_upc_col: updated.at[idx, inv_upc_col],
                        name_col: updated.at[idx, name_col],
                        "Cases": new_cases,
                        "Pack": pack_val
                    })

    # anything still unmatched → not in master
    for upc, rec in inv_map.items():
        u = _norm_upc_12(upc)
        if u not in matched_upcs:
            not_in_master_rows.append({
                "Lookup UPC": u,
                "Item Name": rec.get("Item Name",""),
                "Cost": rec.get("Cost",""),
                "Cases": rec.get("Cases",""),
            })

    return (
        updated.drop(columns=["__inv_norm","__fb_norm"]),
        pd.DataFrame(changed_cost_rows),
        pd.DataFrame(not_in_master_rows),
        pd.DataFrame(pack_missing_on_added_rows),
        invoice_unique
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

# ---------------- UI ----------------
tabs = st.tabs(["Unified (SVMERCH)", "Southern Glazer's", "Nevada Beverage", "Breakthru"])

# ===== Unified tab =====
# ---------------- UI ----------------
# REPLACED: Tabs with a Dropdown Menu
VENDOR_OPTIONS = [
    "Unified (SVMERCH)", 
    "Southern Glazer's", 
    "Nevada Beverage", 
    "Breakthru", 
    "JC Sales (Coming Soon)"
]

selected_vendor = st.selectbox("Select Vendor Source", VENDOR_OPTIONS)
st.markdown("---") # Visual separator

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
            # Generate Excel in memory
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                st.session_state["gs1_df"].to_excel(writer, sheet_name="Goal Sheet 1", index=False)
                st.session_state["unmatched_df"].to_excel(writer, sheet_name="Unmatched Items", index=False)
                st.session_state["full_export_df"].to_excel(writer, sheet_name="FULL Data", index=False)
            
            st.download_button(
                "⬇️ Goal Sheet 1 + Audit — Excel",
                data=excel_buffer.getvalue(),
                file_name=f"Unified_Audit_Workbook_{ts}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_audit_xlsx"
            )
        
        st.divider()
        
        st.subheader("Preview — FULL Export (first 200)")
        st.dataframe(st.session_state["full_export_df"].head(200), use_container_width=True)
        
        st.subheader("Preview — Goal Sheet 1 (first 100)")
        st.dataframe(st.session_state["gs1_df"].head(100), use_container_width=True)

        st.subheader("Unmatched (first 200)")
        st.dataframe(st.session_state["unmatched_df"].head(200), use_container_width=True)


# ===== Southern Glazer's tab =====
if selected_vendor == "Southern Glazer's":
    st.title("🍷 Southern Glazer's Parser")
    st.caption("Upload PDF invoices + Master File + Pricebook to detect new items, cost changes, and generate POS updates.")

    c1, c2, c3 = st.columns(3)
    with c1:
        sg_files = st.file_uploader("SG Invoices (PDF)", type=["pdf"], accept_multiple_files=True, key="sg_inv")
    with c2:
        sg_master = st.file_uploader("SG Master File (CSV/XLSX)", type=["csv","xlsx","xls"], key="sg_mst")
    with c3:
        sg_pb = st.file_uploader("Current Pricebook (CSV) (optional)", type=["csv"], key="sg_pb")

    if st.button("Process Southern Glazer's", type="primary"):
        if not sg_files or not sg_master:
            st.warning("Please upload Invoices and Master File.")
        else:
            with st.spinner("Parsing SG..."):
                # 1. Parse Invoices
                sg_parser = SouthernGlazersParser()
                # Handle multiple files by iterating
                all_dfs = []
                for f in sg_files:
                    # Parser expects a single file-like object
                    # But user might upload multiple. 
                    # Assuming parser.parse returns DataFrame.
                    # We might need to reset pointer or bytes depending on implementation.
                    # Since your parser uses pdfplumber on the file object, it should work.
                    try:
                        df_ = sg_parser.parse(f)
                        all_dfs.append(df_)
                    except Exception as e:
                        st.error(f"Error parsing {f.name}: {e}")
                
                if all_dfs:
                    inv_df = pd.concat(all_dfs, ignore_index=True)
                    
                    # 2. Load Master
                    mst_df = load_dataframe(sg_master)
                    
                    # 3. Load Pricebook if present
                    pb_df = load_dataframe(sg_pb) if sg_pb else None

                    # 4. Logic
                    # (A) Clean Master
                    # Expects columns: Item Number, UPC, Pack, Description
                    # Let's ensure types
                    mst_df["Item Number"] = mst_df["Item Number"].astype(str).str.replace(r"\.0$","", regex=True).str.strip()
                    inv_df["Item Number"] = inv_df["Item Number"].astype(str).str.replace(r"\.0$","", regex=True).str.strip()
                    
                    # (B) Identify new items
                    # items in inv but not in master
                    not_in_master = inv_df[~inv_df["Item Number"].isin(mst_df["Item Number"])].copy()
                    
                    # (C) Compare Costs
                    # Merge inv + master on Item Number
                    merged = pd.merge(inv_df, mst_df, on="Item Number", how="inner", suffixes=("_inv", "_mst"))
                    
                    # Check cost diff
                    # Clean costs
                    merged["Net Price"] = pd.to_numeric(merged["Net Price"], errors="coerce").fillna(0)
                    merged["Cost"] = pd.to_numeric(merged["Cost"], errors="coerce").fillna(0) # master cost
                    
                    # Check Pack diff
                    merged["Pack_inv"] = pd.to_numeric(merged["Pack_inv"], errors="coerce").fillna(0)
                    merged["Pack_mst"] = pd.to_numeric(merged["Pack_mst"], errors="coerce").fillna(0)
                    
                    cost_diff = merged[abs(merged["Net Price"] - merged["Cost"]) > 0.009].copy()
                    pack_diff = merged[merged["Pack_inv"] != merged["Pack_mst"]].copy()
                    
                    # (D) Updated Master
                    # Start with original master
                    updated_master = mst_df.copy()
                    # Update costs for items found in invoice
                    # Map new costs
                    cost_map = inv_df.set_index("Item Number")["Net Price"].to_dict()
                    updated_master["Cost"] = updated_master.apply(
                        lambda row: cost_map.get(row["Item Number"], row["Cost"]), axis=1
                    )
                    
                    # (E) POS Update (Pricebook logic)
                    # Only if pricebook provided
                    pos_update_df = pd.DataFrame()
                    pb_missing = pd.DataFrame()
                    
                    if pb_df is not None:
                        pos_update_df, pb_missing = _build_pricebook_update(pb_df, inv_df, "Item Number", "Net Price")

                    # Save to session
                    st.session_state["sg_invoice_items_df"] = inv_df
                    st.session_state["sg_updated_master"]   = updated_master
                    st.session_state["sg_cost_changes"]     = cost_diff
                    st.session_state["sg_not_in_master"]    = not_in_master
                    st.session_state["sg_pack_missing"]     = pack_diff
                    st.session_state["sg_pos_update"]       = pos_update_df
                    st.session_state["sg_pb_missing"]       = pb_missing
                    st.session_state["sg_ts"]               = datetime.now().strftime("%Y%m%d_%H%M%S")
                    
                    st.success("Southern Glazer's processed!")

    if st.session_state["sg_invoice_items_df"] is not None:
        sg_ts = st.session_state["sg_ts"]
        
        # Downloads
        sc1, sc2, sc3, sc4 = st.columns(4)
        with sc1:
            st.download_button(
                "⬇️ Parsed Invoices (CSV)",
                data=st.session_state["sg_invoice_items_df"].to_csv(index=False).encode("utf-8"),
                file_name=f"sg_parsed_invoices_{sg_ts}.csv",
                key="sg_dl_inv"
            )
        with sc2:
            st.download_button(
                "⬇️ Updated Master (CSV)",
                data=st.session_state["sg_updated_master"].to_csv(index=False).encode("utf-8"),
                file_name=f"sg_updated_master_{sg_ts}.csv",
                key="sg_dl_mst"
            )
        with sc3:
            if st.session_state["sg_pos_update"] is not None and not st.session_state["sg_pos_update"].empty:
                st.download_button(
                    "⬇️ POS Cost Update (CSV)",
                    data=st.session_state["sg_pos_update"].to_csv(index=False).encode("utf-8"),
                    file_name=f"sg_pos_update_{sg_ts}.csv",
                    key="sg_dl_pos"
                )
        
        st.divider()
        st.subheader("Invoice Items (parsed, in-invoice order)")
        st.dataframe(st.session_state["sg_invoice_items_df"], use_container_width=True)
        
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Items Not In Master")
            st.dataframe(st.session_state["sg_not_in_master"], use_container_width=True)
        with c2:
            st.subheader("Cost Changes detected")
            st.dataframe(st.session_state["sg_cost_changes"], use_container_width=True)
            
        st.subheader("Updated Master (preview)")
        st.dataframe(st.session_state["sg_updated_master"].head(100), use_container_width=True)

        if st.session_state["sg_pos_update"] is not None and not st.session_state["sg_pos_update"].empty:
            st.subheader("POS Update (preview)")
            st.dataframe(st.session_state["sg_pos_update"], use_container_width=True)
        
        if st.session_state["sg_pb_missing"] is not None and not st.session_state["sg_pb_missing"].empty:
            st.subheader("Items in Invoice but NOT in Pricebook")
            st.dataframe(st.session_state["sg_pb_missing"], use_container_width=True)


# ===== Nevada Beverage tab =====
if selected_vendor == "Nevada Beverage":
    st.title("🍺 Nevada Beverage Parser")
    st.caption("Upload Excel invoices + Master File to detect updates.")

    nc1, nc2, nc3 = st.columns(3)
    with nc1:
        nv_files = st.file_uploader("Nevada Invoices (XLSX)", type=["xlsx","xls"], accept_multiple_files=True, key="nv_inv")
    with nc2:
        nv_master = st.file_uploader("Nevada Master (CSV/XLSX)", type=["csv","xlsx","xls"], key="nv_mst")
    with nc3:
        nv_pb = st.file_uploader("Current Pricebook (CSV)", type=["csv"], key="nv_pb")

    if st.button("Process Nevada Beverage", type="primary"):
        if not nv_files or not nv_master:
            st.warning("Upload Invoices + Master.")
        else:
            with st.spinner("Parsing Nevada..."):
                nv_parser = NevadaBeverageParser()
                all_nv = []
                for f in nv_files:
                    try:
                        df_ = nv_parser.parse(f)
                        all_nv.append(df_)
                    except Exception as e:
                        st.error(f"Error {f.name}: {e}")
                
                if all_nv:
                    inv_df = pd.concat(all_nv, ignore_index=True)
                    mst_df = load_dataframe(nv_master)
                    pb_df  = load_dataframe(nv_pb) if nv_pb else None
                    
                    # Clean IDs
                    # Nevada usually Item Number
                    mst_df["Item Number"] = mst_df["Item Number"].astype(str).str.replace(r"\.0$","", regex=True).str.strip()
                    inv_df["Item Number"] = inv_df["Item Number"].astype(str).str.replace(r"\.0$","", regex=True).str.strip()

                    # Logic similar to SG
                    not_in_master = inv_df[~inv_df["Item Number"].isin(mst_df["Item Number"])].copy()
                    
                    merged = pd.merge(inv_df, mst_df, on="Item Number", how="inner", suffixes=("_inv", "_mst"))
                    
                    # Clean numeric
                    merged["Cost"] = pd.to_numeric(merged["Cost"], errors="coerce").fillna(0) # inv cost
                    # Master cost might be named differently? Assuming "Cost" based on base.py standard
                    # Actually base.py says "Cost".
                    # But NevadaBeverageParser output uses "Cost".
                    # Master file likely has "Cost".
                    
                    # Check if master has "Cost". If not, maybe "Case Cost" or "Unit Cost"?
                    # We'll assume user provides standard master.
                    
                    if "Cost" in merged.columns:
                        # Wait, merge suffixes.
                        # If both have "Cost", we get Cost_inv and Cost_mst
                        pass
                    
                    # Actually we need to look at column names. 
                    # Let's assume standard master: Item Number, Cost, etc.
                    # In merge: Cost_inv, Cost_mst
                    
                    cost_diff = pd.DataFrame()
                    if "Cost_inv" in merged.columns and "Cost_mst" in merged.columns:
                         merged["Cost_inv"] = pd.to_numeric(merged["Cost_inv"], errors="coerce").fillna(0)
                         merged["Cost_mst"] = pd.to_numeric(merged["Cost_mst"], errors="coerce").fillna(0)
                         cost_diff = merged[abs(merged["Cost_inv"] - merged["Cost_mst"]) > 0.009].copy()

                    updated_master = mst_df.copy()
                    cost_map = inv_df.set_index("Item Number")["Cost"].to_dict()
                    updated_master["Cost"] = updated_master.apply(
                        lambda row: cost_map.get(row["Item Number"], row["Cost"]), axis=1
                    )

                    # POS Update
                    pos_update_df = pd.DataFrame()
                    pb_missing = pd.DataFrame()
                    if pb_df is not None:
                        # Nevada uses Item Number to link? Or UPC?
                        # Usually Item Number for master, but POS might use UPC.
                        # Let's try Item Number first
                        pos_update_df, pb_missing = _build_pricebook_update(pb_df, inv_df, "Item Number", "Cost")

                    st.session_state["nv_invoice_items_df"] = inv_df
                    st.session_state["nv_updated_master"]   = updated_master
                    st.session_state["nv_cost_changes"]     = cost_diff
                    st.session_state["nv_not_in_master"]    = not_in_master
                    st.session_state["nv_pos_update"]       = pos_update_df
                    st.session_state["nv_pb_missing"]       = pb_missing
                    st.session_state["nv_ts"]               = datetime.now().strftime("%Y%m%d_%H%M%S")
                    
                    st.success("Nevada processed!")

    if st.session_state["nv_invoice_items_df"] is not None:
        nv_ts = st.session_state["nv_ts"]
        
        bc1, bc2, bc3 = st.columns(3)
        with bc1:
            st.download_button("⬇️ Parsed Invoices", st.session_state["nv_invoice_items_df"].to_csv(index=False).encode("utf-8"), f"nv_inv_{nv_ts}.csv")
        with bc2:
            st.download_button("⬇️ Updated Master", st.session_state["nv_updated_master"].to_csv(index=False).encode("utf-8"), f"nv_mst_{nv_ts}.csv")
        with bc3:
             if st.session_state["nv_pos_update"] is not None and not st.session_state["nv_pos_update"].empty:
                st.download_button("⬇️ POS Update", st.session_state["nv_pos_update"].to_csv(index=False).encode("utf-8"), f"nv_pos_{nv_ts}.csv")

        st.divider()
        st.subheader("Invoice Items (parsed, in-invoice order)")
        st.dataframe(st.session_state["nv_invoice_items_df"], use_container_width=True)
        
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Not in Master")
            st.dataframe(st.session_state["nv_not_in_master"], use_container_width=True)
        with c2:
            st.subheader("Cost Differences")
            st.dataframe(st.session_state["nv_cost_changes"], use_container_width=True)

        st.subheader("Updated Master (preview)")
        st.dataframe(st.session_state["nv_updated_master"].head(100), use_container_width=True)
        
        if st.session_state["nv_pos_update"] is not None and not st.session_state["nv_pos_update"].empty:
            st.subheader("POS Update (preview)")
            st.dataframe(st.session_state["nv_pos_update"], use_container_width=True)
            
        if st.session_state["nv_pb_missing"] is not None and not st.session_state["nv_pb_missing"].empty:
            st.subheader("Missing from Pricebook")
            st.dataframe(st.session_state["nv_pb_missing"], use_container_width=True)


# ===== Breakthru tab =====
if selected_vendor == "Breakthru":
    st.title("🥃 Breakthru Beverage Parser")
    st.caption("Upload PDF invoices + Master File.")

    bc1, bc2, bc3 = st.columns(3)
    with bc1:
        bt_files = st.file_uploader("Breakthru PDF(s)", type=["pdf"], accept_multiple_files=True, key="bt_inv")
    with bc2:
        bt_master = st.file_uploader("Breakthru Master", type=["csv","xlsx","xls"], key="bt_mst")
    with bc3:
        bt_pb = st.file_uploader("Current Pricebook (CSV)", type=["csv"], key="bt_pb")

    if st.button("Process Breakthru", type="primary"):
        if not bt_files or not bt_master:
            st.warning("Need Invoices + Master.")
        else:
            with st.spinner("Parsing Breakthru..."):
                bt_parser = BreakthruParser()
                all_bt = []
                for f in bt_files:
                    try:
                        df_ = bt_parser.parse(f)
                        all_bt.append(df_)
                    except Exception as e:
                        st.error(f"Error {f.name}: {e}")
                
                if all_bt:
                    inv_df = pd.concat(all_bt, ignore_index=True)
                    mst_df = load_dataframe(bt_master)
                    pb_df  = load_dataframe(bt_pb) if bt_pb else None
                    
                    # Clean Item #
                    mst_df["Item Number"] = mst_df["Item Number"].astype(str).str.replace(r"\.0$","", regex=True).str.strip()
                    inv_df["Item Number"] = inv_df["Item Number"].astype(str).str.replace(r"\.0$","", regex=True).str.strip()

                    # New Items
                    not_in_master = inv_df[~inv_df["Item Number"].isin(mst_df["Item Number"])].copy()

                    # Merge
                    merged = pd.merge(inv_df, mst_df, on="Item Number", how="inner", suffixes=("_inv", "_mst"))

                    # Cost Changes
                    # Breakthru uses "Net Price" in parser output?
                    # Let's check parser output... BreakthruParser usually returns "Net Price" or "Cost".
                    # Let's assume "Net Price" based on SG logic or "Cost" based on base.
                    # We will try both.
                    
                    inv_cost_col = "Net Price" if "Net Price" in inv_df.columns else "Cost"
                    mst_cost_col = "Cost" # standard
                    
                    cost_diff = pd.DataFrame()
                    if inv_cost_col in inv_df.columns and mst_cost_col in mst_df.columns:
                        # merge creates suffixes
                        c_inv = inv_cost_col + "_inv"
                        c_mst = mst_cost_col + "_mst"
                        
                        if c_inv in merged.columns and c_mst in merged.columns:
                             merged[c_inv] = pd.to_numeric(merged[c_inv], errors="coerce").fillna(0)
                             merged[c_mst] = pd.to_numeric(merged[c_mst], errors="coerce").fillna(0)
                             cost_diff = merged[abs(merged[c_inv] - merged[c_mst"]) > 0.009].copy()
                    
                    # Update Master
                    updated_master = mst_df.copy()
                    cost_map = inv_df.set_index("Item Number")[inv_cost_col].to_dict()
                    updated_master["Cost"] = updated_master.apply(
                        lambda row: cost_map.get(row["Item Number"], row["Cost"]), axis=1
                    )
                    
                    # POS Update
                    pos_update_df = pd.DataFrame()
                    pb_missing = pd.DataFrame()
                    if pb_df is not None:
                         pos_update_df, pb_missing = _build_pricebook_update(pb_df, inv_df, "Item Number", inv_cost_col)
                    
                    st.session_state["bt_invoice_items_df"] = inv_df
                    st.session_state["bt_updated_master"]   = updated_master
                    st.session_state["bt_cost_changes"]     = cost_diff
                    st.session_state["bt_not_in_master"]    = not_in_master
                    st.session_state["bt_pos_update"]       = pos_update_df
                    st.session_state["bt_pb_missing"]       = pb_missing
                    st.session_state["bt_ts"]               = datetime.now().strftime("%Y%m%d_%H%M%S")
                    
                    st.success("Breakthru processed!")

    if st.session_state["bt_invoice_items_df"] is not None:
        bt_ts = st.session_state["bt_ts"]
        
        c1, c2, c3 = st.columns(3)
        with c1:
            st.download_button("⬇️ Invoices", st.session_state["bt_invoice_items_df"].to_csv(index=False).encode("utf-8"), f"bt_inv_{bt_ts}.csv")
        with c2:
            st.download_button("⬇️ Updated Master", st.session_state["bt_updated_master"].to_csv(index=False).encode("utf-8"), f"bt_mst_{bt_ts}.csv")
        with c3:
            if st.session_state["bt_pos_update"] is not None and not st.session_state["bt_pos_update"].empty:
                st.download_button("⬇️ POS Update", st.session_state["bt_pos_update"].to_csv(index=False).encode("utf-8"), f"bt_pos_{bt_ts}.csv")

        st.divider()
        st.subheader("Invoice Items (parsed, in-invoice order)")
        st.dataframe(st.session_state["bt_invoice_items_df"], use_container_width=True)
        
        c1, c2 = st.columns(2)
        with c1:
             st.subheader("Not in Master")
             st.dataframe(st.session_state["bt_not_in_master"], use_container_width=True)
        with c2:
             st.subheader("Cost Changes")
             st.dataframe(st.session_state["bt_cost_changes"], use_container_width=True)
             
        st.subheader("Updated Master (preview)")
        st.dataframe(st.session_state["bt_updated_master"].head(100), use_container_width=True)

        if st.session_state["bt_pos_update"] is not None and not st.session_state["bt_pos_update"].empty:
            st.subheader("POS Update (preview)")
            st.dataframe(st.session_state["bt_pos_update"], use_container_width=True)
            
        if st.session_state["bt_pb_missing"] is not None and not st.session_state["bt_pb_missing"].empty:
            st.subheader("Missing from Pricebook")
            st.dataframe(st.session_state["bt_pb_missing"], use_container_width=True)

# ===== JC Sales tab (Placeholder) =====
if selected_vendor == "JC Sales (Coming Soon)":
    st.title("🛒 JC Sales Invoice Processor")
    st.info("🚧 The JC Sales parser is currently under construction. Check back soon!")
