# app.py
import streamlit as st
import pandas as pd
import numpy as np
import re
from io import BytesIO
from datetime import datetime

# ===== vendor parsers =====
from parsers import SouthernGlazersParser, NevadaBeverageParser

st.set_page_config(page_title="Unified — Multi-Vendor Invoice Processor", page_icon="🧾", layout="wide")

# ---------------- shared helpers ----------------
UNIFIED_IGNORE_UPCS = set(["000000000000", "003760010302", "023700052551"])

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

def _download_csv_with_upc_text(df: pd.DataFrame, filename: str, upc_col="UPC"):
    df2 = df.copy()
    if upc_col in df2.columns:
        # Force spreadsheet apps to treat UPC as text and keep leading zeros
        df2[upc_col] = df2[upc_col].astype(str).map(lambda x: f'="{x}"')
    st.download_button(f"⬇️ Download {filename}", df2.to_csv(index=False).encode("utf-8"), filename)

# -------- SG/NV shared helpers --------
def _ensure_invoice_cols(df: pd.DataFrame) -> pd.DataFrame:
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

    master[pack_col]        = master[pack_col].apply(_to_int_safe)
    master[cases_col]       = master[cases_col].apply(_to_int_safe)
    master[total_col]       = master[total_col].apply(_to_float_safe)
    master[cost_dollar_col] = master[cost_dollar_col].apply(_to_float_safe)
    master[cost_cent_col]   = master[cost_cent_col].apply(_to_int_safe)

    # Group invoice rows by UPC (sum cases, keep last cost/name), preserve latest by natural order
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

def _build_pricebook_update(pricebook_csv, updated_master_df):
    pb = pd.read_csv(pricebook_csv, dtype=str).fillna("")

    upc_col = None
    for cand in ["Upc","UPC","upc"]:
        if cand in pb.columns:
            upc_col = cand
            break
    if upc_col is None:
        upc_col = "Upc"
        pb[upc_col] = ""

    for c in ["addstock","cost_cents"]:
        if c not in pb.columns:
            pb[c] = ""

    master = updated_master_df.copy()
    full_barcode_col = _resolve_col(master, ["Full Barcode","FullBarcode","FULL BARCODE"], "Full Barcode")
    total_col        = _resolve_col(master, ["Total","TOTAL","total"], "Total")
    cost_cent_col    = _resolve_col(master, ["Cost ¢","Cost cents","Cost c","COST ¢"], "Cost ¢")

    master["__fb_norm"]   = master[full_barcode_col].map(_norm_upc_12)
    master["__TotalNum"]  = master[total_col].apply(_to_float_safe)
    master["__CostCents"] = master[cost_cent_col].apply(_to_int_safe)

    m = master[master["__TotalNum"] > 0].copy()
    if m.empty:
        return pb.iloc[0:0].copy(), pd.DataFrame([{"note":"No items with Total > 0 in Master after update."}])

    master_map = m.set_index("__fb_norm")[["__TotalNum","__CostCents"]].to_dict(orient="index")
    pb["__upc_norm"] = pb[upc_col].map(_norm_upc_12)

    keep_mask = pb["__upc_norm"].isin(m["__fb_norm"])
    pos_update = pb[keep_mask].copy()

    def _addstock_from_map(k):
        rec = master_map.get(k)
        return int(rec["__TotalNum"]) if rec is not None else 0
    def _cost_from_map(k):
        rec = master_map.get(k)
        return int(rec["__CostCents"]) if rec is not None else 0

    pos_update["addstock"]   = pos_update["__upc_norm"].map(_addstock_from_map)
    pos_update["cost_cents"] = pos_update["__upc_norm"].map(_cost_from_map)

    missing = None
    pos_norm_set = set(pos_update["__upc_norm"])
    master_norm_set = set(m["__fb_norm"])
    if pos_norm_set != master_norm_set:
        missing_norms = sorted(master_norm_set - pos_norm_set)
        if missing_norms:
            cols_to_show = ["__fb_norm", full_barcode_col]
            if "Name" in master.columns:
                cols_to_show.append("Name")
            missing = m[m["__fb_norm"].isin(missing_norms)][cols_to_show].drop_duplicates().rename(
                columns={"__fb_norm":"UPC (normalized)"}
            )

    return pos_update.drop(columns=["__upc_norm"]), missing

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

# ---------------- UI ----------------
tabs = st.tabs(["Unified (SVMERCH)", "Southern Glazer's", "Nevada Beverage"])

# Unified tab
with tabs[0]:
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
                "⬇️ FULL Export (all matched) — CSV",
                data=df_to_csv_bytes(st.session_state["full_export_df"]),
                file_name=f"POS_Full_AllItems_{ts}.csv",
                mime="text/csv",
                key="dl_full_csv",
            )
        with col3:
            st.download_button(
                "⬇️ Audit Workbook (xlsx)",
                data=dfs_to_xlsx_bytes({
                    "Changes Only": st.session_state["pos_update_df"],
                    "Goal Sheet 1": st.session_state["gs1_df"],
                    "Unmatched":    st.session_state["unmatched_df"],
                }),
                file_name=f"Unified_Audit_{ts}_with_GoalSheet1.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_audit_xlsx",
            )

        st.subheader("Preview — FULL Export (first 200)")
        fe = st.session_state["full_export_df"].loc[:, ~st.session_state["full_export_df"].columns.duplicated()].copy()
        st.dataframe(fe.head(200), use_container_width=True)

        st.subheader("Preview — Goal Sheet 1 (first 100)")
        gs = st.session_state["gs1_df"].loc[:, ~st.session_state["gs1_df"].columns.duplicated()].copy()
        st.dataframe(gs.head(100), use_container_width=True)

        st.subheader("Unmatched (first 200)")
        um = st.session_state["unmatched_df"].loc[:, ~st.session_state["unmatched_df"].columns.duplicated()].copy()
        st.dataframe(um.head(200), use_container_width=True)
    else:
        st.info("Upload a POS CSV and at least one Unified invoice file, then click **Process Unified**.")

# Southern Glazer's tab
with tabs[1]:
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
            "⬇️ Download Invoice Items (CSV)",
            data=inv_items_df.to_csv(index=False).encode("utf-8"),
            file_name=f"sg_invoice_items_{sg_ts}.csv",
            mime="text/csv",
            key="sg_dl_invoice_items",
        )

        if st.session_state["sg_updated_master"] is not None:
            st.subheader("Updated Master (preview)")
            st.dataframe(st.session_state["sg_updated_master"].head(100), use_container_width=True)
            bio_master = BytesIO()
            with pd.ExcelWriter(bio_master, engine="openpyxl") as w:
                st.session_state["sg_updated_master"].to_excel(w, index=False, sheet_name="Master")
            st.download_button(
                "⬇️ Download Updated Master (XLSX)",
                data=bio_master.getvalue(),
                file_name=f"Master_UPDATED_{sg_ts}.xlsx",
                key="sg_dl_master_xlsx"
            )

        if st.session_state["sg_cost_changes"] is not None and not st.session_state["sg_cost_changes"].empty:
            st.caption("Cost changes")
            st.dataframe(st.session_state["sg_cost_changes"], use_container_width=True)
            st.download_button(
                "⬇️ Cost Changes (CSV)",
                data=st.session_state["sg_cost_changes"].to_csv(index=False).encode("utf-8"),
                file_name=f"sg_cost_changes_{sg_ts}.csv",
                key="sg_dl_cost_changes"
            )

        if st.session_state["sg_not_in_master"] is not None and not st.session_state["sg_not_in_master"].empty:
            st.caption("Invoice UPCs not in Master")
            st.dataframe(st.session_state["sg_not_in_master"], use_container_width=True)
            st.download_button(
                "⬇️ Not in Master (CSV)",
                data=st.session_state["sg_not_in_master"].to_csv(index=False).encode("utf-8"),
                file_name=f"sg_not_in_master_{sg_ts}.csv",
                key="sg_dl_not_in_master"
            )

        if st.session_state["sg_pack_missing"] is not None and not st.session_state["sg_pack_missing"].empty:
            st.caption("Added items with Pack == 0")
            st.dataframe(st.session_state["sg_pack_missing"], use_container_width=True)
            st.download_button(
                "⬇️ Added with Pack 0 (CSV)",
                data=st.session_state["sg_pack_missing"].to_csv(index=False).encode("utf-8"),
                file_name=f"sg_added_pack_zero_{sg_ts}.csv",
                key="sg_dl_pack_zero"
            )

        if st.session_state["sg_pos_update"] is not None:
            st.subheader("POS Update (preview)")
            st.dataframe(st.session_state["sg_pos_update"].head(100), use_container_width=True)
            bio_pos = BytesIO()
            with pd.ExcelWriter(bio_pos, engine="openpyxl") as w:
                st.session_state["sg_pos_update"].to_excel(w, index=False, sheet_name="POS_Update")
            st.download_button(
                "⬇️ Download POS Update (XLSX)",
                data=bio_pos.getvalue(),
                file_name=f"POS_Update_{sg_ts}.xlsx",
                key="sg_dl_pos_update"
            )

        if st.session_state["sg_pb_missing"] is not None and not st.session_state["sg_pb_missing"].empty:
            st.warning("Some invoice items were not found in the pricebook.")
            st.dataframe(st.session_state["sg_pb_missing"], use_container_width=True)
            st.download_button(
                "⬇️ Pricebook Missing (CSV)",
                data=st.session_state["sg_pb_missing"].to_csv(index=False).encode("utf-8"),
                file_name=f"pricebook_missing_{sg_ts}.csv",
                key="sg_dl_pb_missing"
            )
    else:
        st.info("Upload SG invoice(s) and Master, then click **Process SG**. Downloads will persist afterward.")

# Nevada Beverage tab (PDF-only)
with tabs[2]:
    st.title("Nevada Beverage Processor")
    st.caption("Upload the original Nevada Beverage PDF invoice(s).")
    inv_files_nv = st.file_uploader(
        "Upload NV invoice PDF(s)",
        type=["pdf"],
        accept_multiple_files=True,
        key="nv_inv"
    )
    master_xlsx_nv = st.file_uploader("Upload Master workbook (.xlsx)", type=["xlsx"], key="nv_master")
    pricebook_csv_nv = st.file_uploader("Upload pricebook CSV (optional for POS update)", type=["csv"], key="nv_pb")

    if st.button("Process NV", type="primary"):
        if not inv_files_nv or not master_xlsx_nv:
            st.error("Please upload at least one NV PDF and the Master workbook.")
        else:
            nv_parser = NevadaBeverageParser()
            parts_nv = []
            for f in inv_files_nv:
                f.seek(0)
                df = nv_parser.parse(f)
                if df is not None and not df.empty:
                    parts_nv.append(df)
            invoice_items_nv = pd.concat(parts_nv, ignore_index=True) if parts_nv else pd.DataFrame(columns=["UPC","Item Name","Cost","Cases"])
            invoice_items_nv = _ensure_invoice_cols(invoice_items_nv)

            if invoice_items_nv.empty:
                st.error("Could not parse any NV items (no UPC/Item Name/Cost/Cases).")
            else:
                updated_master_nv, cost_changes_nv, not_in_master_nv, pack_missing_nv, invoice_unique_nv = _update_master_from_invoice(master_xlsx_nv, invoice_items_nv)
                pos_update_nv = None
                pb_missing_nv = None
                if pricebook_csv_nv is not None and updated_master_nv is not None:
                    pos_update_nv, pb_missing_nv = _build_pricebook_update(pricebook_csv_nv, updated_master_nv)

                st.session_state["nv_invoice_items_df"] = invoice_items_nv
                st.session_state["nv_updated_master"]   = updated_master_nv
                st.session_state["nv_cost_changes"]     = cost_changes_nv
                st.session_state["nv_not_in_master"]    = not_in_master_nv
                st.session_state["nv_pack_missing"]     = pack_missing_nv
                st.session_state["nv_pos_update"]       = pos_update_nv
                st.session_state["nv_pb_missing"]       = pb_missing_nv
                st.session_state["nv_ts"]               = datetime.now().strftime("%Y%m%d_%H%M%S")

                st.success("Nevada Beverage — processing complete.")

    if st.session_state["nv_invoice_items_df"] is not None:
        nv_ts = st.session_state["nv_ts"] or datetime.now().strftime("%Y%m%d_%H%M%S")

        st.subheader("Invoice Items (parsed, in-invoice order)")
        st.dataframe(st.session_state["nv_invoice_items_df"].head(100), use_container_width=True)

        inv_items_df = st.session_state["nv_invoice_items_df"].copy()
        if "UPC" in inv_items_df.columns:
            inv_items_df["UPC"] = inv_items_df["UPC"].astype(str).map(lambda x: f'="{x}"')
        st.download_button(
            "⬇️ Download Invoice Items (CSV)",
            data=inv_items_df.to_csv(index=False).encode("utf-8"),
            file_name=f"nv_invoice_items_{nv_ts}.csv",
            mime="text/csv",
            key="nv_dl_invoice_items",
        )

        if st.session_state["nv_updated_master"] is not None:
            st.subheader("Updated Master (preview)")
            st.dataframe(st.session_state["nv_updated_master"].head(100), use_container_width=True)
            bio_master_nv = BytesIO()
            with pd.ExcelWriter(bio_master_nv, engine="openpyxl") as w:
                st.session_state["nv_updated_master"].to_excel(w, index=False, sheet_name="Master")
            st.download_button(
                "⬇️ Download Updated Master (XLSX)",
                data=bio_master_nv.getvalue(),
                file_name=f"Master_UPDATED_NV_{nv_ts}.xlsx",
                key="nv_dl_master_xlsx"
            )

        if st.session_state["nv_cost_changes"] is not None and not st.session_state["nv_cost_changes"].empty:
            st.caption("Cost changes")
            st.dataframe(st.session_state["nv_cost_changes"], use_container_width=True)
            st.download_button(
                "⬇️ Cost Changes (CSV)",
                data=st.session_state["nv_cost_changes"].to_csv(index=False).encode("utf-8"),
                file_name=f"nv_cost_changes_{nv_ts}.csv",
                key="nv_dl_cost_changes"
            )

        if st.session_state["nv_not_in_master"] is not None and not st.session_state["nv_not_in_master"].empty:
            st.caption("Invoice UPCs not in Master")
            st.dataframe(st.session_state["nv_not_in_master"], use_container_width=True)
            st.download_button(
                "⬇️ Not in Master (CSV)",
                data=st.session_state["nv_not_in_master"].to_csv(index=False).encode("utf-8"),
                file_name=f"nv_not_in_master_{nv_ts}.csv",
                key="nv_dl_not_in_master"
            )

        if st.session_state["nv_pack_missing"] is not None and not st.session_state["nv_pack_missing"].empty:
            st.caption("Added items with Pack == 0")
            st.dataframe(st.session_state["nv_pack_missing"], use_container_width=True)
            st.download_button(
                "⬇️ Added with Pack 0 (CSV)",
                data=st.session_state["nv_pack_missing"].to_csv(index=False).encode("utf-8"),
                file_name=f"nv_added_pack_zero_{nv_ts}.csv",
                key="nv_dl_pack_zero"
            )

        if st.session_state["nv_pos_update"] is not None:
            st.subheader("POS Update (preview)")
            st.dataframe(st.session_state["nv_pos_update"].head(100), use_container_width=True)
            bio_pos_nv = BytesIO()
            with pd.ExcelWriter(bio_pos_nv, engine="openpyxl") as w:
                st.session_state["nv_pos_update"].to_excel(w, index=False, sheet_name="POS_Update")
            st.download_button(
                "⬇️ Download POS Update (XLSX)",
                data=bio_pos_nv.getvalue(),
                file_name=f"POS_Update_NV_{nv_ts}.xlsx",
                key="nv_dl_pos_update"
            )

        if st.session_state["nv_pb_missing"] is not None and not st.session_state["nv_pb_missing"].empty:
            st.warning("Some invoice items were not found in the pricebook.")
            st.dataframe(st.session_state["nv_pb_missing"], use_container_width=True)
            st.download_button(
                "⬇️ Pricebook Missing (CSV)",
                data=st.session_state["nv_pb_missing"].to_csv(index=False).encode("utf-8"),
                file_name=f"pricebook_missing_nv_{nv_ts}.csv",
                key="nv_dl_pb_missing"
            )
    else:
        st.info("Upload NV PDF and Master, then click **Process NV**. Downloads will persist afterward.")
