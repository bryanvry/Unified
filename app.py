# app.py — Unified → POS Processor (Streamlit)
import streamlit as st
import pandas as pd
import numpy as np
import re
from io import BytesIO
from datetime import datetime

st.set_page_config(page_title="Unified → POS Processor", page_icon="🧾", layout="wide")

# ---------------------- Constants / helpers ----------------------
IGNORE_UPCS = set(["000000000000", "003760010302", "023700052551"])

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

# one-time init for session state caches
for k in ["full_export_df", "pos_update_df", "gs1_df", "unmatched_df", "ts"]:
    if k not in st.session_state:
        st.session_state[k] = None

# ---------------------- Unified parsing ----------------------
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

    # Keep rows that look like items & arrived (Case Qty > 0)
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

    # ignore list and dedupe latest by UPC
    inv_all = inv_tidy[~inv_tidy["UPC"].isin(IGNORE_UPCS)].copy()
    inv_all = inv_all.sort_values(["UPC","invoice_date"]).drop_duplicates(subset=["UPC"], keep="last")
    return inv_all

# ---------------------- Main pipeline ----------------------
def process(pos_csv_file, unified_files):
    # POS
    pos_df = pd.read_csv(pos_csv_file, dtype=str, keep_default_na=False, na_values=[])
    pos_upc_col = "Upc" if "Upc" in pos_df.columns else ("UPC" if "UPC" in pos_df.columns else pos_df.columns[0])
    pos_df["UPC_norm"] = pos_df[pos_upc_col].astype(str).apply(normalize_pos_upc)
    pos_df["cost_qty_num"]   = pd.to_numeric(pos_df.get("cost_qty", np.nan), errors="coerce")
    pos_df["cost_cents_num"] = pd.to_numeric(pos_df.get("cost_cents", np.nan), errors="coerce")
    cents_col = "cents" if "cents" in pos_df.columns else next((c for c in pos_df.columns if "cent" in c.lower() and c.lower()!="cost_cents"), None)

    # Unified (allow multiple files)
    frames = [parse_unified(f) for f in unified_files]
    inv_all = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["UPC"])
    if not inv_all.empty:
        inv_all = inv_all.sort_values(["UPC","invoice_date"]).drop_duplicates(subset=["UPC"], keep="last")

    # Merge
    merged = pos_df.merge(
        inv_all[["UPC","Pack","+Cost","invoice_date","Brand","Description","Size","Cost"]],
        left_on="UPC_norm", right_on="UPC", how="left"
    )
    matched = merged[~merged["UPC"].isna()].copy()

    # New costs from invoice
    matched["new_cost_qty"]   = pd.to_numeric(matched["Pack"], errors="coerce")
    matched.loc[matched["new_cost_qty"].isna() | (matched["new_cost_qty"]<=0), "new_cost_qty"] = 1
    matched["new_cost_cents"] = (pd.to_numeric(matched["+Cost"], errors="coerce") * 100).round().astype("Int64")

    # Build FULL export using original POS columns but drop old cost fields to avoid collisions
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

    # Only-changed subset
    qty_changed   = (matched["new_cost_qty"].astype("float64") != matched["cost_qty_num"].astype("float64"))
    cents_changed = (matched["new_cost_cents"].astype("float64") != matched["cost_cents_num"].astype("float64"))
    changed = matched[qty_changed | cents_changed].copy()
    pos_update_df = full_export_df.loc[changed.index].copy()

    # Goal Sheet 1
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

    # Unmatched list
    unmatched = inv_all[~inv_all["UPC"].isin(matched["UPC"])][
        ["UPC","Brand","Description","Pack","+Cost","Case Qty","invoice_date"]
    ].copy() if not inv_all.empty else pd.DataFrame()

    # Final safety: ensure no duplicate-named columns in outputs
    full_export_df = full_export_df.loc[:, ~full_export_df.columns.duplicated()].copy()
    pos_update_df  = pos_update_df.loc[:,  ~pos_update_df.columns.duplicated()].copy()

    return full_export_df, pos_update_df, gs1_out, unmatched

# ---------------------- UI ----------------------
st.title("🧾 Unified → POS Processor")
st.caption("Upload Unified invoice(s) + POS CSV to get POS updates, full export, and an audit workbook with Goal Sheet 1.")

with st.sidebar:
    st.markdown("### Rules")
    st.write("- UPC: rightmost 11 digits + computed check digit (UPC-A).")
    st.write("- Ignore Case Qty = 0; keep latest invoice per UPC.")
    st.write("- Ignore list: 000000000000, 003760010302, 023700052551")

pos_file = st.file_uploader("Upload POS pricebook CSV", type=["csv"], accept_multiple_files=False, key="pos")
inv_files = st.file_uploader("Upload Unified invoice file(s) (XLSX/XLS/CSV)", type=["xlsx","xls","csv"], accept_multiple_files=True, key="inv")

# Process button (use session_state so downloads survive reruns)
process_clicked = st.button("Process", type="primary")

if process_clicked:
    if not pos_file or not inv_files:
        st.warning("Upload a POS CSV and at least one Unified invoice file.")
    else:
        with st.spinner("Processing…"):
            full_export_df, pos_update_df, gs1_out, unmatched = process(pos_file, inv_files)

        st.session_state["full_export_df"] = full_export_df
        st.session_state["pos_update_df"]  = pos_update_df
        st.session_state["gs1_df"]         = gs1_out
        st.session_state["unmatched_df"]   = unmatched
        st.session_state["ts"]             = datetime.now().strftime("%Y%m%d_%H%M%S")

        st.success(f"Done! FULL rows: {len(full_export_df)}  |  Only-changed: {len(pos_update_df)}  |  Unmatched: {len(unmatched)}")

# Always show downloads if results exist
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

    # Previews (sanitize duplicate-named cols before rendering)
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
    st.info("Upload a POS CSV and at least one Unified invoice file, then click **Process**.")
