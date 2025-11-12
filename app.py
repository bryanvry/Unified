
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from io import BytesIO

from parsers import ALL_PARSERS
from parsers.utils import normalize_pos_upc, sanitize_columns, IGNORE_UPCS

st.set_page_config(page_title="Multi‑Vendor Invoice → POS Processor", page_icon="🧾", layout="wide")

for k in ["full_export_df", "pos_update_df", "gs1_df", "unmatched_df", "ts"]:
    if k not in st.session_state:
        st.session_state[k] = None

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")

def dfs_to_xlsx_bytes(dfs: dict) -> bytes:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        for name, d in dfs.items():
            d.to_excel(writer, sheet_name=name[:31], index=False)
    bio.seek(0)
    return bio.getvalue()

st.title("🧾 Multi‑Vendor Invoice → POS Processor")
st.caption("Upload a POS CSV and one or more invoice files (Unified/SVMERCH, Southern Glazer's, Nevada Beverage).")

with st.sidebar:
    st.markdown("### Settings")
    vendor_override = st.selectbox(
        "Vendor parser (optional override)",
        options=["Auto‑detect"] + [p.name for p in ALL_PARSERS],
        index=0
    )
    st.divider()
    st.markdown("**Rules:**")
    st.write("- Ignore Case Qty = 0 (arrivals only) where applicable")
    st.write("- UPC normalization to UPC‑A (compute check digit)")
    st.write("- Dedupe by latest invoice date per UPC")
    st.write("- Ignore list: 000000000000, 003760010302, 023700052551")

pos_file = st.file_uploader("Upload POS pricebook CSV", type=["csv"], accept_multiple_files=False, key="pos")
inv_files = st.file_uploader("Upload invoice file(s) (XLSX/XLS/CSV)", type=["xlsx","xls","csv"], accept_multiple_files=True, key="inv")

def autodetect_parser(file, content_head: str):
    best = None
    best_hits = -1
    for p in ALL_PARSERS:
        hits = sum(1 for t in getattr(p, "tokens", []) if t.lower() in content_head.lower())
        if hits > best_hits:
            best, best_hits = p, hits
    return best

def read_head_text(file, nrows=50):
    try:
        if file.name.lower().endswith(".csv"):
            df = pd.read_csv(file, header=None, dtype=str, nrows=nrows)
        else:
            df = pd.read_excel(file, header=None, dtype=str, nrows=nrows)
        return "\n".join(" ".join([str(x) for x in df.iloc[i].tolist()]) for i in range(len(df)))
    except Exception:
        return ""

def process(pos_csv_file, invoice_files, vendor_choice: str):
    pos_df = pd.read_csv(pos_csv_file, dtype=str, keep_default_na=False, na_values=[])
    pos_upc_col = "Upc" if "Upc" in pos_df.columns else ("UPC" if "UPC" in pos_df.columns else pos_df.columns[0])
    pos_df["UPC_norm"] = pos_df[pos_upc_col].astype(str).apply(normalize_pos_upc)
    pos_df["cost_qty_num"]   = pd.to_numeric(pos_df.get("cost_qty", np.nan), errors="coerce")
    pos_df["cost_cents_num"] = pd.to_numeric(pos_df.get("cost_cents", np.nan), errors="coerce")
    cents_col = "cents" if "cents" in pos_df.columns else next((c for c in pos_df.columns if "cent" in c.lower() and c.lower()!="cost_cents"), None)

    parsed_frames = []
    for f in invoice_files:
        if vendor_choice != "Auto‑detect":
            parser = next(p for p in ALL_PARSERS if p.name == vendor_choice)
        else:
            head_text = read_head_text(f)
            parser = autodetect_parser(f, head_text)
        f.seek(0)
        parsed = parser.parse(f)
        parsed_frames.append(parsed)

    inv_all = pd.concat(parsed_frames, ignore_index=True) if parsed_frames else pd.DataFrame(columns=["UPC"])
    inv_all = inv_all[~inv_all["UPC"].isin(IGNORE_UPCS)].copy()
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

    original_pos_cols = [c for c in pos_df.columns if c not in ["UPC_norm","cost_qty_num","cost_cents_num","cost_qty","cost_cents"]]
    out = matched.copy()
    for col in original_pos_cols:
        if col not in out.columns:
            out[col] = ""

    out["cost_qty"]   = matched["new_cost_qty"].astype(pd.Int64Dtype())
    out["cost_cents"] = matched["new_cost_cents"].astype(pd.Int64Dtype())
    full_export_df = sanitize_columns(out[original_pos_cols + ["cost_qty","cost_cents"]])

    qty_changed   = (matched["new_cost_qty"].astype("float64") != matched["cost_qty_num"].astype("float64"))
    cents_changed = (matched["new_cost_cents"].astype("float64") != matched["cost_cents_num"].astype("float64"))
    changed = matched[qty_changed | cents_changed].copy()
    pos_update_df = sanitize_columns(full_export_df.loc[changed.index].copy())

    gs1 = matched.copy()
    gs1["+Cost"] = pd.to_numeric(gs1["+Cost"], errors="coerce")
    gs1["Cost"]  = pd.to_numeric(gs1["Cost"], errors="coerce")
    gs1["Pack"]  = pd.to_numeric(gs1["Pack"], errors="coerce")
    gs1.loc[gs1["Pack"].isna() | (gs1["Pack"]<=0), "Pack"] = 1
    gs1["Unit"]  = gs1["+Cost"] / gs1["Pack"]
    gs1["D40%"]  = gs1["Unit"] / 0.6
    gs1["40%"]   = (gs1["Cost"] / gs1["Pack"]) / 0.6

    def cents_to_dollars(v):
        try: return float(str(v))/100.0
        except: return np.nan
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

    unmatched = inv_all[~inv_all["UPC"].isin(matched["UPC"])][["UPC","Brand","Description","Pack","+Cost","Case Qty","invoice_date"]].copy() if not inv_all.empty else pd.DataFrame()
    return full_export_df, pos_update_df, gs1_out, unmatched

process_clicked = st.button("Process", type="primary")
if process_clicked:
    if not pos_file or not inv_files:
        st.warning("Upload a POS CSV and at least one invoice file.")
    else:
        with st.spinner("Processing…"):
            full_export_df, pos_update_df, gs1_out, unmatched = process(pos_file, inv_files, vendor_override)
        st.session_state["full_export_df"] = full_export_df
        st.session_state["pos_update_df"]  = pos_update_df
        st.session_state["gs1_df"]         = gs1_out
        st.session_state["unmatched_df"]   = unmatched
        st.session_state["ts"]             = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.success(f"Done! FULL rows: {len(full_export_df)}  |  Only-changed: {len(pos_update_df)}  |  Unmatched: {len(unmatched)}")

if st.session_state["full_export_df"] is not None:
    ts = st.session_state["ts"]
    c1, c2, c3 = st.columns(3)
    with c1:
        st.download_button("⬇️ POS Update (only changed) — CSV",
            data=df_to_csv_bytes(st.session_state["pos_update_df"]),
            file_name=f"POS_Update_OnlyChanged_{ts}.csv", mime="text/csv", key="dl_changed_csv")
    with c2:
        st.download_button("⬇️ FULL Export (all matched) — CSV",
            data=df_to_csv_bytes(st.session_state["full_export_df"]),
            file_name=f"POS_Full_AllItems_{ts}.csv", mime="text/csv", key="dl_full_csv")
    with c3:
        st.download_button("⬇️ Audit Workbook (xlsx)",
            data=dfs_to_xlsx_bytes({
                "Changes Only": st.session_state["pos_update_df"],
                "Goal Sheet 1": st.session_state["gs1_df"],
                "Unmatched":    st.session_state["unmatched_df"],
            }),
            file_name=f"Unified_Audit_{ts}_with_GoalSheet1.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_audit_xlsx")

    st.subheader("Preview — FULL Export (first 200)")
    st.dataframe(sanitize_columns(st.session_state["full_export_df"]).head(200), use_container_width=True)
    st.subheader("Preview — Goal Sheet 1 (first 100)")
    st.dataframe(sanitize_columns(st.session_state["gs1_df"]).head(100), use_container_width=True)
    st.subheader("Unmatched (first 200)")
    st.dataframe(sanitize_columns(st.session_state["unmatched_df"]).head(200), use_container_width=True)
else:
    st.info("Upload a POS CSV and at least one invoice file, then click **Process**.")
