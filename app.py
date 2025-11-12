import io
import numpy as np
import pandas as pd
import streamlit as st
from parsers import ALL_PARSERS

st.set_page_config(page_title="Invoice → Master/POS Processor", layout="wide")

# =========================
# Helpers
# =========================

IGNORE_UPCS_UNIFIED = {"000000000000", "003760010302", "023700052551"}

def _digits(s: str) -> str:
    return "".join(ch for ch in str(s) if ch.isdigit())

def _norm12_or_blank(x: str) -> str:
    s = _digits(x)
    if not s:
        return ""
    if len(s) > 12:
        s = s[-12:]
    return s.zfill(12)

# ---- UPC-A check digit for Unified “rightmost 11 + check” rule ----
def _upca_check_digit(first11: str) -> str:
    d = [int(c) for c in first11]
    odd_sum  = d[0] + d[2] + d[4] + d[6] + d[8] + d[10]
    even_sum = d[1] + d[3] + d[5] + d[7] + d[9]
    total = odd_sum * 3 + even_sum
    cd = (10 - (total % 10)) % 10
    return str(cd)

def _unified_fix_upc(raw: str) -> str:
    """Rightmost 11 digits + computed check digit (UPC-A)."""
    d = _digits(raw)
    if len(d) < 11:
        return ""
    last11 = d[-11:]
    return last11 + _upca_check_digit(last11)

def _read_master(master_file) -> pd.DataFrame:
    x = pd.ExcelFile(master_file)
    df = pd.read_excel(x, sheet_name=0, dtype=str)

    # Ensure required columns exist
    for col in ["Full Barcode", "Invoice UPC", "Name", "Size", "Pack", "Cases", "Total", "Cost $", "Cost ¢", "Company"]:
        if col not in df.columns:
            df[col] = ""

    # Normalize IDs
    df["Full Barcode"] = df["Full Barcode"].apply(lambda v: _norm12_or_blank(v) if str(v).strip() else "")
    df["Invoice UPC"]  = df["Invoice UPC"].astype(str).str.strip()

    # Numerics
    df["Pack"] = pd.to_numeric(df["Pack"], errors="coerce").fillna(0).astype(int)
    df["Cases"] = pd.to_numeric(df["Cases"], errors="coerce").fillna(0).astype(int)
    df["Total"] = pd.to_numeric(df["Total"], errors="coerce").fillna(0).astype(int)
    df["Cost $"] = pd.to_numeric(df["Cost $"].astype(str).str.replace(r"[,$]", "", regex=True), errors="coerce").fillna(0.0)
    df["Cost ¢"] = pd.to_numeric(df["Cost ¢"], errors="coerce").fillna(0).astype(int)
    return df

def _read_pricebook(csv_file) -> pd.DataFrame:
    df = pd.read_csv(csv_file, dtype=str, keep_default_na=False)
    for col in ["Upc", "addstock", "cost_cents"]:
        if col not in df.columns:
            df[col] = ""
    df["Upc"]        = df["Upc"].apply(lambda v: _norm12_or_blank(v) if str(v).strip() else "")
    df["addstock"]   = pd.to_numeric(df["addstock"], errors="coerce").fillna(0).astype(int)
    df["cost_cents"] = pd.to_numeric(df["cost_cents"], errors="coerce").fillna(0).astype(int)
    return df

def _df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    if df is None or df.empty:
        buf = io.StringIO()
        pd.DataFrame().to_csv(buf, index=False)
        return buf.getvalue().encode("utf-8")
    df2 = df.copy()
    if "UPC" in df2.columns:
        df2["UPC"] = df2["UPC"].astype(str).apply(_norm12_or_blank)
    buf = io.StringIO()
    df2.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")

def _dfs_to_xlsx_bytes(sheets: dict[str, pd.DataFrame]) -> bytes:
    bio = io.BytesIO()
    try:
        with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
            for name, df in sheets.items():
                d = df.copy()
                if isinstance(d, pd.DataFrame) and "UPC" in d.columns:
                    d["UPC"] = d["UPC"].astype(str)
                d.to_excel(writer, sheet_name=name[:31], index=False)
    except Exception:
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            for name, df in sheets.items():
                d = df.copy()
                if isinstance(d, pd.DataFrame) and "UPC" in d.columns:
                    d["UPC"] = d["UPC"].astype(str)
                d.to_excel(writer, sheet_name=name[:31], index=False)
    return bio.getvalue()

# =========================
# SG/Nevada → Master/POS updater
# =========================

def _update_master_from_invoice(master_df: pd.DataFrame, invoice_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    SG / Nevada:
      JOIN on master["Invoice UPC"] == invoice["UPC"]
      Cases = invoice Cases
      Total = Pack × Cases
      Cost $ = invoice Cost ; Cost ¢ = round(Cost $ * 100)
      Reports:
        cost_changes, not_in_master (invoice rows without a match), missing_pack (matched rows with Pack<=0)
    """
    inv = invoice_df.copy()
    inv["UPC"] = inv["UPC"].astype(str).apply(_norm12_or_blank)

    prev_costs = master_df[["Invoice UPC", "Cost $"]].rename(columns={"Cost $": "_prev_cost"})

    j = master_df.merge(inv[["UPC", "Item Name", "Cost", "Cases"]],
                        left_on="Invoice UPC", right_on="UPC", how="left", suffixes=("", "_inv"))

    matched = j["UPC"].notna()

    j.loc[matched, "Cases"] = pd.to_numeric(j.loc[matched, "Cases_inv"], errors="coerce").fillna(0).astype(int)
    j["Pack"] = pd.to_numeric(j["Pack"], errors="coerce").fillna(0).astype(int)
    j["Cases"] = pd.to_numeric(j["Cases"], errors="coerce").fillna(0).astype(int)
    j.loc[matched, "Total"] = (j.loc[matched, "Pack"] * j.loc[matched, "Cases"]).astype(int)

    new_cost = pd.to_numeric(j["Cost"], errors="coerce")
    j.loc[matched & new_cost.notna(), "Cost $"] = new_cost[matched & new_cost.notna()]
    j["Cost $"] = pd.to_numeric(j["Cost $"], errors="coerce").fillna(0.0)
    j.loc[matched, "Cost ¢"] = (j.loc[matched, "Cost $"] * 100).round().astype(int)

    after_costs = j[["Invoice UPC", "Cost $"]].rename(columns={"Cost $": "_new_cost"})
    cost_changes = prev_costs.merge(after_costs, on="Invoice UPC", how="left")
    cost_changes = cost_changes[(cost_changes["_new_cost"].notna()) & (cost_changes["_prev_cost"] != cost_changes["_new_cost"])]

    not_in_master = inv[~inv["UPC"].isin(master_df["Invoice UPC"].astype(str))][["UPC", "Item Name", "Cost", "Cases"]].copy()
    missing_pack = j[matched & (j["Pack"] <= 0)][["Invoice UPC", "Pack", "Cases", "Total", "Cost $"]].copy()

    # Restore original master shape
    keep_cols = list(master_df.columns)
    for c in ["UPC", "Item Name", "Cost", "Cases_inv"]:
        if c in j.columns and c not in keep_cols:
            j.drop(columns=[c], inplace=True, errors="ignore")
    for c in keep_cols:
        if c not in j.columns:
            j[c] = master_df[c]
    j = j[keep_cols]

    return j, cost_changes, not_in_master, missing_pack

def _build_pricebook_update(pricebook_df: pd.DataFrame, updated_master: pd.DataFrame, invoice_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    inv_upcs = invoice_df["UPC"].dropna().astype(str).apply(_norm12_or_blank)
    inv_upcs = inv_upcs[inv_upcs != ""].unique().tolist()

    pb = pricebook_df.copy()
    master = updated_master[["Full Barcode", "Total", "Cost ¢"]].copy()

    merged = pb.merge(master, left_on="Upc", right_on="Full Barcode", how="left")
    filtered = merged[merged["Upc"].isin(inv_upcs)].copy()

    filtered["addstock"]   = pd.to_numeric(filtered["Total"], errors="coerce").fillna(0).astype(int)
    filtered["cost_cents"] = pd.to_numeric(filtered["Cost ¢"], errors="coerce").fillna(0).astype(int)

    out_cols = pb.columns.tolist()
    out = filtered[out_cols].copy()

    found_upcs = set(filtered["Upc"].astype(str))
    missing = sorted(set(inv_upcs) - found_upcs)
    missing_df = pd.DataFrame({"UPC not in Pricebook": missing})

    return out, missing_df

# =========================
# Unified (SVMERCH) flow — pre-Breakthru behavior
# =========================

def _process_unified(pos_file, inv_files):
    """
    Recreate the pre-Breakthru Unified behavior:
      - Parse all Unified invoices with UnifiedParser (which already handles:
        * Case Qty = 0 ignored
        * Latest invoice date per UPC
        * Net Case Cost (not Extended)
        * The weird UPC: fix via rightmost 11 + computed check digit
        * Ignore list stated above)
      - Build Goal Sheet 1 and Goal Sheet 2 as previously specified.
    """
    # POS
    pos = pd.read_csv(pos_file, dtype=str, keep_default_na=False)
    # Ensure cents & cost_qty are numeric (for $Now, Delta)
    pos["cents"] = pd.to_numeric(pos.get("cents", 0), errors="coerce").fillna(0).astype(int)
    pos["cost_qty"] = pd.to_numeric(pos.get("cost_qty", 0), errors="coerce").fillna(0).astype(int)
    pos["Upc"] = pos["Upc"].apply(_norm12_or_blank)

    # Invoices → concatenated
    from parsers.unified_parser import UnifiedParser
    up = UnifiedParser()

    chunks = []
    for f in inv_files:
        df = up.parse(f)  # expected to return the cleaned invoice rows with: Item UPC, Brand, Description, Pack, Size, Net Case Cost, Unit, D40%, 40%, invoice_date, CaseQty etc.
        if df is not None and not df.empty:
            chunks.append(df.copy())

    if not chunks:
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

    inv = pd.concat(chunks, ignore_index=True)

    # Normalize invoice UPC to 12-digit via Unified rule (rightmost 11 + check)
    inv["Item UPC"] = inv["Item UPC"].astype(str).apply(_unified_fix_upc)

    # Drop ignore list & Case Qty == 0
    inv = inv[~inv["Item UPC"].isin(IGNORE_UPCS_UNIFIED)].copy()
    inv["CaseQty"] = pd.to_numeric(inv.get("CaseQty", 0), errors="coerce").fillna(0).astype(int)
    inv = inv[inv["CaseQty"] > 0]

    # Keep latest invoice per UPC by invoice_date
    if "invoice_date" in inv.columns:
        inv["invoice_date"] = pd.to_datetime(inv["invoice_date"], errors="coerce")
        inv.sort_values(["Item UPC", "invoice_date"], ascending=[True, False], inplace=True)
        inv = inv.drop_duplicates(subset=["Item UPC"], keep="first")

    # Build Goal Sheet 1
    # Columns: POS.UPC + Brand, Description, Pack, Size, Cost(+Cost = Net Case Cost), Unit, D40%, 40%, $Now
    gs1 = inv.rename(columns={"Net Case Cost": "+Cost"})
    keep = ["Item UPC", "Brand", "Description", "Pack", "Size", "+Cost"]
    for c in keep:
        if c not in gs1.columns:
            gs1[c] = "" if c in ("Brand", "Description", "Size") else 0

    # Compute Unit, D40%, 40%
    gs1["Pack"] = pd.to_numeric(gs1["Pack"], errors="coerce").fillna(0)
    gs1["+Cost"] = pd.to_numeric(gs1["+Cost"], errors="coerce").fillna(0.0)
    gs1["Unit"] = gs1["+Cost"] / gs1["Pack"].replace(0, np.nan)
    gs1["D40%"] = gs1["Unit"] / 0.6
    gs1["40%"] = (gs1["+Cost"] / gs1["Pack"].replace(0, np.nan)) / 0.6

    gs1 = gs1[["Item UPC", "Brand", "Description", "Pack", "Size", "+Cost", "Unit", "D40%", "40%"]].copy()
    gs1.rename(columns={"Item UPC": "UPC"}, inplace=True)

    # Join $Now from POS (cents → dollars)
    pos_now = pos[["Upc", "cents", "cost_qty"]].rename(columns={"Upc": "UPC"})
    gs1 = gs1.merge(pos_now, on="UPC", how="left")
    gs1["$Now"] = (pd.to_numeric(gs1["cents"], errors="coerce").fillna(0) / 100.0)
    # Delta: D40% − (cost_cents/ cost_qty / 0.6)
    base = (pd.to_numeric(gs1["cents"], errors="coerce").fillna(0) / (pd.to_numeric(gs1["cost_qty"], errors="coerce").replace(0, np.nan))) / 0.6
    gs1["Delta"] = gs1["D40%"] - base
    gs1.loc[gs1["Delta"].fillna(0).abs() < 1e-9, "Delta"] = "="
    gs1.drop(columns=["cents", "cost_qty"], inplace=True, errors="ignore")

    # Goal Sheet 2 (POS upload subset)
    # same columns as POS, but only items found in Unified sheet; cost_qty = Pack; cost_cents = +Cost * 100
    pos2 = pos.merge(gs1[["UPC", "Pack", "+Cost"]], on="UPC", how="inner")
    pos2["cost_qty"] = pd.to_numeric(pos2["Pack"], errors="coerce").fillna(0).astype(int)
    pos2["cost_cents"] = (pd.to_numeric(pos2["+Cost"], errors="coerce").fillna(0.0) * 100).round().astype(int)
    pos2_out = pos2[pos.columns]  # retain original POS columns order

    # Unmatched UPCs from Unified invoice not found in POS
    unmatched = gs1[~gs1["UPC"].isin(pos["Upc"])][["UPC"]].drop_duplicates()

    return (gs1, pos2_out, unmatched, inv)
    # (Goal Sheet 1, Goal Sheet 2/POS upload, Unmatched UPCs, Raw unified inv used)

# =========================
# UI
# =========================

st.title("Invoice → Master/POS Processor")

vendor_label = st.selectbox("Choose vendor", list(ALL_PARSERS.keys()), index=0)
slug, ParserClass = ALL_PARSERS[vendor_label]
parser = ParserClass()

c1, c2, c3 = st.columns(3)
with c1:
    if slug == "unified":
        inv_files = st.file_uploader("Upload Unified invoice XLSX/XLS/CSV (one or many)", type=["xlsx","xls","csv"], accept_multiple_files=True)
    elif slug == "southern_glazers":
        inv_files = st.file_uploader("Upload Southern Glazer's PDF file(s)", type=["pdf"], accept_multiple_files=True)
    else:  # nevada
        inv_files = st.file_uploader("Upload Nevada Beverage PDF file(s)", type=["pdf"], accept_multiple_files=True)

with c2:
    master_xlsx = st.file_uploader("Upload MASTER (XLSX) — SG/Nevada only", type=["xlsx"])
with c3:
    pricebook_csv = st.file_uploader("Upload PRICEBOOK (CSV) — SG/Nevada only", type=["csv"])

process = st.button("Process")

# Persist between downloads
if "outputs" not in st.session_state:
    st.session_state["outputs"] = None
if "previews" not in st.session_state:
    st.session_state["previews"] = None

if process:
    if not inv_files:
        st.error("Upload at least one invoice file.")
    else:
        if slug == "unified":
            # Use the dedicated Unified flow
            if not pricebook_csv:
                st.error("Upload a POS/pricebook CSV for Unified.")
            else:
                gs1, gs2_posupload, unmatched, raw_used = _process_unified(pricebook_csv, inv_files)

                out1 = _df_to_csv_bytes(gs1)
                out2 = _dfs_to_xlsx_bytes({"POS Update": gs2_posupload})
                out3 = _df_to_csv_bytes(unmatched)

                st.session_state["outputs"] = {
                    "invoice_items_csv": None,
                    "updated_master_xlsx": None,
                    "cost_changes_csv": None,
                    "not_in_master_csv": out3,
                    "missing_pack_csv": None,
                    "pos_update_xlsx": out2,
                    "missing_in_pricebook_csv": None,
                    "gs1_csv": out1,
                }
                st.session_state["previews"] = {
                    "invoice_items": gs1.head(200),
                    "updated_master": pd.DataFrame(),
                    "pos_update": gs2_posupload.head(200),
                }
        else:
            # SG/Nevada generic path
            parts = []
            for f in inv_files:
                try:
                    df = parser.parse(f)
                except Exception:
                    df = pd.DataFrame(columns=["UPC","Item Name","Cost","Cases"])
                if df is not None and not df.empty:
                    df = df.copy()
                    df["UPC"] = df["UPC"].astype(str).apply(_norm12_or_blank)
                    parts.append(df)
            invoice_items = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["UPC","Item Name","Cost","Cases"])

            inv_csv = _df_to_csv_bytes(invoice_items)

            updated_master_xlsx = None
            cost_changes_csv = None
            not_in_master_csv = None
            missing_pack_csv = None
            pos_update_xlsx = None
            missing_in_pricebook_csv = None
            updated_master = None
            pos_update_df = None

            if master_xlsx is not None and pricebook_csv is not None and not invoice_items.empty:
                master_df = _read_master(master_xlsx)
                updated_master, cost_changes, not_in_master, missing_pack = _update_master_from_invoice(master_df, invoice_items)

                pricebook_df = _read_pricebook(pricebook_csv)
                pos_update_df, missing_pb = _build_pricebook_update(pricebook_df, updated_master, invoice_items)

                updated_master_xlsx = _dfs_to_xlsx_bytes({"Master": updated_master})
                cost_changes_csv = _df_to_csv_bytes(cost_changes)
                not_in_master_csv = _df_to_csv_bytes(not_in_master)
                missing_pack_csv = _df_to_csv_bytes(missing_pack)
                pos_update_xlsx = _dfs_to_xlsx_bytes({"POS Update": pos_update_df})
                missing_in_pricebook_csv = _df_to_csv_bytes(missing_pb)

            st.session_state["outputs"] = {
                "invoice_items_csv": inv_csv,
                "updated_master_xlsx": updated_master_xlsx,
                "cost_changes_csv": cost_changes_csv,
                "not_in_master_csv": not_in_master_csv,
                "missing_pack_csv": missing_pack_csv,
                "pos_update_xlsx": pos_update_xlsx,
                "missing_in_pricebook_csv": missing_in_pricebook_csv,
                "gs1_csv": None,
            }
            st.session_state["previews"] = {
                "invoice_items": invoice_items.head(200),
                "updated_master": (updated_master.head(100) if isinstance(updated_master, pd.DataFrame) and not updated_master.empty else pd.DataFrame()),
                "pos_update": (pos_update_df.head(200) if isinstance(pos_update_df, pd.DataFrame) and not pos_update_df.empty else pd.DataFrame()),
            }

# =========================
# Downloads + Previews
# =========================

outs = st.session_state["outputs"]
prev = st.session_state["previews"]

if outs is not None:
    st.subheader("Downloads")

    d1, d2, d3 = st.columns(3)
    with d1:
        if outs["gs1_csv"] is not None:
            st.download_button("⬇️ Goal_Sheet_1.csv", data=outs["gs1_csv"], file_name="Goal_Sheet_1.csv", mime="text/csv")
        if outs["invoice_items_csv"] is not None:
            st.download_button("⬇️ invoice_items.csv", data=outs["invoice_items_csv"], file_name="invoice_items.csv", mime="text/csv")
        if outs["updated_master_xlsx"] is not None:
            st.download_button("⬇️ Updated_Master.xlsx", data=outs["updated_master_xlsx"], file_name="Updated_Master.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    with d2:
        if outs["cost_changes_csv"] is not None:
            st.download_button("⬇️ Cost_Changes.csv", data=outs["cost_changes_csv"], file_name="Cost_Changes.csv", mime="text/csv")
        if outs["not_in_master_csv"] is not None:
            st.download_button("⬇️ Invoice_NotIn_Master.csv", data=outs["not_in_master_csv"], file_name="Invoice_NotIn_Master.csv", mime="text/csv")

    with d3:
        if outs["missing_pack_csv"] is not None:
            st.download_button("⬇️ Missing_Pack.csv", data=outs["missing_pack_csv"], file_name="Missing_Pack.csv", mime="text/csv")
        if outs["pos_update_xlsx"] is not None:
            st.download_button("⬇️ POS_Update.xlsx", data=outs["pos_update_xlsx"], file_name="POS_Update.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        if outs["missing_in_pricebook_csv"] is not None:
            st.download_button("⬇️ Pricebook_Missing.csv", data=outs["missing_in_pricebook_csv"], file_name="Pricebook_Missing.csv", mime="text/csv")

    # Previews (persistent)
    st.subheader("Previews")
    with st.expander("Invoice items / Goal Sheet (first 200)"):
        if prev and isinstance(prev.get("invoice_items"), pd.DataFrame) and not prev["invoice_items"].empty:
            st.dataframe(prev["invoice_items"], use_container_width=True)
        else:
            st.write("No preview available.")

    cA, cB = st.columns(2)
    with cA:
        st.caption("Updated Master (first 100)")
        if prev and isinstance(prev.get("updated_master"), pd.DataFrame) and not prev["updated_master"].empty:
            st.dataframe(prev["updated_master"], use_container_width=True)
        else:
            st.write("No preview available.")
    with cB:
        st.caption("POS Update (first 200)")
        if prev and isinstance(prev.get("pos_update"), pd.DataFrame) and not prev["pos_update"].empty:
            st.dataframe(prev["pos_update"], use_container_width=True)
        else:
            st.write("No preview available.")
