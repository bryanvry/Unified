# app.py
import io
import datetime as dt
import numpy as np
import pandas as pd
import streamlit as st

from parsers import ALL_PARSERS

st.set_page_config(page_title="Unified POS Tools", layout="wide")

# =========================
# Helpers
# =========================

def _norm12_or_blank(x: str) -> str:
    s = "".join(ch for ch in str(x) if ch.isdigit())
    if not s:
        return ""
    if len(s) > 12:
        s = s[-12:]
    return s.zfill(12)

def _read_master(master_file) -> pd.DataFrame:
    x = pd.ExcelFile(master_file)
    df = pd.read_excel(x, sheet_name=0, dtype=str)
    # Ensure columns exist
    for col in ["Full Barcode", "Invoice UPC", "Pack", "Cases", "Total", "Cost $", "Cost ¢"]:
        if col not in df.columns:
            df[col] = ""
    # Normalize barcodes as 12-digit strings (keep blanks empty)
    df["Full Barcode"] = df["Full Barcode"].apply(lambda v: _norm12_or_blank(v) if str(v).strip() else "")
    df["Invoice UPC"] = df["Invoice UPC"].astype(str).str.strip()
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
    # Normalize Upc to 12 digits
    df["Upc"] = df["Upc"].apply(lambda v: _norm12_or_blank(v) if str(v).strip() else "")
    # Numerics
    df["addstock"] = pd.to_numeric(df["addstock"], errors="coerce").fillna(0).astype(int)
    df["cost_cents"] = pd.to_numeric(df["cost_cents"], errors="coerce").fillna(0).astype(int)
    return df

def _df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """
    Preserve leading zeros for UPC by forcing string dtype and zfill(12) before writing.
    (Excel may still display without zeros, but the file will contain them.)
    """
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
    # Try xlsxwriter; fall back to openpyxl if needed
    writer_engine = "xlsxwriter"
    try:
        with pd.ExcelWriter(bio, engine=writer_engine) as writer:
            for name, df in sheets.items():
                # Keep UPC as text in Excel by writing as string
                if isinstance(df, pd.DataFrame) and "UPC" in df.columns:
                    df = df.copy()
                    df["UPC"] = df["UPC"].astype(str)
                df.to_excel(writer, sheet_name=name[:31], index=False)
    except Exception:
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            for name, df in sheets.items():
                if isinstance(df, pd.DataFrame) and "UPC" in df.columns:
                    df = df.copy()
                    df["UPC"] = df["UPC"].astype(str)
                df.to_excel(writer, sheet_name=name[:31], index=False)
    return bio.getvalue()

def _update_master_from_invoice(master_df: pd.DataFrame, invoice_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Join invoice items into Master on Full Barcode == invoice UPC.
    Set Cases (from invoice), Total = Pack × Cases, Cost $/¢ from invoice.
    Produce reports: cost_changes, not_in_master, missing_pack.
    """
    inv = invoice_df.copy()
    # Normalize UPCs (safety)
    inv["UPC"] = inv["UPC"].astype(str).apply(_norm12_or_blank)

    prev_costs = master_df[["Full Barcode", "Cost $"]].rename(columns={"Cost $": "_prev_cost"})

    merged = master_df.merge(inv[["UPC", "Item Name", "Cost", "Cases"]],
                             left_on="Full Barcode", right_on="UPC", how="left", suffixes=("", "_inv"))

    has_inv = merged["UPC"].notna()

    # Update Cases only where invoice matched
    merged.loc[has_inv, "Cases"] = pd.to_numeric(merged.loc[has_inv, "Cases_inv"], errors="coerce").fillna(0).astype(int)

    # Recompute Total = Pack × Cases
    merged["Pack"] = pd.to_numeric(merged["Pack"], errors="coerce").fillna(0).astype(int)
    merged["Cases"] = pd.to_numeric(merged["Cases"], errors="coerce").fillna(0).astype(int)
    merged.loc[has_inv, "Total"] = (merged.loc[has_inv, "Pack"] * merged.loc[has_inv, "Cases"]).astype(int)

    # Update Cost $ and Cost ¢ where invoice matched
    new_cost = pd.to_numeric(merged["Cost"], errors="coerce")
    merged.loc[has_inv & new_cost.notna(), "Cost $"] = new_cost[has_inv & new_cost.notna()]
    merged["Cost $"] = pd.to_numeric(merged["Cost $"], errors="coerce").fillna(0.0)
    merged.loc[has_inv, "Cost ¢"] = (merged.loc[has_inv, "Cost $"] * 100).round().astype(int)

    # Reports
    after_costs = merged[["Full Barcode", "Cost $"]].rename(columns={"Cost $": "_new_cost"})
    cost_changes = prev_costs.merge(after_costs, on="Full Barcode", how="left")
    cost_changes = cost_changes[(cost_changes["_new_cost"].notna()) & (cost_changes["_prev_cost"] != cost_changes["_new_cost"])]

    not_in_master = inv[~inv["UPC"].isin(master_df["Full Barcode"])][["UPC", "Item Name", "Cost", "Cases"]].copy()

    missing_pack = merged[has_inv & (merged["Pack"] <= 0)][["Full Barcode", "Pack", "Cases", "Total", "Cost $"]].copy()

    # Restore master column order; drop helper cols
    keep_cols = list(master_df.columns)
    drop_candidates = [c for c in ["UPC", "Item Name", "Cost", "Cases_inv"] if c in merged.columns and c not in keep_cols]
    merged.drop(columns=drop_candidates, inplace=True, errors="ignore")
    for c in keep_cols:
        if c not in merged.columns:
            merged[c] = master_df[c]
    merged = merged[keep_cols]

    return merged, cost_changes, not_in_master, missing_pack

def _build_pricebook_update(pricebook_df: pd.DataFrame, updated_master: pd.DataFrame, invoice_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    POS update contains ONLY items that were in the invoice.
    Join Pricebook.Upc -> Master.Full Barcode.
    Set:
      addstock   = Master.Total
      cost_cents = Master.Cost ¢
    Return (pos_update_df, missing_in_pricebook_df)
    """
    inv_upcs = invoice_df["UPC"].dropna().astype(str).apply(_norm12_or_blank).unique().tolist()

    pb = pricebook_df.copy()
    master = updated_master[["Full Barcode", "Total", "Cost ¢"]].copy()

    merged = pb.merge(master, left_on="Upc", right_on="Full Barcode", how="left")

    # Keep only those rows whose Upc was in the current invoice list
    filtered = merged[merged["Upc"].isin(inv_upcs)].copy()

    # Update fields
    filtered["addstock"] = pd.to_numeric(filtered["Total"], errors="coerce").fillna(0).astype(int)
    filtered["cost_cents"] = pd.to_numeric(filtered["Cost ¢"], errors="coerce").fillna(0).astype(int)

    # Collapse back to pricebook columns
    out_cols = pb.columns.tolist()
    out = filtered[out_cols].copy()

    # Any invoice UPCs not found in pricebook?
    found_upcs = set(filtered["Upc"].astype(str))
    missing = sorted(set(inv_upcs) - found_upcs)
    missing_df = pd.DataFrame({"UPC not in Pricebook": missing})

    return out, missing_df

# =========================
# UI
# =========================

st.title("Invoice → Master/POS Processor")

vendor_label = st.selectbox(
    "Choose vendor",
    list(ALL_PARSERS.keys()),
    index=0
)

slug, ParserClass = ALL_PARSERS[vendor_label]
parser = ParserClass()

c1, c2, c3 = st.columns(3)
with c1:
    if slug == "unified":
        inv_files = st.file_uploader("Upload Unified invoice XLSX/XLS/CSV (one or many)", type=["xlsx","xls","csv"], accept_multiple_files=True)
    elif slug == "southern_glazers":
        inv_files = st.file_uploader("Upload Southern Glazer's PDF file(s)", type=["pdf"], accept_multiple_files=True)
    elif slug == "nevada_beverage":
        inv_files = st.file_uploader("Upload Nevada Beverage PDF file(s)", type=["pdf"], accept_multiple_files=True)
    else:  # breakthru
        inv_files = st.file_uploader("Upload Breakthru CSV file(s)", type=["csv"], accept_multiple_files=True)

with c2:
    master_xlsx = st.file_uploader("Upload MASTER (XLSX)", type=["xlsx"])
with c3:
    pricebook_csv = st.file_uploader("Upload PRICEBOOK (CSV)", type=["csv"])

process = st.button("Process")

# Persist outputs & previews
if "outputs" not in st.session_state:
    st.session_state["outputs"] = None
if "previews" not in st.session_state:
    st.session_state["previews"] = None

if process:
    if not inv_files:
        st.error("Upload at least one invoice file.")
    else:
        # Parse all invoices
        parsed_parts = []
        for f in inv_files:
            try:
                df = parser.parse(f)
            except Exception:
                df = pd.DataFrame(columns=["UPC","Item Name","Cost","Cases"])
            if df is not None and not df.empty:
                # Ensure UPC as 12-digit string (critical for keys)
                df = df.copy()
                df["UPC"] = df["UPC"].astype(str).apply(_norm12_or_blank)
                parsed_parts.append(df)

        invoice_items = pd.concat(parsed_parts, ignore_index=True) if parsed_parts else pd.DataFrame(columns=["UPC","Item Name","Cost","Cases"])

        # BREAKTHRU special backfill: UPC from Master via Item Number → Master["Invoice UPC"] → Master["Full Barcode"]
        if slug == "breakthru" and not invoice_items.empty:
            if "Item Number" not in invoice_items.columns:
                invoice_items["Item Number"] = ""
            if master_xlsx is not None:
                mtmp = _read_master(master_xlsx)[["Invoice UPC", "Full Barcode"]].copy()
                mtmp["Invoice UPC"] = mtmp["Invoice UPC"].astype(str).str.strip()
                invoice_items["Item Number"] = invoice_items["Item Number"].astype(str).str.strip()
                bf = invoice_items.merge(mtmp, left_on="Item Number", right_on="Invoice UPC", how="left")
                needs = (bf["UPC"].astype(str).str.strip() == "") & bf["Full Barcode"].notna()
                bf.loc[needs, "UPC"] = bf.loc[needs, "Full Barcode"].astype(str)
                bf["UPC"] = bf["UPC"].astype(str).apply(_norm12_or_blank)
                invoice_items = bf[["UPC","Item Name","Cost","Cases","Item Number"]].copy()

        # Serialize invoice CSV (keep zeros)
        inv_csv = _df_to_csv_bytes(invoice_items)

        # Build downstream only if provided
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

        # Save for persistent buttons & previews
        st.session_state["outputs"] = {
            "invoice_items_csv": inv_csv,
            "updated_master_xlsx": updated_master_xlsx,
            "cost_changes_csv": cost_changes_csv,
            "not_in_master_csv": not_in_master_csv,
            "missing_pack_csv": missing_pack_csv,
            "pos_update_xlsx": pos_update_xlsx,
            "missing_in_pricebook_csv": missing_in_pricebook_csv,
        }
        st.session_state["previews"] = {
            "invoice_items": invoice_items.head(200),
            "updated_master": (updated_master.head(100) if isinstance(updated_master, pd.DataFrame) and not updated_master.empty else pd.DataFrame()),
            "pos_update": (pos_update_df.head(200) if isinstance(pos_update_df, pd.DataFrame) and not pos_update_df.empty else pd.DataFrame())
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
        st.download_button(
            "⬇️ bthru_invoice_items.csv" if vendor_label == "Breakthru" else "⬇️ invoice_items.csv",
            data=outs["invoice_items_csv"],
            file_name=("bthru_invoice_items.csv" if vendor_label == "Breakthru" else "invoice_items.csv"),
            mime="text/csv"
        )
        if outs["updated_master_xlsx"] is not None:
            st.download_button(
                "⬇️ Updated_Master.xlsx",
                data=outs["updated_master_xlsx"],
                file_name="Updated_Master.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    with d2:
        if outs["cost_changes_csv"] is not None:
            st.download_button(
                "⬇️ Cost_Changes.csv",
                data=outs["cost_changes_csv"],
                file_name="Cost_Changes.csv",
                mime="text/csv"
            )
        if outs["not_in_master_csv"] is not None:
            st.download_button(
                "⬇️ Invoice_NotIn_Master.csv",
                data=outs["not_in_master_csv"],
                file_name="Invoice_NotIn_Master.csv",
                mime="text/csv"
            )

    with d3:
        if outs["missing_pack_csv"] is not None:
            st.download_button(
                "⬇️ Missing_Pack.csv",
                data=outs["missing_pack_csv"],
                file_name="Missing_Pack.csv",
                mime="text/csv"
            )
        if outs["pos_update_xlsx"] is not None:
            st.download_button(
                "⬇️ POS_Update.xlsx",
                data=outs["pos_update_xlsx"],
                file_name="POS_Update.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        if outs["missing_in_pricebook_csv"] is not None:
            st.download_button(
                "⬇️ Pricebook_Missing.csv",
                data=outs["missing_in_pricebook_csv"],
                file_name="Pricebook_Missing.csv",
                mime="text/csv"
            )

    # Previews (persist after downloads)
    st.subheader("Previews")
    with st.expander("Invoice items (first 200)"):
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
