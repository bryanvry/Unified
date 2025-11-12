# app.py
import io
import datetime as dt
import numpy as np
import pandas as pd
import streamlit as st

from parsers import ALL_PARSERS

st.set_page_config(page_title="Unified POS Tools", layout="wide")

# -------------------- helpers --------------------

def _read_master(master_file) -> pd.DataFrame:
    x = pd.ExcelFile(master_file)
    # Use first sheet
    df = pd.read_excel(x, sheet_name=0, dtype=str)
    # Normalize important columns
    for col in ["Full Barcode", "Invoice UPC", "Pack", "Cases", "Total", "Cost $", "Cost ¢"]:
        if col not in df.columns:
            df[col] = ""
    # Numeric coercions
    df["Pack"] = pd.to_numeric(df["Pack"], errors="coerce").fillna(0).astype(int)
    df["Cases"] = pd.to_numeric(df["Cases"], errors="coerce").fillna(0).astype(int)
    df["Total"] = pd.to_numeric(df["Total"], errors="coerce").fillna(0).astype(int)
    df["Cost $"] = pd.to_numeric(df["Cost $"].astype(str).str.replace(r"[,$]", "", regex=True), errors="coerce").fillna(0.0)
    df["Cost ¢"] = pd.to_numeric(df["Cost ¢"], errors="coerce").fillna(0).astype(int)

    # Barcodes as 12-digit zero-padded numeric strings (keep blanks empty)
    def norm12(x):
        s = "".join(ch for ch in str(x) if ch.isdigit())
        if not s:
            return ""
        if len(s) > 12:
            s = s[-12:]
        return s.zfill(12)
    df["Full Barcode"] = df["Full Barcode"].apply(lambda x: norm12(x) if str(x).strip() != "" else "")
    df["Invoice UPC"] = df["Invoice UPC"].astype(str).str.strip()
    return df

def _read_pricebook(csv_file) -> pd.DataFrame:
    # POS pricebook is always CSV from your previous specs
    df = pd.read_csv(csv_file, dtype=str, keep_default_na=False)
    # Ensure key columns exist
    for col in ["Upc", "addstock", "cost_cents"]:
        if col not in df.columns:
            df[col] = ""
    # Normalize Upc to 12-digit string (keep blanks empty)
    def norm12(x):
        s = "".join(ch for ch in str(x) if ch.isdigit())
        if not s:
            return ""
        if len(s) > 12:
            s = s[-12:]
        return s.zfill(12)
    df["Upc"] = df["Upc"].apply(lambda x: norm12(x) if str(x).strip() != "" else "")
    # Numeric
    df["addstock"] = pd.to_numeric(df["addstock"], errors="coerce").fillna(0).astype(int)
    df["cost_cents"] = pd.to_numeric(df["cost_cents"], errors="coerce").fillna(0).astype(int)
    return df

def _df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")

def _df_to_xlsx_bytes(dfs: dict[str, pd.DataFrame]) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        for name, df in dfs.items():
            df.to_excel(writer, sheet_name=name[:31], index=False)
    return bio.getvalue()

def _update_master_from_invoice(master_df: pd.DataFrame, invoice_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Update Master with invoice items:
      - join on Master["Full Barcode"] == invoice["UPC"]
      - set Master["Cases"] = invoice["Cases"]
      - set Master["Total"] = Master["Pack"] * Master["Cases"]
      - set Master["Cost $"], Master["Cost ¢"]
    Returns: (updated_master, cost_changes, not_in_master, missing_pack)
    """
    inv = invoice_df.copy()

    # Track previous costs to detect changes
    prev = master_df[["Full Barcode", "Cost $"]].rename(columns={"Cost $": "_prev_cost"})
    m = master_df.merge(inv[["UPC", "Cost", "Cases"]], left_on="Full Barcode", right_on="UPC", how="left")

    # Apply updates only where invoice rows exist
    has_inv = m["UPC"].notna()
    m.loc[has_inv, "Cases"] = pd.to_numeric(m.loc[has_inv, "Cases_y"], errors="coerce").fillna(0).astype(int)
    # Recompute Total = Pack × Cases
    m["Pack"] = pd.to_numeric(m["Pack"], errors="coerce").fillna(0).astype(int)
    m["Cases"] = pd.to_numeric(m["Cases"], errors="coerce").fillna(0).astype(int)
    m.loc[has_inv, "Total"] = (m.loc[has_inv, "Pack"] * m.loc[has_inv, "Cases"]).astype(int)

    # Cost $ / Cost ¢
    new_cost = pd.to_numeric(m["Cost"], errors="coerce")
    m.loc[has_inv & new_cost.notna(), "Cost $"] = new_cost[has_inv & new_cost.notna()]
    m["Cost $"] = pd.to_numeric(m["Cost $"], errors="coerce").fillna(0.0)
    m.loc[has_inv, "Cost ¢"] = (m.loc[has_inv, "Cost $"] * 100).round().astype(int)

    # Build reports
    after = m[["Full Barcode", "Cost $"]].rename(columns={"Cost $": "_new_cost"})
    cost_changes = prev.merge(after, on="Full Barcode", how="left")
    cost_changes = cost_changes[(cost_changes["_new_cost"].notna()) & (cost_changes["_prev_cost"] != cost_changes["_new_cost"])]

    not_in_master = inv[~inv["UPC"].isin(master_df["Full Barcode"])][["UPC", "Item Name", "Cost", "Cases"]].copy()

    # Missing pack (for items present in invoice)
    missing_pack = m[has_inv & (m["Pack"] <= 0)][["Full Barcode", "Pack", "Cases", "Total", "Cost $"]].copy()

    # Clean up columns: restore original shape
    keep_cols = list(master_df.columns)
    m_final = m.copy()
    # Drop merge helper columns if present
    for col in ["UPC", "Cost", "Cases_y", "Cases_x"]:
        if col in m_final.columns and col not in keep_cols:
            m_final.drop(columns=[col], inplace=True, errors="ignore")
    # Ensure final columns present
    for col in keep_cols:
        if col not in m_final.columns:
            m_final[col] = master_df[col]
    m_final = m_final[keep_cols]

    return m_final, cost_changes, not_in_master, missing_pack

def _build_pricebook_update(pricebook_df: pd.DataFrame, updated_master: pd.DataFrame, invoice_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Return POS update sheet:
      - Only items that were found in the invoice (i.e., invoice UPCs that exist in Master)
      - addstock = Master.Total
      - cost_cents = Master."Cost ¢"
    Also returns 'missing_in_pricebook' list of any UPCs from Master (from this invoice) not found in pricebook.
    """
    inv_upcs = invoice_df["UPC"].dropna().astype(str).tolist()
    inv_upcs = [u for u in inv_upcs if u != ""]

    pb = pricebook_df.copy()
    master = updated_master[["Full Barcode", "Total", "Cost ¢"]].copy()

    merged = pb.merge(master, left_on="Upc", right_on="Full Barcode", how="left")
    # Keep only rows that were in invoice (Total notna) AND Upc matches
    filtered = merged[merged["Full Barcode"].notna()].copy()

    # Update addstock and cost_cents
    filtered["addstock"] = pd.to_numeric(filtered["Total"], errors="coerce").fillna(0).astype(int)
    filtered["cost_cents"] = pd.to_numeric(filtered["Cost ¢"], errors="coerce").fillna(0).astype(int)

    # Collapse back to POS columns
    out_cols = pb.columns.tolist()
    out = filtered[out_cols].copy()

    # Missing in pricebook
    found_upcs = set(filtered["Upc"].astype(str))
    missing = sorted(set(inv_upcs) - set(found_upcs))
    missing_df = pd.DataFrame({"UPC not in Pricebook": missing})

    return out, missing_df

# -------------------- UI --------------------

st.title("Invoice → Master/POS Processor")

vendor_label = st.selectbox(
    "Choose vendor",
    list(ALL_PARSERS.keys()),
    index=0
)

slug, ParserClass = ALL_PARSERS[vendor_label]
parser = ParserClass()

col1, col2, col3 = st.columns(3)
with col1:
    if slug == "unified":
        inv_files = st.file_uploader("Upload Unified invoice XLSX/XLS/CSV (one or many)", type=["xlsx","xls","csv"], accept_multiple_files=True)
    elif slug == "southern_glazers":
        inv_files = st.file_uploader("Upload Southern Glazer's PDF file(s)", type=["pdf"], accept_multiple_files=True)
    elif slug == "nevada_beverage":
        inv_files = st.file_uploader("Upload Nevada Beverage PDF file(s)", type=["pdf"], accept_multiple_files=True)
    else:  # breakthru
        inv_files = st.file_uploader("Upload Breakthru CSV file(s)", type=["csv"], accept_multiple_files=True)

with col2:
    master_xlsx = st.file_uploader("Upload MASTER (XLSX)", type=["xlsx"])
with col3:
    pricebook_csv = st.file_uploader("Upload PRICEBOOK (CSV)", type=["csv"])

process = st.button("Process")

# Session persistence for download buttons
if "outputs" not in st.session_state:
    st.session_state["outputs"] = None

def _norm12_or_blank(x: str) -> str:
    s = "".join(ch for ch in str(x) if ch.isdigit())
    if not s:
        return ""
    if len(s) > 12:
        s = s[-12:]
    return s.zfill(12)

if process:
    if not inv_files:
        st.error("Upload at least one invoice file.")
    else:
        # Parse invoice(s) and concatenate
        rows = []
        for f in inv_files:
            try:
                df = parser.parse(f)
            except Exception:
                df = pd.DataFrame(columns=["UPC", "Item Name", "Cost", "Cases"])
            if df is not None and not df.empty:
                rows.append(df)

        invoice_items = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=["UPC","Item Name","Cost","Cases"])

        # BREAKTHRU: backfill UPC using Master when UPC is blank via Item Number → Master["Invoice UPC"] → Master["Full Barcode"]
        if slug == "breakthru":
            if "Item Number" not in invoice_items.columns:
                invoice_items["Item Number"] = ""
            # If Master is provided, do the backfill now
            if master_xlsx is not None and not invoice_items.empty:
                master_df_tmp = _read_master(master_xlsx)[["Invoice UPC", "Full Barcode"]].copy()
                # Normalize join keys as strings
                master_df_tmp["Invoice UPC"] = master_df_tmp["Invoice UPC"].astype(str).str.strip()
                invoice_items["Item Number"] = invoice_items["Item Number"].astype(str).str.strip()

                backfill = invoice_items.merge(master_df_tmp, left_on="Item Number", right_on="Invoice UPC", how="left")
                # Where UPC is blank, replace with Full Barcode from master
                needs = (backfill["UPC"].astype(str).str.strip() == "") & backfill["Full Barcode"].notna()
                backfill.loc[needs, "UPC"] = backfill.loc[needs, "Full Barcode"].astype(str)
                # Normalize UPC to 12
                backfill["UPC"] = backfill["UPC"].apply(lambda x: _norm12_or_blank(x))
                # Drop helper columns
                keep = ["UPC", "Item Name", "Cost", "Cases", "Item Number"]
                invoice_items = backfill[keep].copy()

        # Invoice CSV (in invoice order)
        inv_csv_bytes = _df_to_csv_bytes(invoice_items)

        # If we also have MASTER + PRICEBOOK, build the rest
        updated_master_xlsx = None
        cost_changes_csv = None
        not_in_master_csv = None
        missing_pack_csv = None
        pos_update_xlsx = None
        missing_in_pricebook_csv = None

        if master_xlsx is not None and pricebook_csv is not None and not invoice_items.empty:
            master_df = _read_master(master_xlsx)

            # Update master
            updated_master, cost_changes, not_in_master, missing_pack = _update_master_from_invoice(master_df, invoice_items)

            # POS update
            pricebook_df = _read_pricebook(pricebook_csv)
            pos_update_df, missing_pb = _build_pricebook_update(pricebook_df, updated_master, invoice_items)

            # Serialize
            updated_master_xlsx = _df_to_xlsx_bytes({"Master": updated_master})
            cost_changes_csv = _df_to_csv_bytes(cost_changes)
            not_in_master_csv = _df_to_csv_bytes(not_in_master)
            missing_pack_csv = _df_to_csv_bytes(missing_pack)
            pos_update_xlsx = _df_to_xlsx_bytes({"POS Update": pos_update_df})
            missing_in_pricebook_csv = _df_to_csv_bytes(missing_pb)

        # Stash outputs for persistent download buttons
        st.session_state["outputs"] = {
            "invoice_items_csv": inv_csv_bytes,
            "updated_master_xlsx": updated_master_xlsx,
            "cost_changes_csv": cost_changes_csv,
            "not_in_master_csv": not_in_master_csv,
            "missing_pack_csv": missing_pack_csv,
            "pos_update_xlsx": pos_update_xlsx,
            "missing_in_pricebook_csv": missing_in_pricebook_csv,
        }

# -------------- Downloads --------------
if st.session_state["outputs"] is not None:
    outs = st.session_state["outputs"]

    st.subheader("Downloads")
    c1, c2, c3 = st.columns(3)

    with c1:
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

    with c2:
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

    with c3:
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
