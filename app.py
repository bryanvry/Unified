# app.py
import io
import numpy as np
import pandas as pd
import streamlit as st
from parsers import ALL_PARSERS

st.set_page_config(page_title="Invoice → Master/POS Processor", layout="wide")

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

    # Ensure expected columns exist
    for col in ["Full Barcode", "Invoice UPC", "Pack", "Cases", "Total", "Cost $", "Cost ¢", "Name"]:
        if col not in df.columns:
            df[col] = ""

    # Normalize keys & numerics
    df["Full Barcode"] = df["Full Barcode"].apply(lambda v: _norm12_or_blank(v) if str(v).strip() else "")
    df["Invoice UPC"]  = df["Invoice UPC"].astype(str).str.strip()

    df["Pack"]   = pd.to_numeric(df["Pack"], errors="coerce").fillna(0).astype(int)
    df["Cases"]  = pd.to_numeric(df["Cases"], errors="coerce").fillna(0).astype(int)
    df["Total"]  = pd.to_numeric(df["Total"], errors="coerce").fillna(0).astype(int)
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
                if "UPC" in d.columns:
                    d["UPC"] = d["UPC"].astype(str)
                d.to_excel(writer, sheet_name=name[:31], index=False)
    except Exception:
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            for name, df in sheets.items():
                d = df.copy()
                if "UPC" in d.columns:
                    d["UPC"] = d["UPC"].astype(str)
                d.to_excel(writer, sheet_name=name[:31], index=False)
    return bio.getvalue()

# ----------- Master updater (vendor-aware keys) -----------

def _update_master_from_invoice(master_df: pd.DataFrame,
                                invoice_df: pd.DataFrame,
                                vendor_slug: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Vendor join rules:
      - southern_glazers / nevada_beverage:
          JOIN on Master["Invoice UPC"] == invoice["UPC"]
      - breakthru:
          Primary JOIN on Master["Invoice UPC"] == invoice["UPC"].
          Fallback JOIN on Master["Invoice UPC"] == invoice["Item Number"] (for rows with blank UPC in CSV)
      - unified: handled elsewhere (do NOT call this for unified path)

    After join:
      Cases = invoice Cases
      Total = Pack × Cases
      Cost $ = invoice Cost
      Cost ¢ = round(Cost $ * 100)
    Reports:
      - cost_changes: rows where Cost $ changed
      - not_in_master: invoice rows that didn’t match any Master key
      - missing_pack: matched rows where Pack <= 0
    """
    inv = invoice_df.copy()

    # Normalize invoice UPCs
    if "UPC" in inv.columns:
        inv["UPC"] = inv["UPC"].astype(str).apply(_norm12_or_blank)

    # Build candidate keys depending on vendor
    # ---------------- Southern Glazer's / Nevada Beverage ----------------
    if vendor_slug in ("southern_glazers", "nevada_beverage"):
        # Join on Invoice UPC
        j = master_df.merge(inv[["UPC", "Item Name", "Cost", "Cases"]],
                            left_on="Invoice UPC", right_on="UPC", how="left", suffixes=("", "_inv"))

        matched = j["UPC"].notna()

    # ---------------- Breakthru ----------------
    elif vendor_slug == "breakthru":
        inv["Item Number"] = inv.get("Item Number", "").astype(str).str.strip()

        # First: match by UPC → Invoice UPC
        j = master_df.merge(inv[["UPC", "Item Name", "Cost", "Cases", "Item Number"]],
                            left_on="Invoice UPC", right_on="UPC", how="left", suffixes=("", "_inv"))

        matched = j["UPC"].notna()

        # Fallback for rows still unmatched: try Item Number → Invoice UPC
        if (~matched).any():
            left_unmatched = j[~matched].copy()
            fb = left_unmatched.merge(
                inv[["Item Number", "Item Name", "Cost", "Cases"]],
                left_on="Invoice UPC", right_on="Item Number",
                how="left", suffixes=("", "_fb")
            )

            # Prefer fallback values where main match was missing
            for col_src, col_fb in [
                ("UPC", "Item Number"),    # using Item Number as the key source
                ("Item Name", "Item Name_fb"),
                ("Cost", "Cost_fb"),
                ("Cases", "Cases_fb"),
            ]:
                if col_src not in fb.columns:
                    continue
                # fill only where original is NaN
                if col_fb in fb.columns:
                    fb[col_src] = fb[col_src].where(fb[col_src].notna(), fb[col_fb])

            # Put fallback rows back
            j.loc[~matched, ["UPC", "Item Name", "Cost", "Cases"]] = fb[["UPC", "Item Name", "Cost", "Cases"]].to_numpy()
            matched = j["UPC"].notna()

    else:
        # Unified should not hit this updater
        return master_df, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # Apply updates where matched
    prev_costs = master_df[["Invoice UPC", "Cost $"]].rename(columns={"Cost $": "_prev_cost"})
    j["Pack"]  = pd.to_numeric(j["Pack"],  errors="coerce").fillna(0).astype(int)

    # Cases
    j.loc[matched, "Cases"] = pd.to_numeric(j.loc[matched, "Cases"], errors="coerce").fillna(0).astype(int)

    # Total = Pack × Cases
    j["Cases"] = pd.to_numeric(j["Cases"], errors="coerce").fillna(0).astype(int)
    j.loc[matched, "Total"] = (j.loc[matched, "Pack"] * j.loc[matched, "Cases"]).astype(int)

    # Costs
    new_cost = pd.to_numeric(j["Cost"], errors="coerce")
    j.loc[matched & new_cost.notna(), "Cost $"] = new_cost[matched & new_cost.notna()]
    j["Cost $"] = pd.to_numeric(j["Cost $"], errors="coerce").fillna(0.0)
    j.loc[matched, "Cost ¢"] = (j.loc[matched, "Cost $"] * 100).round().astype(int)

    # Reports
    after_costs = j[["Invoice UPC", "Cost $"]].rename(columns={"Cost $": "_new_cost"})
    cost_changes = prev_costs.merge(after_costs, on="Invoice UPC", how="left")
    cost_changes = cost_changes[(cost_changes["_new_cost"].notna()) & (cost_changes["_prev_cost"] != cost_changes["_new_cost"])]

    # Not in master: those invoice UPCs not found by either rule
    inv_keys = inv["UPC"].tolist()
    if vendor_slug == "breakthru" and "Item Number" in inv.columns:
        inv_keys = inv_keys + inv["Item Number"].astype(str).tolist()
    inv_keys = pd.Series(inv_keys, dtype=str)

    matched_keys = set(j.loc[matched, "Invoice UPC"].astype(str))
    not_in_master = inv[~inv["UPC"].isin(matched_keys)]
    # Also add BRK items where UPC blank and Item Number didn’t match:
    if vendor_slug == "breakthru":
        still_unmatched_item = inv[(inv["UPC"] == "") & (~inv["Item Number"].isin(matched_keys))]
        not_in_master = pd.concat([not_in_master, still_unmatched_item], ignore_index=True).drop_duplicates()

    not_in_master = not_in_master[["UPC", "Item Name", "Cost", "Cases"]].copy()

    # Missing Pack (for matched rows only)
    missing_pack = j[matched & (j["Pack"] <= 0)][["Invoice UPC", "Pack", "Cases", "Total", "Cost $"]].copy()

    # Restore the original master shape (drop helper cols)
    keep_cols = list(master_df.columns)
    j_final = j.copy()
    for c in ["UPC", "Item Name", "Cost", "Cases_x", "Cases_y", "Item Number"]:
        if c in j_final.columns and c not in keep_cols:
            j_final.drop(columns=[c], inplace=True, errors="ignore")
    for c in keep_cols:
        if c not in j_final.columns:
            j_final[c] = master_df[c]
    j_final = j_final[keep_cols]

    return j_final, cost_changes, not_in_master, missing_pack

def _build_pricebook_update(pricebook_df: pd.DataFrame, updated_master: pd.DataFrame, invoice_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Keep ONLY invoice items:
      Upc ∈ invoice UPCs (12-digit)
      addstock   = Master.Total
      cost_cents = Master.Cost ¢
    """
    inv_upcs = invoice_df["UPC"].dropna().astype(str).apply(_norm12_or_blank)
    inv_upcs = inv_upcs[inv_upcs != ""].unique().tolist()

    pb = pricebook_df.copy()
    master = updated_master[["Full Barcode", "Total", "Cost ¢"]].copy()

    merged = pb.merge(master, left_on="Upc", right_on="Full Barcode", how="left")
    filtered = merged[merged["Upc"].isin(inv_upcs)].copy()

    filtered["addstock"] = pd.to_numeric(filtered["Total"], errors="coerce").fillna(0).astype(int)
    filtered["cost_cents"] = pd.to_numeric(filtered["Cost ¢"], errors="coerce").fillna(0).astype(int)

    out_cols = pb.columns.tolist()
    out = filtered[out_cols].copy()

    found_upcs = set(filtered["Upc"].astype(str))
    missing = sorted(set(inv_upcs) - found_upcs)
    missing_df = pd.DataFrame({"UPC not in Pricebook": missing})

    return out, missing_df

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
        # Parse
        chunks = []
        for f in inv_files:
            try:
                df = parser.parse(f)
            except Exception:
                df = pd.DataFrame(columns=["UPC","Item Name","Cost","Cases"])
            if df is not None and not df.empty:
                df = df.copy()
                if "UPC" in df.columns:
                    df["UPC"] = df["UPC"].astype(str).apply(_norm12_or_blank)
                chunks.append(df)

        invoice_items = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=["UPC","Item Name","Cost","Cases"])

        # Breakthru: backfill UPC via Master if blank, using Item Number → Master["Invoice UPC"] → Full Barcode
        if slug == "breakthru" and not invoice_items.empty and master_xlsx is not None:
            mtmp = _read_master(master_xlsx)[["Invoice UPC", "Full Barcode"]].copy()
            mtmp["Invoice UPC"] = mtmp["Invoice UPC"].astype(str).str.strip()
            invoice_items["Item Number"] = invoice_items.get("Item Number", "").astype(str).str.strip()
            bf = invoice_items.merge(mtmp, left_on="Item Number", right_on="Invoice UPC", how="left")
            needs = (bf["UPC"].astype(str).str.strip() == "") & bf["Full Barcode"].notna()
            bf.loc[needs, "UPC"] = bf.loc[needs, "Full Barcode"].astype(str)
            bf["UPC"] = bf["UPC"].astype(str).apply(_norm12_or_blank)
            invoice_items = bf[[c for c in invoice_items.columns if c in bf.columns]].copy()
            # Ensure required columns exist
            for c in ["UPC","Item Name","Cost","Cases"]:
                if c not in invoice_items.columns:
                    invoice_items[c] = "" if c in ("UPC","Item Name") else 0

        # Build invoice CSV (keep zeros)
        inv_csv = _df_to_csv_bytes(invoice_items)

        # Default outputs
        outs = {
            "invoice_items_csv": inv_csv,
            "updated_master_xlsx": None,
            "cost_changes_csv": None,
            "not_in_master_csv": None,
            "missing_pack_csv": None,
            "pos_update_xlsx": None,
            "missing_in_pricebook_csv": None,
        }

        updated_master = None
        pos_update_df = None

        # Unified has a different workflow—do NOT run the generic updater here
        if slug != "unified" and master_xlsx is not None and pricebook_csv is not None and not invoice_items.empty:
            master_df = _read_master(master_xlsx)

            updated_master, cost_changes, not_in_master, missing_pack = _update_master_from_invoice(master_df, invoice_items, slug)

            pricebook_df = _read_pricebook(pricebook_csv)
            pos_update_df, missing_pb = _build_pricebook_update(pricebook_df, updated_master, invoice_items)

            outs["updated_master_xlsx"] = _dfs_to_xlsx_bytes({"Master": updated_master})
            outs["cost_changes_csv"] = _df_to_csv_bytes(cost_changes)
            outs["not_in_master_csv"] = _df_to_csv_bytes(not_in_master)
            outs["missing_pack_csv"] = _df_to_csv_bytes(missing_pack)
            outs["pos_update_xlsx"] = _dfs_to_xlsx_bytes({"POS Update": pos_update_df})
            outs["missing_in_pricebook_csv"] = _df_to_csv_bytes(missing_pb)

        st.session_state["outputs"] = outs
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

    # Previews
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
