import streamlit as st
import pandas as pd
import numpy as np
import re
from io import BytesIO
from datetime import datetime, timedelta
from sqlalchemy import text

# ===== vendor parsers =====
# Assuming these files exist in your parsers/ folder as before
from parsers import SouthernGlazersParser, NevadaBeverageParser, BreakthruParser, JCSalesParser, UnifiedParser, CostcoParser

# --- CONFIGURATION ---
st.set_page_config(page_title="LFM Process — Database Edition", page_icon="🧾", layout="wide")

# Force Sidebar width
st.markdown(
    """
    <style>
        section[data-testid="stSidebar"] {
            width: 250px !important;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- GLOBAL HELPERS ---
def _norm_upc_12(u: str) -> str:
    """Standardize UPC to 12 digits for DB lookups."""
    s = str(u or "").strip().replace("-", "").replace(" ", "")
    s = "".join(ch for ch in s if ch.isdigit())
    if len(s) == 13 and s.startswith("0"):
        s = s[1:]
    if len(s) > 12:
        s = s[-12:]
    if len(s) < 12:
        s = s.zfill(12)
    return s

def to_csv_bytes(df):
    return df.to_csv(index=False).encode('utf-8')

def to_xlsx_bytes(dfs_dict):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        for sheet_name, df in dfs_dict.items():
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    output.seek(0)
    return output.getvalue()

# --- DATABASE HANDLERS ---
def get_db_connection():
    return st.connection("supabase", type="sql")

def load_pricebook(table_name):
    conn = get_db_connection()
    # Fetch essential columns for processing
    query = f'SELECT "Upc", "cents", "cost_cents", "setstock", "cost_qty", "Name" FROM "{table_name}"'
    try:
        df = conn.query(query, ttl=0) # ttl=0 to ensure fresh data
        # Normalize UPCs immediately for merging
        df["_norm_upc"] = df["Upc"].apply(_norm_upc_12)
        return df
    except Exception as e:
        st.error(f"Error loading Pricebook ({table_name}): {e}")
        return pd.DataFrame()

def load_vendor_map():
    conn = get_db_connection()
    query = 'SELECT * FROM "BeerandLiquorKey"'
    try:
        df = conn.query(query, ttl=0)
        if not df.empty:
             df["_inv_upc_norm"] = df["Invoice UPC"].apply(_norm_upc_12)
        return df
    except Exception as e:
        st.error(f"Error loading Vendor Map: {e}")
        return pd.DataFrame()

# --- SIDEBAR: STORE SELECTION ---
with st.sidebar:
    st.title("Store Selector")
    selected_store = st.radio("Active Store", ["Twain", "Rancho"], index=0)
    
    # Map selection to Table Names
    if selected_store == "Twain":
        PRICEBOOK_TABLE = "PricebookTwain"
        SALES_TABLE = "salestwain1"
    else:
        PRICEBOOK_TABLE = "PricebookRancho"
        SALES_TABLE = "salesrancho1"

    st.divider()
    st.caption(f"Connected to: **{PRICEBOOK_TABLE}**")

# --- MAIN APP TABS ---
tab_order, tab_invoice, tab_admin = st.tabs(["📋 Order Management", "🧾 Invoice Processing", "⚙️ Admin / Uploads"])

# ==============================================================================
# TAB 1: ORDER MANAGEMENT
# ==============================================================================
with tab_order:
    st.header(f"Order Management — {selected_store}")
    
    col_sales, col_gen = st.columns([1, 1])
    
    # --- A. Upload Weekly Sales ---
    with col_sales:
        st.subheader("1. Update Sales History")
        sales_date = st.date_input("Week Ending Date", datetime.today())
        sales_file = st.file_uploader("Upload itemsales.csv", type=["csv"], key="sales_upload")
        
        if sales_file and st.button("Save Sales to DB", type="primary"):
            try:
                sales_df = pd.read_csv(sales_file, dtype=str)
                # Normalize Columns
                # Expected: UPC, Item, # of Items, Sales $
                
                # Check required columns
                req_cols = {"UPC", "Item", "# of Items", "Sales $"}
                if not req_cols.issubset(sales_df.columns):
                    st.error(f"CSV missing columns. Found: {list(sales_df.columns)}")
                else:
                    # Prepare for DB
                    db_rows = pd.DataFrame()
                    db_rows["week_date"] = [sales_date] * len(sales_df)
                    db_rows["UPC"] = sales_df["UPC"]
                    db_rows["Item"] = sales_df["Item"]
                    db_rows["qty_sold"] = pd.to_numeric(sales_df["# of Items"], errors='coerce').fillna(0)
                    db_rows["Sales_Dollars"] = pd.to_numeric(sales_df["Sales $"].astype(str).str.replace('$','').str.replace(',',''), errors='coerce').fillna(0)
                    
                    conn = get_db_connection()
                    db_rows.to_sql(SALES_TABLE, conn.engine, if_exists='append', index=False)
                    st.success(f"Successfully added {len(db_rows)} sales records to {SALES_TABLE} for {sales_date}")
            except Exception as e:
                st.error(f"Failed to process sales file: {e}")

    # --- B. Generate Order Sheet ---
    with col_gen:
        st.subheader("2. Generate Order Sheet")
        template_file = st.file_uploader("Upload Order Template (Beer/Liquor xlsx)", type=["xlsx"], key="template_upload")
        
        if template_file and st.button("Generate Filled Order Sheet"):
            try:
                # 1. Load Template
                template_df = pd.read_csv(template_file) if template_file.name.endswith('.csv') else pd.read_excel(template_file)
                # Expecting 'Full Barcode' or 'Full UPC' column in template to match Pricebook
                # Clean column names
                template_df.columns = [c.strip() for c in template_df.columns]
                
                # Find the Key Column (UPC)
                key_col = None
                for c in ["Full Barcode", "Full UPC", "UPC", "Barcode"]:
                    if c in template_df.columns:
                        key_col = c
                        break
                
                if not key_col:
                    st.error("Could not find a UPC column in template (Full Barcode, Full UPC, UPC).")
                else:
                    # 2. Load Current Data (Pricebook)
                    pb_df = load_pricebook(PRICEBOOK_TABLE)
                    
                    # 3. Load Recent Sales (Last 8 weeks)
                    conn = get_db_connection()
                    start_date = datetime.today() - timedelta(weeks=8)
                    sales_query = f"""
                        SELECT "UPC", "week_date", "qty_sold" 
                        FROM "{SALES_TABLE}" 
                        WHERE "week_date" >= '{start_date.strftime('%Y-%m-%d')}'
                    """
                    sales_hist = conn.query(sales_query, ttl=0)
                    
                    # 4. Merge Data
                    # Normalize keys
                    template_df["_key_norm"] = template_df[key_col].astype(str).apply(_norm_upc_12)
                    
                    # Join Stock & Cost/Price
                    merged = template_df.merge(pb_df, left_on="_key_norm", right_on="_norm_upc", how="left")
                    
                    # Pivot Sales Data (Columns by Date)
                    if not sales_hist.empty:
                        sales_hist["_upc_norm"] = sales_hist["UPC"].astype(str).apply(_norm_upc_12)
                        sales_pivot = sales_hist.pivot_table(
                            index="_upc_norm", 
                            columns="week_date", 
                            values="qty_sold", 
                            aggfunc="sum"
                        ).fillna(0)
                        
                        # Join Sales to Merged
                        merged = merged.merge(sales_pivot, left_on="_key_norm", right_index=True, how="left")
                    
                    # 5. Format Output
                    # Update 'Stock' column in template if exists, else create
                    if "Stock" in merged.columns:
                        merged["Stock"] = merged["setstock"]
                    
                    # Add Cost/Price for reference
                    merged["Current Cost"] = merged["cost_cents"] / 100.0
                    merged["Current Price"] = merged["cents"] / 100.0
                    
                    # Clean up
                    drop_cols = ["_key_norm", "_norm_upc", "setstock", "cost_cents", "cents", "cost_qty"]
                    final_df = merged.drop(columns=[c for c in drop_cols if c in merged.columns], errors='ignore')
                    
                    st.download_button(
                        "⬇️ Download Filled Order Sheet",
                        data=to_xlsx_bytes({"OrderSheet": final_df}),
                        file_name=f"Filled_{template_file.name}",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    st.success("Order Sheet Generated with Live Stock & Sales Data!")
                    
            except Exception as e:
                st.error(f"Error generating sheet: {e}")

# ==============================================================================
# TAB 2: INVOICE PROCESSING
# ==============================================================================
with tab_invoice:
    vendor = st.selectbox("Select Vendor", ["Unified / JC Sales", "Southern Glazer's", "Nevada Beverage", "Breakthru", "Costco"])
    
    # --- A. Unified / JC Sales (Pricebook Direct Match) ---
    if vendor == "Unified / JC Sales":
        st.info(f"Processing against **{PRICEBOOK_TABLE}**")
        
        up_file = st.file_uploader("Upload Unified Invoice(s)", accept_multiple_files=True, key="un_files")
        jc_text = st.text_area("Or Paste JC Sales Text", height=150)
        
        if st.button("Process Invoice"):
            pb_df = load_pricebook(PRICEBOOK_TABLE)
            if pb_df.empty:
                st.error("Pricebook is empty. Please upload one in Admin tab.")
                st.stop()

            # 1. Parse Inputs
            inv_dfs = []
            
            # Unified
            if up_file:
                for f in up_file:
                    try:
                        f.seek(0)
                        df = UnifiedParser().parse(f)
                        inv_dfs.append(df)
                    except Exception as e:
                        st.error(f"Error parsing {f.name}: {e}")
            
            # JC Sales
            if jc_text:
                jc_df, _ = JCSalesParser().parse(jc_text)
                if not jc_df.empty:
                    # Rename JC cols to match Unified standard for processing
                    # JC: ITEM, DESCRIPTION, PACK, COST, UNIT
                    # Unified Std: UPC, Description, Pack, +Cost
                    jc_df = jc_df.rename(columns={"ITEM": "inv_upc_raw", "DESCRIPTION": "Description", "PACK": "Pack", "UNIT": "+Cost"})
                    jc_df["UPC"] = jc_df["inv_upc_raw"] # Start with raw
                    inv_dfs.append(jc_df)
            
            if not inv_dfs:
                st.warning("No data parsed.")
                st.stop()

            # 2. Merge all invoice rows
            full_inv = pd.concat(inv_dfs, ignore_index=True)
            full_inv["_norm_upc"] = full_inv["UPC"].astype(str).apply(_norm_upc_12)
            
            # 3. Match against Pricebook
            # PB has: _norm_upc, Upc, cents, cost_cents, setstock, cost_qty
            merged = full_inv.merge(pb_df, on="_norm_upc", how="left")
            
            # 4. Determine Updates
            # Logic: If invoice cost != PB cost OR Pack != PB cost_qty -> Update
            merged["New_Cost_Cents"] = (pd.to_numeric(merged["+Cost"], errors='coerce') * 100).fillna(0).astype(int)
            merged["New_Pack"] = pd.to_numeric(merged["Pack"], errors='coerce').fillna(1).astype(int)
            
            # Filter matched items
            matched = merged[merged["Upc"].notna()].copy()
            
            # Identify Changes
            # Cost change > 1 cent diff?
            matched["Cost_Changed"] = abs(matched["New_Cost_Cents"] - matched["cost_cents"]) > 1
            matched["Pack_Changed"] = matched["New_Pack"] != matched["cost_qty"]
            
            updates = matched[matched["Cost_Changed"] | matched["Pack_Changed"]].copy()
            
            # 5. Output POS Update File
            if not updates.empty:
                # Format for POS Import (preserving original Pricebook columns logic)
                # We need "Upc", "addstock" (from Total), "cost_cents", "cost_qty"
                pos_out = pd.DataFrame()
                pos_out["Upc"] = updates["Upc"]
                pos_out["cost_cents"] = updates["New_Cost_Cents"]
                pos_out["cost_qty"] = updates["New_Pack"]
                # For unified, we usually add stock based on 'Case Qty' * 'Pack'. 
                # If 'Case Qty' exists in invoice (Unified does, JC might not)
                if "Case Qty" in updates.columns:
                     pos_out["addstock"] = pd.to_numeric(updates["Case Qty"], errors='coerce').fillna(0) * updates["New_Pack"]
                else:
                     pos_out["addstock"] = 0 # Safety
                
                st.success(f"Found {len(updates)} items requiring POS updates.")
                st.download_button("⬇️ Download POS Update CSV", to_csv_bytes(pos_out), f"POS_Update_{selected_store}.csv", "text/csv")
            else:
                st.info("No cost or pack changes detected against current Pricebook.")
            
            # 6. Unmatched Report
            unmatched = merged[merged["Upc"].isna()].copy()
            if not unmatched.empty:
                st.warning(f"{len(unmatched)} items not found in Pricebook.")
                st.dataframe(unmatched[["UPC", "Description", "+Cost"]])

    # --- B. SG / NV / Breakthru (Vendor Map + Pricebook) ---
    elif vendor in ["Southern Glazer's", "Nevada Beverage", "Breakthru"]:
        st.info(f"Using **BeerandLiquorKey** Map + **{PRICEBOOK_TABLE}**")
        
        inv_files = st.file_uploader(f"Upload {vendor} Invoice(s)", accept_multiple_files=True)
        
        if st.button("Analyze Invoice"):
            # 1. Load Data
            map_df = load_vendor_map()
            pb_df = load_pricebook(PRICEBOOK_TABLE)
            
            if map_df.empty: 
                st.error("Vendor Map is empty. Go to Admin.")
                st.stop()
            
            # 2. Parse Invoice
            rows = []
            for f in inv_files:
                f.seek(0)
                if vendor == "Southern Glazer's":
                    rows.append(SouthernGlazersParser().parse(f))
                elif vendor == "Nevada Beverage":
                    rows.append(NevadaBeverageParser().parse(f))
                elif vendor == "Breakthru":
                    rows.append(BreakthruParser().parse(f))
            
            if not rows: st.stop()
            
            inv_df = pd.concat(rows, ignore_index=True)
            # Ensure standard cols: UPC, Item Name, Cost, Cases
            # Normalize Invoice UPC
            inv_df["_inv_upc_norm"] = inv_df["UPC"].astype(str).apply(_norm_upc_12)
            
            # 3. Map Invoice UPC -> System UPC (Full Barcode)
            # Join with Map
            mapped = inv_df.merge(map_df, on="_inv_upc_norm", how="left")
            
            # 4. Handle "Not in Master" (Interactive Add)
            missing = mapped[mapped["Full Barcode"].isna()].copy()
            valid = mapped[mapped["Full Barcode"].notna()].copy()
            
            if not missing.empty:
                st.warning(f"⚠️ {len(missing)} items not found in Vendor Map.")
                st.caption("Edit below and click 'Save to Map' to add them.")
                
                # Prepare Editor DF
                edit_df = pd.DataFrame({
                    "Invoice UPC": missing["UPC"],
                    "Name": missing["Item Name"],
                    "Full Barcode": "", # User must fill
                    "PACK": 1,
                    "Company": vendor,
                    "0": ""
                })
                
                edited_rows = st.data_editor(edit_df, num_rows="dynamic", key="editor_missing")
                
                if st.button("Save New Items to Map"):
                    # Filter for rows where user actually added a Barcode
                    to_insert = edited_rows[edited_rows["Full Barcode"].str.len() > 3].copy()
                    if not to_insert.empty:
                        conn = get_db_connection()
                        # Clean up for DB
                        to_insert["Invoice UPC"] = to_insert["Invoice UPC"].astype(str)
                        to_insert["Full Barcode"] = to_insert["Full Barcode"].astype(str)
                        to_insert.to_sql("BeerandLiquorKey", conn.engine, if_exists='append', index=False)
                        st.success("Items added to Map! Please click 'Analyze Invoice' again to process them.")
                        st.rerun()

            # 5. Compare Costs against Pricebook
            if not valid.empty:
                # Normalize System UPC from Map
                valid["_sys_upc_norm"] = valid["Full Barcode"].astype(str).apply(_norm_upc_12)
                
                # Join with Pricebook
                # PB has cost_cents. Invoice has Cost (dollars).
                final_check = valid.merge(pb_df, left_on="_sys_upc_norm", right_on="_norm_upc", how="left")
                
                # Calculate diffs
                final_check["Inv_Cost_Cents"] = (pd.to_numeric(final_check["Cost"], errors='coerce') * 100).fillna(0).astype(int)
                final_check["PB_Cost_Cents"] = final_check["cost_cents"].fillna(0).astype(int)
                
                # Logic: Diff > 1 cent
                final_check["Diff"] = final_check["Inv_Cost_Cents"] - final_check["PB_Cost_Cents"]
                changes = final_check[abs(final_check["Diff"]) > 1].copy()
                
                if not changes.empty:
                    st.error(f"{len(changes)} Cost Changes Detected")
                    st.dataframe(changes[["Full Barcode", "Name_x", "Cost", "cost_cents", "Diff"]])
                    
                    st.download_button(
                        "⬇️ Download Cost Changes",
                        to_csv_bytes(changes),
                        f"Cost_Changes_{vendor}.csv",
                        "text/csv"
                    )
                else:
                    st.success("All mapped items match Pricebook costs.")

# ==============================================================================
# TAB 3: ADMIN / UPLOADS
# ==============================================================================
with tab_admin:
    st.header("Database Administration")
    
    col_pb, col_map = st.columns(2)
    
    # --- A. Pricebook Initialization ---
    with col_pb:
        st.subheader(f"Update Pricebook ({selected_store})")
        st.caption(f"Target Table: `{PRICEBOOK_TABLE}`")
        pb_upload = st.file_uploader("Upload Pricebook CSV", type=["csv"], key="pb_admin")
        
        if pb_upload and st.button("Replace Pricebook in DB", type="primary"):
            try:
                df = pd.read_csv(pb_upload, dtype=str)
                # Cleanup keys
                df.columns = [c.strip() for c in df.columns]
                
                # Ensure primary key 'Upc' exists
                upc_col = next((c for c in df.columns if c.lower() == 'upc'), None)
                if not upc_col:
                    st.error("CSV must have a 'Upc' column.")
                else:
                    df = df.rename(columns={upc_col: "Upc"})
                    
                    # Connection
                    conn = get_db_connection()
                    
                    # 1. Truncate (Clear) old data for this store
                    # Using text() for raw SQL
                    with conn.session as session:
                        session.execute(text(f'TRUNCATE TABLE "{PRICEBOOK_TABLE}";'))
                        session.commit()
                    
                    # 2. Insert New
                    # We only insert columns that match the DB schema to avoid errors
                    # DB Cols: Upc, Department, qty, cents, setstock, cost_qty, cost_cents
                    valid_cols = ["Upc", "Department", "qty", "cents", "setstock", "cost_qty", "cost_cents", "Name"]
                    # Filter df to valid cols only (if they exist in csv)
                    cols_to_use = [c for c in valid_cols if c in df.columns]
                    
                    df[cols_to_use].to_sql(PRICEBOOK_TABLE, conn.engine, if_exists='append', index=False)
                    st.success(f"Successfully replaced {len(df)} rows in {PRICEBOOK_TABLE}.")
            except Exception as e:
                st.error(f"Error updating pricebook: {e}")

    # --- B. Vendor Map Initialization ---
    with col_map:
        st.subheader("Update Vendor Map (Global)")
        st.caption("Target Table: `BeerandLiquorKey`")
        map_upload = st.file_uploader("Upload Beer & Liquor Master xlsx", type=["xlsx"], key="map_admin")
        
        if map_upload and st.button("Append/Update Map"):
            try:
                df = pd.read_excel(map_upload, dtype=str)
                # Cols: Full Barcode, Invoice UPC, 0, Name, Size, PACK, Company
                
                # Check required
                if "Full Barcode" not in df.columns or "Invoice UPC" not in df.columns:
                    st.error("File missing 'Full Barcode' or 'Invoice UPC'.")
                else:
                    conn = get_db_connection()
                    # Filter standard cols
                    target_cols = ["Full Barcode", "Invoice UPC", "0", "Name", "Size", "PACK", "Company"]
                    cols_to_load = [c for c in target_cols if c in df.columns]
                    
                    # Simple Append strategy for now (or Replace if this is a master re-load)
                    # Use Append to keep history if user prefers, or Replace to reset.
                    # Given the user context ("Not in master... add on"), Append is safer, but duplication is a risk.
                    # Let's do a Replace for the "Master Upload" action to ensure clean state.
                    with conn.session as session:
                        session.execute(text('TRUNCATE TABLE "BeerandLiquorKey";'))
                        session.commit()
                        
                    df[cols_to_load].to_sql("BeerandLiquorKey", conn.engine, if_exists='append', index=False)
                    st.success(f"Map updated with {len(df)} rows.")
            except Exception as e:
                st.error(f"Error updating map: {e}")
