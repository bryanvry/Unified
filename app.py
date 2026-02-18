import streamlit as st
import pandas as pd
import numpy as np
import re
from io import BytesIO
from datetime import datetime, timedelta
from sqlalchemy import text

# ===== vendor parsers =====
from parsers import SouthernGlazersParser, NevadaBeverageParser, BreakthruParser, JCSalesParser, UnifiedParser, CostcoParser

# --- CONFIGURATION ---
st.set_page_config(page_title="LFM Process", page_icon="🧾", layout="wide")

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
    query = f'SELECT "Upc", "cents", "cost_cents", "setstock", "cost_qty", "Name" FROM "{table_name}"'
    try:
        df = conn.query(query, ttl=0)
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

# --- HEADER & STORE SELECTOR (Top Right) ---
col_title, col_store = st.columns([7, 1]) # 7:1 ratio pushes selector to the right

with col_title:
    st.title("LFM Process")

with col_store:
    # Tiny footprint: Selectbox with hidden label
    # This sits neatly in the top right
    selected_store = st.selectbox("Store", ["Twain", "Rancho"], label_visibility="collapsed")
    # Small indicator of connection
    st.caption(f"📍 **{selected_store}**")

# Map selection to Table Names
if selected_store == "Twain":
    PRICEBOOK_TABLE = "PricebookTwain"
    SALES_TABLE = "salestwain1"
else:
    PRICEBOOK_TABLE = "PricebookRancho"
    SALES_TABLE = "salesrancho1"


# --- MAIN APP TABS ---
tab_order, tab_invoice, tab_admin = st.tabs(["📋 Order Management", "🧾 Invoice Processing", "⚙️ Admin / Uploads"])

# ==============================================================================
# TAB 1: ORDER MANAGEMENT
# ==============================================================================
with tab_order:
    st.header(f"Orders: {selected_store}")
    
    col_sales, col_gen = st.columns([1, 1])
    
    # --- A. Upload Weekly Sales ---
    with col_sales:
        st.subheader("1. Update Sales History")
        sales_date = st.date_input("Week Ending Date", datetime.today())
        sales_file = st.file_uploader("Upload itemsales.csv", type=["csv"], key="sales_upload")
        
        if sales_file and st.button("Save Sales to DB", type="primary"):
            try:
                sales_df = pd.read_csv(sales_file, dtype=str)
                # Check for required columns based on your file: "# of Items", "Sales $"
                req_cols = {"UPC", "Item", "# of Items", "Sales $"}
                if not req_cols.issubset(sales_df.columns):
                    st.error(f"CSV missing columns. Found: {list(sales_df.columns)}")
                else:
                    db_rows = pd.DataFrame()
                    db_rows["week_date"] = [sales_date] * len(sales_df)
                    db_rows["UPC"] = sales_df["UPC"]
                    db_rows["Item"] = sales_df["Item"]
                    db_rows["qty_sold"] = pd.to_numeric(sales_df["# of Items"], errors='coerce').fillna(0)
                    db_rows["Sales_Dollars"] = pd.to_numeric(sales_df["Sales $"].astype(str).str.replace('$','').str.replace(',',''), errors='coerce').fillna(0)
                    
                    conn = get_db_connection()
                    db_rows.to_sql(SALES_TABLE, conn.engine, if_exists='append', index=False)
                    st.success(f"Added {len(db_rows)} records to {SALES_TABLE}")
            except Exception as e:
                st.error(f"Failed to process sales file: {e}")

    # --- B. Generate Order Sheet (Database Driven) ---
    with col_gen:
        st.subheader("2. Generate Order Sheet")
        
        # 1. Select Company to Generate For
        target_company = st.selectbox("Select Company", ["Breakthru", "Southern Glazer's", "Nevada Beverage"])
        
        if st.button(f"Generate {target_company} Sheet"):
            try:
                conn = get_db_connection()
                
                # SQL FIX: Escape the single quote for names like "Southern Glazer's"
                safe_company = target_company.replace("'", "''")
                
                # 1. Fetch Vendor Map (Added Invoice UPC and "0" column as requested)
                map_query = f"""
                    SELECT "Full Barcode", "Invoice UPC", "0", "Name", "Size", "PACK", "Company" 
                    FROM "BeerandLiquorKey" 
                    WHERE "Company" = '{safe_company}'
                """
                vendor_df = conn.query(map_query, ttl=0)
                
                if vendor_df.empty:
                    st.warning(f"No items found for {target_company} in BeerandLiquorKey.")
                else:
                    vendor_df["_key_norm"] = vendor_df["Full Barcode"].astype(str).apply(_norm_upc_12)
                    
                    # 2. Fetch Current Stock/Price (Pricebook)
                    pb_df = load_pricebook(PRICEBOOK_TABLE)
                    
                    # 3. Fetch Recent Sales (Last 8 Weeks)
                    start_date = datetime.today() - timedelta(weeks=8)
                    sales_query = f"""
                        SELECT "UPC", "week_date", "qty_sold" 
                        FROM "{SALES_TABLE}" 
                        WHERE "week_date" >= '{start_date.strftime('%Y-%m-%d')}'
                    """
                    sales_hist = conn.query(sales_query, ttl=0)
                    
                    # 4. Merge: Vendor Map + Pricebook (To get Stock)
                    merged = vendor_df.merge(pb_df, left_on="_key_norm", right_on="_norm_upc", how="left")
                    
                    # 5. Pivot & Merge Sales
                    sales_cols = []
                    if not sales_hist.empty:
                        sales_hist["_upc_norm"] = sales_hist["UPC"].astype(str).apply(_norm_upc_12)
                        
                        # Pivot date columns
                        sales_pivot = sales_hist.pivot_table(
                            index="_upc_norm", 
                            columns="week_date", 
                            values="qty_sold", 
                            aggfunc="sum"
                        ).fillna(0)
                        
                        # Sort columns so most recent date is first
                        # Convert column names to dates for sorting, then back to strings
                        sorted_dates = sorted(sales_pivot.columns, key=lambda x: str(x), reverse=True)
                        sales_pivot = sales_pivot[sorted_dates]
                        
                        # KEEP ONLY TOP 4
                        sales_cols = sorted_dates[:4]
                        sales_pivot = sales_pivot[sales_cols]
                        
                        # Join Sales to the Master List
                        merged = merged.merge(sales_pivot, left_on="_key_norm", right_index=True, how="left")
                    
                    # 6. Formatting Columns
                    if "setstock" in merged.columns:
                        merged["Current Stock"] = merged["setstock"].fillna(0)
                    else:
                        merged["Current Stock"] = 0

                    merged["Cost"] = merged["cost_cents"] / 100.0
                    
                    # Define Final Column Order
                    # Requested: Full Barcode, Invoice UPC, 0, Name, Size, PACK, Sales1...4
                    # Added: Current Stock, Cost (Useful for ordering)
                    
                    base_cols = ["Full Barcode", "Invoice UPC", "0", "Name", "Size", "PACK", "Current Stock", "Cost"]
                    
                    # Add the dynamic sales date columns
                    final_cols = base_cols + [c for c in sales_cols if c in merged.columns]
                    
                    final_df = merged[final_cols].copy()
                    
                    st.download_button(
                        f"⬇️ Download {target_company} Sheet",
                        data=to_xlsx_bytes({target_company: final_df}),
                        file_name=f"{target_company}_OrderSheet_{datetime.today().strftime('%Y-%m-%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    st.success(f"Generated sheet with {len(final_df)} items.")
                    
            except Exception as e:
                st.error(f"Error generating sheet: {e}")
# ==============================================================================
# TAB 2: INVOICE PROCESSING
# ==============================================================================
with tab_invoice:
    # 1. Vendor Selection
    vendor_options = ["Unified", "JC Sales", "Southern Glazer's", "Nevada Beverage", "Breakthru", "Costco"]
    vendor = st.selectbox("Select Vendor", vendor_options)
    
    # --- UNIFIED / JC SALES ---
    if vendor in ["Unified", "JC Sales"]:
        st.info(f"Processing against **{PRICEBOOK_TABLE}**")
        
        inv_dfs = []
        should_process = False

        # Unified UI
        if vendor == "Unified":
            up_files = st.file_uploader("Upload Unified Invoice(s)", accept_multiple_files=True, key="un_files")
            if up_files and st.button("Process Unified"):
                for f in up_files:
                    try:
                        f.seek(0)
                        df = UnifiedParser().parse(f)
                        inv_dfs.append(df)
                    except Exception as e:
                        st.error(f"Error parsing {f.name}: {e}")
                should_process = True
        
        # JC Sales UI
        elif vendor == "JC Sales":
            jc_text = st.text_area("Paste JC Sales Text", height=200)
            if jc_text and st.button("Process JC Sales"):
                jc_df, _ = JCSalesParser().parse(jc_text)
                if not jc_df.empty:
                    jc_df = jc_df.rename(columns={"ITEM": "inv_upc_raw", "DESCRIPTION": "Description", "PACK": "Pack", "UNIT": "+Cost"})
                    jc_df["UPC"] = jc_df["inv_upc_raw"]
                    inv_dfs.append(jc_df)
                should_process = True

        if should_process:
            pb_df = load_pricebook(PRICEBOOK_TABLE)
            if pb_df.empty:
                st.error("Pricebook is empty.")
                st.stop()
            
            if not inv_dfs:
                st.warning("No valid data parsed.")
                st.stop()

            full_inv = pd.concat(inv_dfs, ignore_index=True)
            full_inv["_norm_upc"] = full_inv["UPC"].astype(str).apply(_norm_upc_12)
            merged = full_inv.merge(pb_df, on="_norm_upc", how="left")
            
            merged["New_Cost_Cents"] = (pd.to_numeric(merged["+Cost"], errors='coerce') * 100).fillna(0).astype(int)
            merged["New_Pack"] = pd.to_numeric(merged["Pack"], errors='coerce').fillna(1).astype(int)
            
            matched = merged[merged["Upc"].notna()].copy()
            matched["Cost_Changed"] = abs(matched["New_Cost_Cents"] - matched["cost_cents"]) > 1
            matched["Pack_Changed"] = matched["New_Pack"] != matched["cost_qty"]
            
            updates = matched[matched["Cost_Changed"] | matched["Pack_Changed"]].copy()
            
            if not updates.empty:
                pos_out = pd.DataFrame()
                pos_out["Upc"] = updates["Upc"]
                pos_out["cost_cents"] = updates["New_Cost_Cents"]
                pos_out["cost_qty"] = updates["New_Pack"]
                
                if "Case Qty" in updates.columns:
                     pos_out["addstock"] = pd.to_numeric(updates["Case Qty"], errors='coerce').fillna(0) * updates["New_Pack"]
                else:
                     pos_out["addstock"] = 0
                
                st.success(f"Found {len(updates)} items requiring POS updates.")
                st.download_button("⬇️ Download POS Update CSV", to_csv_bytes(pos_out), f"POS_Update_{selected_store}.csv", "text/csv")
            else:
                st.info("No cost/pack changes detected.")
            
            unmatched = merged[merged["Upc"].isna()].copy()
            if not unmatched.empty:
                st.warning(f"{len(unmatched)} items not found in Pricebook.")
                st.dataframe(unmatched[["UPC", "Description", "+Cost"]])

    # --- SG / NV / Breakthru ---
    elif vendor in ["Southern Glazer's", "Nevada Beverage", "Breakthru"]:
        st.info(f"Using **BeerandLiquorKey** Map + **{PRICEBOOK_TABLE}**")
        
        inv_files = st.file_uploader(f"Upload {vendor} Invoice(s)", accept_multiple_files=True)
        
        if st.button("Analyze Invoice"):
            map_df = load_vendor_map()
            pb_df = load_pricebook(PRICEBOOK_TABLE)
            
            if map_df.empty: 
                st.error("Vendor Map is empty.")
                st.stop()
            
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
            inv_df["_inv_upc_norm"] = inv_df["UPC"].astype(str).apply(_norm_upc_12)
            mapped = inv_df.merge(map_df, on="_inv_upc_norm", how="left")
            
            missing = mapped[mapped["Full Barcode"].isna()].copy()
            valid = mapped[mapped["Full Barcode"].notna()].copy()
            
            if not missing.empty:
                st.warning(f"⚠️ {len(missing)} items not found in Vendor Map.")
                st.caption("Edit below and click 'Save to Map'.")
                
                edit_df = pd.DataFrame({
                    "Invoice UPC": missing["UPC"],
                    "Name": missing["Item Name"],
                    "Full Barcode": "",
                    "PACK": 1,
                    "Company": vendor,
                    "0": ""
                })
                
                edited_rows = st.data_editor(edit_df, num_rows="dynamic", key="editor_missing")
                
                if st.button("Save New Items to Map"):
                    to_insert = edited_rows[edited_rows["Full Barcode"].str.len() > 3].copy()
                    if not to_insert.empty:
                        conn = get_db_connection()
                        to_insert["Invoice UPC"] = to_insert["Invoice UPC"].astype(str)
                        to_insert["Full Barcode"] = to_insert["Full Barcode"].astype(str)
                        to_insert.to_sql("BeerandLiquorKey", conn.engine, if_exists='append', index=False)
                        st.success("Items added! Click 'Analyze Invoice' again.")
                        st.rerun()

            if not valid.empty:
                valid["_sys_upc_norm"] = valid["Full Barcode"].astype(str).apply(_norm_upc_12)
                final_check = valid.merge(pb_df, left_on="_sys_upc_norm", right_on="_norm_upc", how="left")
                
                final_check["Inv_Cost_Cents"] = (pd.to_numeric(final_check["Cost"], errors='coerce') * 100).fillna(0).astype(int)
                final_check["PB_Cost_Cents"] = final_check["cost_cents"].fillna(0).astype(int)
                final_check["Diff"] = final_check["Inv_Cost_Cents"] - final_check["PB_Cost_Cents"]
                
                changes = final_check[abs(final_check["Diff"]) > 1].copy()
                
                if not changes.empty:
                    st.error(f"{len(changes)} Cost Changes Detected")
                    st.dataframe(changes[["Full Barcode", "Name_x", "Cost", "cost_cents", "Diff"]])
                    st.download_button("⬇️ Download Cost Changes", to_csv_bytes(changes), f"Cost_Changes_{vendor}.csv", "text/csv")
                else:
                    st.success("All mapped items match Pricebook costs.")

    # --- COSTCO ---
    elif vendor == "Costco":
        st.header("Costco Processor")
        st.markdown("**Note:** Upload your Costco Master List manually.")
        
        costco_master = st.file_uploader("Upload Costco Master List (XLSX)", type=["xlsx"], key="costco_master")
        costco_text = st.text_area("Paste Costco Receipt Text", height=200, key="costco_text")

        if st.button("Process Costco Receipt"):
            if not costco_master or not costco_text:
                st.error("Please provide both Master file and Receipt text.")
            else:
                parsed_df = CostcoParser().parse(costco_text)
                if parsed_df.empty:
                    st.error("No items found in receipt.")
                else:
                    try:
                        master_df = pd.read_excel(costco_master, dtype=str)
                        # Clean Master
                        m_item_num = next((c for c in ["Item Number", "Item #"] if c in master_df.columns), "Item Number")
                        m_cost = next((c for c in ["Cost"] if c in master_df.columns), "Cost")
                        
                        master_df["_item_str"] = master_df[m_item_num].astype(str).str.strip()
                        master_df["_cost_float"] = pd.to_numeric(master_df[m_cost], errors="coerce").fillna(0.0)
                        item_cost_map = dict(zip(master_df["_item_str"], master_df["_cost_float"]))
                        
                        # Auto-Match
                        parsed_df["Item Number"] = parsed_df["Item Number"].astype(str).str.strip()
                        
                        results = []
                        for _, row in parsed_df.iterrows():
                            item = row["Item Number"]
                            price = float(row["Receipt Price"])
                            known_cost = item_cost_map.get(item, 0.0)
                            
                            qty = 1
                            if known_cost > 0:
                                ratio = price / known_cost
                                if abs(ratio - round(ratio)) < 0.05:
                                    qty = int(round(ratio))
                                    if qty == 0: qty = 1
                            
                            results.append({
                                "Item Number": item,
                                "Description": row["Item Name"],
                                "Receipt Price": price,
                                "Calc Qty": qty,
                                "Unit Cost": price / qty
                            })
                        
                        res_df = pd.DataFrame(results)
                        st.success(f"Processed {len(res_df)} items.")
                        st.dataframe(res_df)
                        st.download_button("⬇️ Download Costco Report", to_xlsx_bytes({"Costco": res_df}), "Costco_Report.xlsx")
                        
                    except Exception as e:
                        st.error(f"Error processing master file: {e}")

# ==============================================================================
# TAB 3: ADMIN / UPLOADS
# ==============================================================================
with tab_admin:
    st.header("Database Administration")
    col_pb, col_map = st.columns(2)
    
    with col_pb:
        st.subheader(f"Update Pricebook ({selected_store})")
        st.caption(f"Target: `{PRICEBOOK_TABLE}`")
        pb_upload = st.file_uploader("Upload Pricebook CSV", type=["csv"], key="pb_admin")
        
        if pb_upload and st.button("Replace Pricebook", type="primary"):
            try:
                df = pd.read_csv(pb_upload, dtype=str)
                df.columns = [c.strip() for c in df.columns]
                
                upc_col = next((c for c in df.columns if c.lower() == 'upc'), None)
                if not upc_col:
                    st.error("CSV must have a 'Upc' column.")
                else:
                    df = df.rename(columns={upc_col: "Upc"})
                    conn = get_db_connection()
                    
                    with conn.session as session:
                        session.execute(text(f'TRUNCATE TABLE "{PRICEBOOK_TABLE}";'))
                        session.commit()
                    
                    valid_cols = ["Upc", "Department", "qty", "cents", "setstock", "cost_qty", "cost_cents", "Name"]
                    cols_to_use = [c for c in valid_cols if c in df.columns]
                    
                    df[cols_to_use].to_sql(PRICEBOOK_TABLE, conn.engine, if_exists='append', index=False)
                    st.success(f"Replaced {len(df)} rows in {PRICEBOOK_TABLE}.")
            except Exception as e:
                st.error(f"Error updating pricebook: {e}")

    with col_map:
        st.subheader("Update Vendor Map (Global)")
        st.caption("Target: `BeerandLiquorKey`")
        map_upload = st.file_uploader("Upload Beer & Liquor Master xlsx", type=["xlsx"], key="map_admin")
        
        if map_upload and st.button("Append/Update Map"):
            try:
                df = pd.read_excel(map_upload, dtype=str)
                if "Full Barcode" not in df.columns or "Invoice UPC" not in df.columns:
                    st.error("File missing 'Full Barcode' or 'Invoice UPC'.")
                else:
                    conn = get_db_connection()
                    target_cols = ["Full Barcode", "Invoice UPC", "0", "Name", "Size", "PACK", "Company"]
                    cols_to_load = [c for c in target_cols if c in df.columns]
                    
                    with conn.session as session:
                        session.execute(text('TRUNCATE TABLE "BeerandLiquorKey";'))
                        session.commit()
                        
                    df[cols_to_load].to_sql("BeerandLiquorKey", conn.engine, if_exists='append', index=False)
                    st.success(f"Map updated with {len(df)} rows.")
            except Exception as e:
                st.error(f"Error updating map: {e}")
