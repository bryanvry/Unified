import streamlit as st
import pandas as pd
import numpy as np
import re
import os
from io import BytesIO
import xlsxwriter
import barcode
from barcode.writer import ImageWriter
from datetime import datetime, timedelta
from sqlalchemy import text

# ===== vendor parsers =====
from parsers import SouthernGlazersParser, NevadaBeverageParser, BreakthruParser, JCSalesParser, UnifiedParser, CostcoParser

# --- CONFIGURATION ---
st.set_page_config(page_title="LFM Process", page_icon="🧾", layout="wide")

# ==============================================================================
# --- AUTHENTICATION GATE & LOGO ---
# ==============================================================================
MASTER_PASSKEY = st.secrets["APP_PASSKEY"]

# Dynamically build the exact path to the logo file
current_dir = os.path.dirname(os.path.abspath(__file__))
LOGO_PATH = os.path.join(current_dir, "logo.png")

# Initialize session state for authentication
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

# 1. THE LOGIN SCREEN
if not st.session_state["authenticated"]:
    # Center the login box
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.write("")
        st.write("")
        
        # --- Centered Logo on Login Screen ---
        # We use nested columns here to force the logo perfectly into the middle
        logo_col1, logo_col2, logo_col3 = st.columns([1, 2, 1])
        with logo_col2:
            st.image(LOGO_PATH, use_column_width=True)
        
        
        entered_key = st.text_input("Passkey", type="password", placeholder="Enter passkey...")
        
        if st.button("Login", use_container_width=True):
            if entered_key == MASTER_PASSKEY:
                st.session_state["authenticated"] = True
                st.rerun() 
            else:
                st.error("❌ Incorrect passkey. Please try again.")
                
    st.stop() # Prevents the rest of the app from loading


# ==============================================================================

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
def generate_barcode_excel(df):
    """Generates an Excel file with embedded barcode images."""
    output = BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet('New Prices')
    
    # Write headers
    headers = ["UPC", "Brand", "Description", "Now", "New", "Barcode"]
    for col_num, header in enumerate(headers):
        worksheet.write(0, col_num, header)
        
    EAN = barcode.get_barcode_class('ean13')
    
    for row_num, (index, row) in enumerate(df.iterrows(), 1):
        worksheet.write(row_num, 0, str(row['UPC']))
        worksheet.write(row_num, 1, str(row['Brand']))
        worksheet.write(row_num, 2, str(row['Description']))
        worksheet.write(row_num, 3, f"${row['Now']:.2f}" if pd.notna(row['Now']) else "")
        worksheet.write(row_num, 4, f"${row['New']:.2f}")
        
        # Barcode logic (Pad to 12, add leading 0 for EAN-13)
        clean_upc = "".join(filter(str.isdigit, str(row['UPC']))).zfill(12)
        ean_str = "0" + clean_upc
        
        try:
            ean_img = EAN(ean_str, writer=ImageWriter())
            img_io = BytesIO()
            # Generate a clean, text-less barcode image
            ean_img.write(img_io, options={"write_text": False, "module_height": 8.0, "quiet_zone": 2.0})
            img_io.seek(0)
            
            # Insert image into cell
            worksheet.insert_image(row_num, 5, 'barcode.png', {
                'image_data': img_io, 
                'x_scale': 0.4, 
                'y_scale': 0.4,
                'positioning': 1
            })
            worksheet.set_row(row_num, 35) # Make row tall enough for image
        except Exception:
            worksheet.write(row_num, 5, "Error generating")
            
    # Format column widths
    worksheet.set_column('A:A', 15)
    worksheet.set_column('C:C', 40)
    worksheet.set_column('F:F', 30)
    
    workbook.close()
    output.seek(0)
    return output.getvalue()
# --- DATABASE HANDLERS ---
def get_db_connection():
    return st.connection("supabase", type="sql")    
def log_activity(store, vendor, items_cnt, changes_cnt):
    """Logs invoice processing events to Supabase."""
    try:
        conn = get_db_connection()
        # Create a 1-row DataFrame
        log_data = pd.DataFrame([{
            "store": store,
            "vendor": vendor,
            "items_found": int(items_cnt),
            "price_changes_found": int(changes_cnt),
            "created_at": datetime.now()
        }])
        # Append to the SQL table
        log_data.to_sql("invoice_history", conn.engine, if_exists='append', index=False)
    except Exception as e:
        print(f"Logging failed: {e}") # Fail silently so we don't stop the user

def load_pricebook(table_name):
    conn = get_db_connection()
    # CHANGE: Select * to get Department, incltaxes, ebt, etc.
    query = f'SELECT * FROM "{table_name}"'
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
    # We use width=250 so it doesn't stretch massively across the whole screen.
    # You can change 250 to 300, 400, etc., to make it bigger!
    st.image(LOGO_PATH, width=250)

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
tab_order, tab_invoice, tab_search, tab_admin = st.tabs(["Order Management", "Invoice Processing", "🔍 Item Search", "Admin / Uploads"])

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

    # --- B. Interactive Order Builder ---
    with col_gen:
        st.subheader("2. Build Order")
        
        try:
            conn = get_db_connection()
            
            # 1. Company List
            companies_df = conn.query('SELECT DISTINCT "Company" FROM "BeerandLiquorKey"', ttl=0)
            if not companies_df.empty:
                company_options = sorted([str(c) for c in companies_df["Company"].unique() if c is not None and str(c).strip() != 'nan'])
            else:
                company_options = ["Breakthru", "Southern Glazer's", "Nevada Beverage"]
            
            target_company = st.selectbox("Select Company", company_options)
            
            # Button to Load Data into Session State
            if st.button(f"Load {target_company} Items"):
                
                # --- SECURITY FIX: Parameterized Query (No f-string) ---
                map_query = """
                    SELECT "Full Barcode", "Invoice UPC", "0", "Name", "Size", "PACK", "Company" 
                    FROM "BeerandLiquorKey" 
                    WHERE "Company" = :company_name
                """
                vendor_df = conn.query(map_query, params={"company_name": target_company}, ttl=0)
                # -------------------------------------------------------

                if vendor_df.empty:
                    st.warning(f"No items found for {target_company}.")
                    st.session_state['order_df'] = None
                else:
                    vendor_df["_key_norm"] = vendor_df["Full Barcode"].astype(str).apply(_norm_upc_12)
                    
                    # Fetch Pricebook & Sales
                    pb_df = load_pricebook(PRICEBOOK_TABLE)
                    
                    # Look back 24 weeks to ensure we get at least 15 weeks of valid data
                    start_date = datetime.today() - timedelta(weeks=24) 
                    
                    # Note: {SALES_TABLE} is safe here because users can't edit that variable
                    sales_query = f"""
                        SELECT "UPC", "week_date", "qty_sold" 
                        FROM "{SALES_TABLE}" 
                        WHERE "week_date" >= :start_date
                    """
                    sales_hist = conn.query(sales_query, params={"start_date": start_date.strftime('%Y-%m-%d')}, ttl=0)
                    
                    # Merge Map + Pricebook
                    merged = vendor_df.merge(pb_df, left_on="_key_norm", right_on="_norm_upc", how="left")
                    
                    # Handle Name Collision
                    if "Name_x" in merged.columns:
                        merged = merged.rename(columns={"Name_x": "Name"})
                    elif "Name_y" in merged.columns and "Name" not in merged.columns:
                        merged = merged.rename(columns={"Name_y": "Name"})
                    
                    # Pivot Sales (Ascending Order: Oldest -> Newest)
                    sales_cols = []
                    if not sales_hist.empty:
                        sales_hist["_upc_norm"] = sales_hist["UPC"].astype(str).apply(_norm_upc_12)
                        
                        # --- NEW: Convert date objects to strings to prevent JSON crash ---
                        sales_hist["week_date"] = pd.to_datetime(sales_hist["week_date"]).dt.strftime('%Y-%m-%d')
                        # ------------------------------------------------------------------
                        
                        sales_pivot = sales_hist.pivot_table(
                            index="_upc_norm", 
                            columns="week_date", 
                            values="qty_sold", 
                            aggfunc="sum"
                        ).fillna(0)
                        
                        # Sort Oldest to Newest
                        sorted_dates = sorted(sales_pivot.columns, key=lambda x: str(x), reverse=False)
                        
                        # Keep last 15 weeks
                        sales_cols = sorted_dates[-15:]
                        sales_pivot = sales_pivot[sales_cols]
                        
                        merged = merged.merge(sales_pivot, left_on="_key_norm", right_index=True, how="left")

                    # Logic: Stock
                    if "setstock" in merged.columns:
                        clean_stock = merged["setstock"].astype(str).str.replace('=', '').str.replace('"', '').str.strip()
                        merged["Stock"] = pd.to_numeric(clean_stock, errors='coerce').fillna(0)
                    else:
                        merged["Stock"] = 0
                    
                    # Logic: Initialize Order Column
                    merged["Order"] = 0
                    
                    # Final Columns (REMOVED: "Invoice UPC" and "0")
                    base_cols = ["Full Barcode", "Name", "Size", "PACK"]
                    available_base = [c for c in base_cols if c in merged.columns]
                    
                    final_cols = available_base + sales_cols + ["Stock", "Order"]
                    
                    # Save to Session State
                    st.session_state['order_df'] = merged[final_cols].copy()
                    st.session_state['active_company'] = target_company

        except Exception as e:
            st.error(f"System Error: {e}")

    # 2. Render Interactive Table (Outside col_gen for full width)
    if 'order_df' in st.session_state and st.session_state['order_df'] is not None:
        st.divider()
        st.write(f"**Building Order for: {st.session_state.get('active_company')}**")
        
        # --- Lock all columns EXCEPT 'Order' ---
        all_columns = st.session_state['order_df'].columns.tolist()
        locked_columns = [col for col in all_columns if col != "Order"]
        
        # Editable Dataframe
        edited_df = st.data_editor(
            st.session_state['order_df'],
            use_container_width=True,
            height=600,
            disabled=locked_columns, # Locks everything except 'Order'
            column_config={
                "Order": st.column_config.NumberColumn(
                    "Order Qty",
                    help="Enter cases to order",
                    min_value=0,
                    step=1,
                    required=True
                )
                # Removed the emoji column configs for Stock and PACK
            },
            hide_index=True
        )
        
        # 3. Finish & Download
        if st.button("Finish & Download Order"):
            # Filter: Order > 0
            final_order = edited_df[edited_df["Order"] > 0].copy()
            
            if final_order.empty:
                st.warning("No items ordered (Order Qty is 0 for all rows).")
            else:
                # Sort Alphabetically by Name
                final_order = final_order.sort_values(by="Name", ascending=True)
                
                # Select only requested columns
                output_cols = ["Name", "Size", "Order"]
                valid_cols = [c for c in output_cols if c in final_order.columns]
                
                download_df = final_order[valid_cols]
                
                st.download_button(
                    label=f"⬇️ Download {st.session_state['active_company']} Order",
                    data=to_xlsx_bytes({st.session_state['active_company']: download_df}),
                    file_name=f"ORDER_{st.session_state['active_company']}_{datetime.today().strftime('%Y-%m-%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                st.success(f"Ready! Contains {len(download_df)} items.")
# ==============================================================================
# TAB 2: INVOICE PROCESSING
# ==============================================================================
with tab_invoice:
    # Helper to normalize UPCs to 12 digits (for matching)
    def _norm_upc_12(u):
        if pd.isna(u): return ""
        s = str(u).strip()
        digits = "".join(filter(str.isdigit, s))
        if not digits: return ""
        return digits[-12:].zfill(12) if len(digits) > 0 else ""

    vendor_options = ["Unified", "JC Sales", "Southern Glazer's", "Nevada Beverage", "Breakthru", "Costco"]
    vendor = st.selectbox("Select Vendor", vendor_options)
    
    # --- UNIFIED / JC SALES ---
    if vendor in ["Unified", "JC Sales"]:
        st.info(f"Processing against **{PRICEBOOK_TABLE}**")
        
        # 1. MEMORY LOCK: Keep track of the vendor to reset analysis if switched
        if st.session_state.get("current_un_vendor") != vendor:
            st.session_state["analyze_unified"] = False
            st.session_state["current_un_vendor"] = vendor
            if "unified_final_df" in st.session_state:
                del st.session_state["unified_final_df"]
                
        inv_dfs = []

        # Unified UI
        if vendor == "Unified":
            up_files = st.file_uploader("Upload Unified Invoice(s)", type=["csv", "xlsx", "xls"], accept_multiple_files=True, key="un_files")
            
            # Reset if files are cleared
            if not up_files:
                st.session_state["analyze_unified"] = False
                
            if st.button("Process Unified"):
                st.session_state["analyze_unified"] = True
                if "unified_final_df" in st.session_state: # Clear old exports
                    del st.session_state["unified_final_df"]
                    
            if st.session_state.get("analyze_unified", False) and up_files:
                for f in up_files:
                    try:
                        f.seek(0)
                        df = UnifiedParser().parse(f)
                        inv_dfs.append(df)
                    except Exception as e:
                        st.error(f"Error parsing {f.name}: {e}")
        
        # JC Sales UI
        elif vendor == "JC Sales":
            jc_text = st.text_area("Paste JC Sales Text", height=200)
            
            if not jc_text:
                st.session_state["analyze_unified"] = False
                
            if st.button("Process JC Sales"):
                st.session_state["analyze_unified"] = True
                if "unified_final_df" in st.session_state:
                    del st.session_state["unified_final_df"]
                    
            if st.session_state.get("analyze_unified", False) and jc_text:
                jc_df, _ = JCSalesParser().parse(jc_text)
                if not jc_df.empty:
                    # Rename JC cols to match Unified standard
                    jc_df = jc_df.rename(columns={"ITEM": "inv_upc_raw", "DESCRIPTION": "Description", "PACK": "Pack", "UNIT": "+Cost"})
                    jc_df["UPC"] = jc_df["inv_upc_raw"]
                    jc_df["Brand"] = "" 
                    jc_df["invoice_date"] = datetime.today() 
                    inv_dfs.append(jc_df)

        # Shared Processing Logic
        if inv_dfs:
            pb_df = load_pricebook(PRICEBOOK_TABLE)
            if pb_df.empty:
                st.error("Pricebook is empty. Please upload one in Admin tab.")
                st.stop()

            full_inv = pd.concat(inv_dfs, ignore_index=True)
            
            # --- 1. DEDUPLICATE (Keep Latest Date) ---
            if "invoice_date" in full_inv.columns:
                full_inv["invoice_date"] = pd.to_datetime(full_inv["invoice_date"], errors='coerce')
                full_inv = full_inv.sort_values("invoice_date", ascending=True)
                full_inv = full_inv.drop_duplicates(subset=["UPC"], keep="last")
            
            full_inv["_norm_upc"] = full_inv["UPC"].astype(str).apply(_norm_upc_12)
            
            # Match against Pricebook
            merged = full_inv.merge(pb_df, on="_norm_upc", how="left")
            
            # 1. Calculate New Values (Invoice)
            merged["New_Cost_Cents"] = (pd.to_numeric(merged["+Cost"], errors='coerce') * 100).fillna(0).astype(int)
            merged["New_Pack"] = pd.to_numeric(merged["Pack"], errors='coerce').fillna(1).astype(int)
            
            # 2. Get Old Values (Database)
            merged["cost_cents"] = pd.to_numeric(merged["cost_cents"], errors='coerce').fillna(0).astype(int)
            merged["cost_qty"] = pd.to_numeric(merged["cost_qty"], errors='coerce').fillna(1).astype(int)
            merged["cents"] = pd.to_numeric(merged["cents"], errors='coerce').fillna(0).astype(int) # Current Unit Price
            
            # 3. Identify Matches
            matched = merged[merged["Upc"].notna()].copy()
            unmatched = merged[merged["Upc"].isna()].copy()
            
            # --- LOG ACTIVITY & UNIT COST COMPARISON ---
            changes_count = 0
            if not matched.empty:
                 safe_old_pack = matched["cost_qty"].replace(0, 1)
                 safe_new_pack = matched["New_Pack"].replace(0, 1)
                 
                 matched["Old_Unit_Cents"] = matched["cost_cents"] / safe_old_pack
                 matched["New_Unit_Cents"] = matched["New_Cost_Cents"] / safe_new_pack
                 
                 matched["Cost_Changed"] = abs(matched["New_Unit_Cents"] - matched["Old_Unit_Cents"]) > 1.0
                 changes_count = matched["Cost_Changed"].sum()

            log_activity(selected_store, vendor, len(full_inv), changes_count)

            if not matched.empty:
                st.divider()
                st.subheader("📊 Invoice Item Details & Retail Calculator")
                
                # --- A. CALCULATE METRICS ---
                margin_divisor = 0.7 if selected_store == "Rancho" else 0.6
                margin_label = "30%" if selected_store == "Rancho" else "40%"
                
                def calc_row_metrics(row):
                    case_cost = row["+Cost"] if pd.notna(row["+Cost"]) else 0.0
                    pack = row["New_Pack"] if row["New_Pack"] > 0 else 1
                    unit_cost = case_cost / pack
                    
                    target_retail = unit_cost / margin_divisor
                    retail_val = np.ceil(target_retail * 10) / 10.0 - 0.01
                    if retail_val < 0: retail_val = 0
                    
                    retail_str = f"${retail_val:.2f}"
                    if row["Cost_Changed"]:
                        retail_str += " *"
                        
                    return unit_cost, retail_str

                metrics = matched.apply(calc_row_metrics, axis=1, result_type='expand')
                matched["Unit Cost"] = metrics[0]
                matched["Retail String"] = metrics[1]
                matched["Now"] = matched["cents"] / 100.0
                
                # --- B. DISPLAY MAIN TABLE ---
                matched["New"] = None
                display_cols = ["UPC", "Brand", "Description", "+Cost", "Unit Cost", "Now", "Retail String", "New"]
                
                final_view = matched[display_cols].rename(columns={
                    "+Cost": "Case Cost",
                    "Unit Cost": "Unit",
                    "Retail String": "Retail"
                })
                
                st.write("**Edit the 'New' column to set a custom retail price.**")
                edited_df = st.data_editor(
                    final_view,
                    column_config={
                        "UPC": st.column_config.TextColumn(disabled=True),
                        "Brand": st.column_config.TextColumn(disabled=True),
                        "Description": st.column_config.TextColumn(disabled=True),
                        "Case Cost": st.column_config.NumberColumn(format="$%.2f", disabled=True),
                        "Unit": st.column_config.NumberColumn(format="$%.2f", disabled=True),
                        "Now": st.column_config.NumberColumn(format="$%.2f", help="Current Pricebook Price", disabled=True),
                        "Retail": st.column_config.TextColumn(help=f"Calculated Retail ({margin_label} Margin). * indicates cost change.", disabled=True),
                        "New": st.column_config.NumberColumn("New ($)", format="$%.2f", min_value=0.0)
                    },
                    use_container_width=True,
                    hide_index=True,
                    height=450
                )

                # --- C. PRICE CHANGES TABLE ---
                changes = matched[matched["Cost_Changed"]].copy()
                if not changes.empty:
                    st.error(f"{len(changes)} Unit Price Changes Detected")
                    display_changes = pd.DataFrame()
                    display_changes["UPC"] = changes["Upc"]
                    display_changes["Brand"] = changes["Brand"] 
                    display_changes["Description"] = changes["Description"]
                    display_changes["Old Unit Cost"] = changes["Old_Unit_Cents"] / 100.0
                    display_changes["New Unit Cost"] = changes["New_Unit_Cents"] / 100.0
                    display_changes["Old Case"] = changes["cost_cents"] / 100.0
                    display_changes["New Case"] = changes["New_Cost_Cents"] / 100.0
                    
                    st.dataframe(display_changes, column_config={"Old Unit Cost": st.column_config.NumberColumn(format="$%.2f"), "New Unit Cost": st.column_config.NumberColumn(format="$%.2f"), "Old Case": st.column_config.NumberColumn(format="$%.2f"), "New Case": st.column_config.NumberColumn(format="$%.2f")}, hide_index=True)

                # --- D. CONFIRM & GENERATE FILES ---
                st.divider()
                st.subheader("Generate Export Files")
                st.caption("When you are finished entering prices, click the button below to build your POS update and Labels.")
                
                if st.button("Confirm Prices & Generate Files", type="primary"):
                    st.session_state["unified_final_df"] = edited_df
                    st.session_state["unified_matched_df"] = matched
                
                # Check if the user has clicked "Confirm"
                if "unified_final_df" in st.session_state and "unified_matched_df" in st.session_state:
                    st.success("Files prepared successfully! Ready for download.")
                    
                    final_edited = st.session_state["unified_final_df"]
                    final_matched = st.session_state["unified_matched_df"]
                    
                    # Capture the finalized edited prices
                    final_matched["User_New_Price"] = final_edited["New"].values
                
                    pos_cols = ["Upc", "Department", "qty", "cents", "incltaxes", "inclfees", "Name", "size", "ebt", "byweight", "Fee Multiplier", "cost_qty", "cost_cents", "addstock"]
                    pos_out = pd.DataFrame()
                    
                    def clean_and_format_upc(u):
                        s = str(u).replace('=', '').replace('"', '').strip()
                        return f'="{s}"'

                    pos_out["Upc"] = final_matched["Upc"].apply(clean_and_format_upc)
                    pos_out["cost_cents"] = final_matched["New_Cost_Cents"]
                    pos_out["cost_qty"] = final_matched["New_Pack"]
                    
                    qty_col = next((c for c in final_matched.columns if c in ["Case Qty", "Case Quantity", "Cases", "Qty"]), None)
                    if qty_col:
                        cases = pd.to_numeric(final_matched[qty_col], errors='coerce').fillna(0)
                        pos_out["addstock"] = (cases * pos_out["cost_qty"]).astype(int)
                    else:
                        pos_out["addstock"] = 0
                    
                    for col in ["Department", "qty", "cents", "incltaxes", "inclfees", "ebt", "byweight", "Fee Multiplier", "size", "Name"]:
                        if col == "cents":
                            base_cents = pd.to_numeric(final_matched["cents"], errors='coerce').fillna(0).astype(int)
                            mask = final_matched["User_New_Price"].notna()
                            base_cents[mask] = (final_matched.loc[mask, "User_New_Price"] * 100).astype(int)
                            pos_out["cents"] = base_cents
                        elif col in final_matched.columns:
                            pos_out[col] = final_matched[col]
                        else:
                            pos_out[col] = ""

                    final_pos_out = pos_out[pos_cols].copy()
                    
                    dl_col1, dl_col2 = st.columns(2)
                    
                    with dl_col1:
                        st.download_button("⬇️ Download POS Update CSV", to_csv_bytes(final_pos_out), f"POS_Update_{vendor}_{datetime.today().strftime('%Y-%m-%d')}.csv", "text/csv", use_container_width=True)
                    
                    with dl_col2:
                        edited_items_only = final_edited[final_edited["New"].notna() & (final_edited["New"] > 0)].copy()
                        if not edited_items_only.empty:
                            st.download_button("🏷️ Download Price Labels (Excel)", data=generate_barcode_excel(edited_items_only), file_name=f"Price_Labels_{datetime.today().strftime('%Y-%m-%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
                        else:
                            st.button("🏷️ Download Price Labels (Excel)", disabled=True, help="Set a 'New' price to generate labels", use_container_width=True)
            
            # Show Unmatched Items
            if not unmatched.empty:
                st.warning(f"{len(unmatched)} items not found in Pricebook.")
                st.dataframe(unmatched[["UPC", "Description", "+Cost"]])
   # --- SG / NV / Breakthru ---
    elif vendor in ["Southern Glazer's", "Nevada Beverage", "Breakthru"]:
        st.info(f"Using **BeerandLiquorKey** Map + **{PRICEBOOK_TABLE}**")
        
        # 1. Keep track of the vendor so we reset analysis if they switch tabs/vendors
        if st.session_state.get("current_sg_vendor") != vendor:
            st.session_state["analyze_sg"] = False
            st.session_state["current_sg_vendor"] = vendor
            
        inv_files = st.file_uploader(f"Upload {vendor} Invoice(s)", accept_multiple_files=True)
        
        # 2. Reset if they clear the uploaded files
        if not inv_files:
            st.session_state["analyze_sg"] = False

        # 3. Use the button to flip a persistent "Session State" switch
        if st.button("Analyze Invoice"):
            st.session_state["analyze_sg"] = True
            
        # 4. Check the session state switch instead of the button!
        if st.session_state.get("analyze_sg", False) and inv_files:
            map_df = load_vendor_map()
            pb_df = load_pricebook(PRICEBOOK_TABLE)
            
            if map_df.empty: 
                st.error("Vendor Map is empty. Go to Admin.")
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

            # --- CRITICAL FIX: Ensure 'Item Number' exists ---
            if "Item Number" not in inv_df.columns:
                inv_df["Item Number"] = ""
            
            # ==============================================================================
            # PRIORITY MATCHING LOGIC
            # ==============================================================================
            map_df["_map_key"] = map_df["Invoice UPC"].astype(str).apply(_norm_upc_12)
            inv_df["_key_item"] = inv_df["Item Number"].astype(str).apply(_norm_upc_12)
            inv_df["_key_upc"] = inv_df["UPC"].astype(str).apply(_norm_upc_12)
            
            # Priority 1: Item Number
            merged_item = inv_df.merge(map_df, left_on="_key_item", right_on="_map_key", how="left", suffixes=("", "_map"))
            mask_matched = merged_item["Full Barcode"].notna()
            
            # Priority 2: UPC (for unmatched)
            unmatched_df = inv_df[~inv_df.index.isin(merged_item[mask_matched].index)].copy()
            if not unmatched_df.empty:
                merged_upc = unmatched_df.merge(map_df, left_on="_key_upc", right_on="_map_key", how="left", suffixes=("", "_map"))
                mapped = pd.concat([merged_item[mask_matched], merged_upc], ignore_index=True)
            else:
                mapped = merged_item

            # --------------------------------------------
            
            missing = mapped[mapped["Full Barcode"].isna()].copy()
            valid = mapped[mapped["Full Barcode"].notna()].copy()
            
            # --- LOG ACTIVITY ---
            changes_count = 0 
            # --------------------

            st.markdown(f"""
            ### 📊 Status Report
            * **Items Found on Invoice:** {len(inv_df)}
            * **Successfully Mapped:** {len(valid)}
            * **Missing from Map:** {len(missing)}
            """)

            st.subheader("Invoice Items Found")
            st.dataframe(inv_df, use_container_width=True)
            
            if not missing.empty:
                st.warning(f"⚠️ {len(missing)} items are not in your Database Map.")
                st.caption("Please add the Full Barcode below and click 'Save to Map'.")
                
                missing["_display_id"] = missing["Item Number"]
                missing.loc[missing["_display_id"] == "", "_display_id"] = missing["UPC"]
                
                # --- FIX 1: Map Vendor name to the Key's short name ---
                company_db_name = vendor
                if vendor == "Southern Glazer's":
                    company_db_name = "Southern"
                elif vendor == "Nevada Beverage":
                    company_db_name = "Nevada"
                # (Breakthru already matches perfectly, so we leave it as is)
                # ------------------------------------------------------

                # --- FIX 2: Reorder columns and add "Size" ---
                edit_df = pd.DataFrame({
                    "Full Barcode": "",
                    "Invoice UPC": missing["_display_id"], 
                    "0": "",
                    "Name": missing["Item Name"],
                    "Size": "",
                    "PACK": 1,
                    "Company": company_db_name
                })
                # ---------------------------------------------
                
                edited_rows = st.data_editor(edit_df, num_rows="dynamic", key="editor_missing")
                
                if st.button("Save New Items to Map"):
                    to_insert = edited_rows[edited_rows["Full Barcode"].astype(str).str.len() > 3].copy()
                    
                    if not to_insert.empty:
                        conn = get_db_connection()
                        to_insert["Invoice UPC"] = to_insert["Invoice UPC"].astype(str)
                        to_insert["Full Barcode"] = to_insert["Full Barcode"].astype(str)
                        # We pass the perfectly ordered and formatted dataframe directly to the DB!
                        to_insert.to_sql("BeerandLiquorKey", conn.engine, if_exists='append', index=False)
                        
                        st.success("Items successfully mapped! Re-analyzing invoice...")
                        st.rerun() 
                    else:
                        st.error("No valid Barcodes were entered.")

            # 2. Process Valid Items
            if not valid.empty:
                valid["_sys_upc_norm"] = valid["Full Barcode"].astype(str).apply(_norm_upc_12)
                final_check = valid.merge(pb_df, left_on="_sys_upc_norm", right_on="_norm_upc", how="left")
                
                # Calculate Costs
                final_check["Inv_Cost_Cents"] = (pd.to_numeric(final_check["Cost"], errors='coerce') * 100).fillna(0).astype(int)
                final_check["PB_Cost_Cents"] = final_check["cost_cents"].fillna(0).astype(int)
                final_check["Diff"] = final_check["Inv_Cost_Cents"] - final_check["PB_Cost_Cents"]
                
                # --- A. DETECT COST CHANGES ---
                changes = final_check[abs(final_check["Diff"]) > 1].copy()
                changes_count = len(changes)
                
                # LOG ACTIVITY
                log_activity(selected_store, vendor, len(inv_df), changes_count)
                
                if not changes.empty:
                    st.error(f"{len(changes)} Cost Changes Detected")
                    
                    display_changes = pd.DataFrame()
                    display_changes["Barcode"] = changes["Full Barcode"]
                    if "Name_y" in changes.columns:
                        display_changes["Item"] = changes["Name_y"]
                    elif "Name_x" in changes.columns:
                        display_changes["Item"] = changes["Name_x"]
                    else:
                        display_changes["Item"] = changes["Name"]
                        
                    display_changes["Old Cost"] = changes["PB_Cost_Cents"] / 100.0
                    display_changes["New Cost"] = changes["Inv_Cost_Cents"] / 100.0
                    
                    st.dataframe(
                        display_changes,
                        column_config={
                            "Old Cost": st.column_config.NumberColumn(format="$%.2f"),
                            "New Cost": st.column_config.NumberColumn(format="$%.2f")
                        },
                        hide_index=True
                    )
                else:
                    st.success("All mapped items match Pricebook costs.")

                # --- B. GENERATE POS UPDATE ---
                st.divider()
                st.subheader("POS Update File")
                
                pos_cols = [
                    "Upc", "Department", "qty", "cents", "incltaxes", "inclfees", 
                    "Name", "size", "ebt", "byweight", "Fee Multiplier", 
                    "cost_qty", "cost_cents", "addstock"
                ]
                
                pos_out = pd.DataFrame()
                
                # --- FIX: Clean UPC Formatter ---
                def clean_and_format_upc(u):
                    s = str(u).replace('=', '').replace('"', '').strip()
                    return f'="{s}"'

                raw_upc = final_check["Full Barcode"].astype(str)
                pos_out["Upc"] = raw_upc.apply(clean_and_format_upc)
                # --------------------------------
                
                pos_out["cost_cents"] = final_check["Inv_Cost_Cents"]
                pos_out["cost_qty"] = pd.to_numeric(final_check["PACK"], errors='coerce').fillna(1).astype(int)
                
                qty_col = "Cases" if "Cases" in final_check.columns else "Qty"
                if qty_col in final_check.columns:
                    cases = pd.to_numeric(final_check[qty_col], errors='coerce').fillna(0)
                    pos_out["addstock"] = (cases * pos_out["cost_qty"]).astype(int)
                else:
                    pos_out["addstock"] = 0

                # Name Logic
                if "Name_y" in final_check.columns:
                    pos_out["Name"] = final_check["Name_y"]
                elif "Name_x" in final_check.columns:
                    pos_out["Name"] = final_check["Name_x"]
                elif "Name" in final_check.columns:
                    pos_out["Name"] = final_check["Name"]
                else:
                    pos_out["Name"] = ""

                # Metadata Fill
                for col in ["Department", "qty", "cents", "incltaxes", "inclfees", "ebt", "byweight", "Fee Multiplier"]:
                    if col in final_check.columns:
                        pos_out[col] = final_check[col]
                    else:
                        pos_out[col] = "" 

                # Size Logic
                if "size" in final_check.columns:
                    pos_out["size"] = final_check["size"]
                elif "Size" in final_check.columns:
                    pos_out["size"] = final_check["Size"]
                else:
                    pos_out["size"] = ""
                
                final_pos_out = pos_out[pos_cols].copy()
                
                total_cases = pos_out["addstock"].sum()
                st.caption(f"Ready to update stock for {len(final_pos_out)} items (Total Units: {total_cases})")
                
                st.download_button(
                    "⬇️ Download POS Update CSV", 
                    to_csv_bytes(final_pos_out), 
                    f"POS_Update_{vendor}_{datetime.today().strftime('%Y-%m-%d')}.csv", 
                    "text/csv"
                )
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
                try:
                    parsed_df = CostcoParser().parse(costco_text)
                    if parsed_df.empty:
                        st.error("No items found in receipt.")
                    else:
                        master_df = pd.read_excel(costco_master, dtype=str)
                        m_item_num = next((c for c in ["Item Number", "Item #"] if c in master_df.columns), "Item Number")
                        m_cost = next((c for c in ["Cost"] if c in master_df.columns), "Cost")
                        
                        master_df["_item_str"] = master_df[m_item_num].astype(str).str.strip()
                        master_df["_cost_float"] = pd.to_numeric(master_df[m_cost], errors="coerce").fillna(0.0)
                        item_cost_map = dict(zip(master_df["_item_str"], master_df["_cost_float"]))
                        
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
                    st.error(f"Error processing master/receipt: {e}")
# ==============================================================================
# TAB 3: ITEM SEARCH
# ==============================================================================
with tab_search:
    st.header(f"Pricebook & Sales Search: {selected_store}")
    
    conn = get_db_connection()
    
    # 1. Figure out exactly how many weeks of sales history exist in the database
    try:
        max_wks_df = conn.query(f'SELECT COUNT(DISTINCT week_date) AS count FROM "{SALES_TABLE}"', ttl=0)
        max_available_weeks = int(max_wks_df.iloc[0]["count"])
    except Exception:
        max_available_weeks = 15 # Fallback
        
    if max_available_weeks < 1: 
        max_available_weeks = 1
    
    # 2. Build the Search UI
    col_search, col_weeks = st.columns([3, 1])
    with col_search:
        search_query = st.text_input("Search by UPC or Item Name", placeholder="e.g. 'Coors' or '071990095116'")
    with col_weeks:
        num_weeks = st.number_input(
            "Sales Weeks to Show", 
            min_value=1, 
            max_value=max_available_weeks, 
            value=min(15, max_available_weeks),
            help=f"Maximum available in database: {max_available_weeks}"
        )
        
    if st.button("Search Pricebook", type="primary") and search_query:
        # Use ILIKE for case-insensitive partial matching (% surrounds the query)
        safe_search = f"%{search_query}%"
        
        pb_query = f"""
            SELECT "Upc", "Name", "size", "cost_cents", "cost_qty", "cents"
            FROM "{PRICEBOOK_TABLE}"
            WHERE "Upc" ILIKE :sq OR "Name" ILIKE :sq
        """
        matched_pb = conn.query(pb_query, params={"sq": safe_search}, ttl=0)
        
        if matched_pb.empty:
            st.warning("No items found matching your search.")
        else:
            # --- Format Pricebook Data ---
            matched_pb["UPC"] = matched_pb["Upc"].astype(str).str.replace('=', '').str.replace('"', '').str.strip()
            matched_pb["Item Name"] = matched_pb["Name"]
            matched_pb["Size"] = matched_pb["size"]
            
            # Calculate Cost (Cost Cents / Cost Qty -> Dollars)
            safe_cost_qty = pd.to_numeric(matched_pb["cost_qty"], errors="coerce").fillna(1).replace(0, 1)
            cost_cents = pd.to_numeric(matched_pb["cost_cents"], errors="coerce").fillna(0)
            matched_pb["Cost"] = (cost_cents / safe_cost_qty) / 100.0
            
            # Calculate Price (Cents -> Dollars)
            price_cents = pd.to_numeric(matched_pb["cents"], errors="coerce").fillna(0)
            matched_pb["Price"] = price_cents / 100.0
            
            base_display = matched_pb[["UPC", "Item Name", "Size", "Cost", "Price"]].copy()
            base_display["_norm_upc"] = base_display["UPC"].apply(_norm_upc_12)
            
            # --- Fetch Sales History ---
            # Smart querying: Only pull sales data back to the exact number of weeks requested
            dates_df = conn.query(f'SELECT DISTINCT week_date FROM "{SALES_TABLE}" ORDER BY week_date DESC LIMIT :limit', params={"limit": num_weeks}, ttl=0)
            
            sales_cols = []
            if not dates_df.empty:
                # FIX: Force datetime conversion and drop any blanks/floats so .min() doesn't crash
                dates_df["week_date"] = pd.to_datetime(dates_df["week_date"], errors="coerce")
                clean_dates = dates_df.dropna(subset=["week_date"])
                
                if not clean_dates.empty:
                    cutoff_date = clean_dates["week_date"].min()
                    
                    # Convert to string for the SQL query parameter to be extra safe
                    cutoff_str = cutoff_date.strftime('%Y-%m-%d')
                    
                    sales_query = f"""
                        SELECT "UPC", "week_date", "qty_sold" 
                        FROM "{SALES_TABLE}" 
                        WHERE "week_date" >= :start_date
                    """
                    sales_hist = conn.query(sales_query, params={"start_date": cutoff_str}, ttl=0)
                
                if not sales_hist.empty:
                    sales_hist["_upc_norm"] = sales_hist["UPC"].astype(str).apply(_norm_upc_12)
                    
                    # Convert dates to strings to prevent JSON rendering crashes
                    sales_hist["week_date"] = pd.to_datetime(sales_hist["week_date"]).dt.strftime('%Y-%m-%d')
                    
                    sales_pivot = sales_hist.pivot_table(
                        index="_upc_norm", 
                        columns="week_date", 
                        values="qty_sold", 
                        aggfunc="sum"
                    ).fillna(0)
                    
                    # Sort dates from oldest to newest
                    sales_cols = sorted(sales_pivot.columns, key=lambda x: str(x), reverse=False)
                    sales_pivot = sales_pivot[sales_cols]
                    
                    # Merge sales back into our main display
                    base_display = base_display.merge(sales_pivot, left_on="_norm_upc", right_index=True, how="left")
            
            # --- Cleanup and Display ---
            base_display = base_display.drop(columns=["_norm_upc"])
            
            # Fill NaNs with 0 for items that have no sales history in the timeframe
            for c in sales_cols:
                if c in base_display.columns:
                    base_display[c] = base_display[c].fillna(0).astype(int)
                    
            st.success(f"Found {len(base_display)} items matching '{search_query}'.")
            
            st.dataframe(
                base_display,
                column_config={
                    "UPC": st.column_config.TextColumn("UPC"),
                    "Item Name": st.column_config.TextColumn("Item Name"),
                    "Size": st.column_config.TextColumn("Size"),
                    "Cost": st.column_config.NumberColumn("Unit Cost", format="$%.2f"),
                    "Price": st.column_config.NumberColumn("Retail Price", format="$%.2f")
                },
                hide_index=True,
                use_container_width=True
            )                    
# ==============================================================================
# TAB 4: ADMIN / UPLOADS
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
                    
                    # UPDATED: Expanded list of columns to save to DB
                    valid_cols = [
                        "Upc", "Department", "qty", "cents", "setstock", "cost_qty", "cost_cents", "Name",
                        "incltaxes", "inclfees", "size", "ebt", "byweight", "Fee Multiplier"
                    ]
                    
                    # Only save columns that actually exist in the uploaded file
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
