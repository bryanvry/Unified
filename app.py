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
# This MUST be the very first Streamlit command!
st.set_page_config(page_title="LFM Process", page_icon="🧾", layout="wide")

# ==============================================================================
# --- AUTHENTICATION GATE ---
# ==============================================================================
MASTER_PASSKEY = st.secrets["APP_PASSKEY"]

# Initialize session state for authentication
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

# If not authenticated, show the login page and STOP the script
if not st.session_state["authenticated"]:
    # Center the login box for a cleaner landing page look
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.write("")
        st.write("")
        st.title("🔒 LFM Portal Login")
        st.markdown("Please enter the master passkey to access the system.")
        
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
                    
                    # Look back 12 weeks to ensure we get at least 6 weeks of valid data
                    start_date = datetime.today() - timedelta(weeks=12) 
                    
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
                        sales_pivot = sales_hist.pivot_table(
                            index="_upc_norm", 
                            columns="week_date", 
                            values="qty_sold", 
                            aggfunc="sum"
                        ).fillna(0)
                        
                        # Sort Oldest to Newest
                        sorted_dates = sorted(sales_pivot.columns, key=lambda x: str(x), reverse=False)
                        
                        # CHANGED: Keep last 6 weeks instead of 4
                        sales_cols = sorted_dates[-6:]
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
        
        # Editable Dataframe
        edited_df = st.data_editor(
            st.session_state['order_df'],
            use_container_width=True,
            height=600,
            column_config={
                "Order": st.column_config.NumberColumn(
                    "Order Qty",
                    help="Enter cases to order",
                    min_value=0,
                    step=1,
                    required=True
                ),
                "Stock": st.column_config.NumberColumn(
                    "Stock",
                    disabled=True # Prevent editing stock
                )
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
        
        inv_dfs = []
        should_process = False

        # Unified UI
        if vendor == "Unified":
            up_files = st.file_uploader("Upload Unified Invoice(s)", type=["csv", "xlsx", "xls"], accept_multiple_files=True, key="un_files")
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
                    # Rename JC cols to match Unified standard
                    jc_df = jc_df.rename(columns={"ITEM": "inv_upc_raw", "DESCRIPTION": "Description", "PACK": "Pack", "UNIT": "+Cost"})
                    jc_df["UPC"] = jc_df["inv_upc_raw"]
                    jc_df["Brand"] = "" 
                    jc_df["invoice_date"] = datetime.today() 
                    inv_dfs.append(jc_df)
                should_process = True

        # Shared Processing Logic
        if should_process:
            pb_df = load_pricebook(PRICEBOOK_TABLE)
            if pb_df.empty:
                st.error("Pricebook is empty. Please upload one in Admin tab.")
                st.stop()
            
            if not inv_dfs:
                st.warning("No valid data parsed.")
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
            
            # --- LOG ACTIVITY ---
            changes_count = 0
            if not matched.empty:
                 matched["Cost_Changed"] = abs(matched["New_Cost_Cents"] - matched["cost_cents"]) > 1
                 changes_count = matched["Cost_Changed"].sum()

            log_activity(selected_store, vendor, len(full_inv), changes_count)
            # --------------------

            if not matched.empty:
                st.divider()
                st.subheader("📊 Invoice Item Details & Retail Calculator")
                
                # --- A. CALCULATE METRICS ---
                
                def calc_row_metrics(row):
                    # 1. Costs
                    case_cost = row["+Cost"] if pd.notna(row["+Cost"]) else 0.0
                    pack = row["New_Pack"] if row["New_Pack"] > 0 else 1
                    unit_cost = case_cost / pack
                    
                    # 2. Retail Calc (Target 40% margin -> Cost / 0.6 -> Round to .x9)
                    target_retail = unit_cost / 0.6
                    # Round up to next 10 cents, minus 1 cent (e.g. 3.12 -> 3.20 -> 3.19)
                    retail_val = np.ceil(target_retail * 10) / 10.0 - 0.01
                    if retail_val < 0: retail_val = 0
                    
                    # 3. Format Retail String (Add * if Cost Changed)
                    retail_str = f"${retail_val:.2f}"
                    if row["Cost_Changed"]:
                        retail_str += " *"
                        
                    return unit_cost, retail_str

                # Apply
                metrics = matched.apply(calc_row_metrics, axis=1, result_type='expand')
                matched["Unit Cost"] = metrics[0]
                matched["Retail String"] = metrics[1]
                
                # "Now" = Current Pricebook Unit Price (cents column)
                matched["Now"] = matched["cents"] / 100.0
                
                # --- B. DISPLAY MAIN TABLE ---
                # Columns: UPC, Brand, Description, Case Cost, Unit, Now, Retail
                display_cols = ["UPC", "Brand", "Description", "+Cost", "Unit Cost", "Now", "Retail String"]
                
                final_view = matched[display_cols].rename(columns={
                    "+Cost": "Case Cost",
                    "Unit Cost": "Unit",
                    "Retail String": "Retail"
                })
                
                st.dataframe(
                    final_view,
                    column_config={
                        "Case Cost": st.column_config.NumberColumn(format="$%.2f"),
                        "Unit": st.column_config.NumberColumn(format="$%.2f"),
                        "Now": st.column_config.NumberColumn(format="$%.2f", help="Current Pricebook Price (cents)"),
                        "Retail": st.column_config.TextColumn(help="Calculated Retail (40% Margin). * indicates cost change.")
                    },
                    use_container_width=True,
                    hide_index=True
                )

                # --- C. PRICE CHANGES TABLE ---
                changes = matched[matched["Cost_Changed"]].copy()
                
                st.divider()
                if not changes.empty:
                    st.error(f"{len(changes)} Price Changes Detected")
                    
                    display_changes = pd.DataFrame()
                    display_changes["UPC"] = changes["Upc"]
                    display_changes["Brand"] = changes["Brand"] 
                    display_changes["Description"] = changes["Description"]
                    display_changes["Old Cost"] = changes["cost_cents"] / 100.0
                    display_changes["New Cost"] = changes["New_Cost_Cents"] / 100.0
                    
                    st.dataframe(
                        display_changes,
                        column_config={
                            "Old Cost": st.column_config.NumberColumn(format="$%.2f"),
                            "New Cost": st.column_config.NumberColumn(format="$%.2f")
                        },
                        hide_index=True
                    )
                else:
                    st.success("No price changes detected.")

                # --- D. POS UPDATE FILE ---
                st.subheader("POS Update File")
                
                pos_cols = [
                    "Upc", "Department", "qty", "cents", "incltaxes", "inclfees", 
                    "Name", "size", "ebt", "byweight", "Fee Multiplier", 
                    "cost_qty", "cost_cents", "addstock"
                ]
                
                pos_out = pd.DataFrame()
                
                # 1. UPC Format (Clean first, then apply ="01234")
                def clean_and_format_upc(u):
                    s = str(u).replace('=', '').replace('"', '').strip()
                    return f'="{s}"'

                pos_out["Upc"] = matched["Upc"].apply(clean_and_format_upc)
                
                # 2. Key Update Fields
                pos_out["cost_cents"] = matched["New_Cost_Cents"]
                pos_out["cost_qty"] = matched["New_Pack"]
                
                # 3. Calculate AddStock (Cases * Pack)
                qty_col = next((c for c in matched.columns if c in ["Case Qty", "Case Quantity", "Cases", "Qty"]), None)
                
                if qty_col:
                    cases = pd.to_numeric(matched[qty_col], errors='coerce').fillna(0)
                    pos_out["addstock"] = (cases * pos_out["cost_qty"]).astype(int)
                else:
                    pos_out["addstock"] = 0
                
                # 4. Fill Metadata from Pricebook
                for col in ["Department", "qty", "cents", "incltaxes", "inclfees", "ebt", "byweight", "Fee Multiplier", "size", "Name"]:
                    if col in matched.columns:
                        pos_out[col] = matched[col]
                    else:
                        pos_out[col] = ""

                # 5. Filter & Download
                final_pos_out = pos_out[pos_cols].copy()
                total_units = final_pos_out["addstock"].sum()
                
                st.caption(f"Ready to update {len(final_pos_out)} items (Total Stock Added: {total_units})")
                
                st.download_button(
                    "⬇️ Download POS Update CSV", 
                    to_csv_bytes(final_pos_out), 
                    f"POS_Update_{vendor}_{datetime.today().strftime('%Y-%m-%d')}.csv", 
                    "text/csv"
                )
            
            # Show Unmatched Items
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
                
                edit_df = pd.DataFrame({
                    "Invoice UPC": missing["_display_id"], 
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
                        st.success("Items added! Click 'Analyze Invoice' again to include them.")
                        st.rerun()

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
