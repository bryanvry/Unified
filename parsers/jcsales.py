import re
import pandas as pd
import numpy as np
import pdfplumber

# --- HELPER FUNCTIONS ---

def normalize_for_match(val):
    """
    Strips non-digits and leading zeros for loose matching.
    e.g. '00012345' -> '12345'
    """
    if pd.isna(val) or val == "":
        return ""
    s = str(val).strip()
    s = re.sub(r"\D", "", s)  # Remove non-digits
    return s.lstrip("0")      # Remove leading zeros

def normalize_output_upc(val):
    """
    Formats UPC to standard 12-digit string for output/POS.
    """
    if pd.isna(val) or val == "":
        return ""
    s = str(val).strip()
    s = re.sub(r"\D", "", s)
    # If it's valid data, pad it to 12 digits
    if len(s) > 0:
        return s.zfill(12)[-12:]
    return ""

class JCSalesParser:
    name = "JC Sales"

    def parse(self, invoice_pdf, master_file, pricebook_file):
        # 1. Parse the PDF Invoice to get ITEM, DESCRIPTION, PACK, COST
        invoice_items = self._extract_pdf_data(invoice_pdf)
        invoice_df = pd.DataFrame(invoice_items)
        
        if invoice_df.empty:
            # Return empty if nothing extracted
            return pd.DataFrame(), pd.DataFrame()

        # 2. Load JC Sales Master (ITEM -> UPC1, UPC2)
        if master_file.name.endswith('.csv'):
            master_df = pd.read_csv(master_file, dtype=str)
        else:
            master_df = pd.read_excel(master_file, dtype=str)
        
        # Normalize ITEM column for joining
        invoice_df['match_item'] = invoice_df['ITEM'].apply(normalize_for_match)
        master_df['match_item'] = master_df['ITEM'].apply(normalize_for_match)
        
        # 3. Load Pricebook
        pb_df = pd.read_csv(pricebook_file, dtype=str)
        # Create a normalized UPC column for matching
        pb_df['match_upc'] = pb_df['Upc'].apply(normalize_for_match)
        
        # --- FIX: Create lookup dictionaries robustly ---
        # Map normalized UPC -> Index in PB dataframe (to grab full row later)
        # We use a simple dictionary comprehension
        pb_map_idx = dict(zip(pb_df['match_upc'], pb_df.index))
        
        # Also keep a dict for fast lookup of pricing data
        # .set_index(...).to_dict('index') works on DataFrame, not Index
        pb_data_map = pb_df.set_index('match_upc').to_dict('index')
        # ------------------------------------------------

        # 4. Merge Logic
        parsed_rows = []
        pos_update_indices = [] # Store indices of PB rows to keep

        for _, inv_row in invoice_df.iterrows():
            item_id = inv_row['match_item']
            
            # Lookup in Master
            master_record = master_df[master_df['match_item'] == item_id]
            
            final_upc_raw = None
            final_upc_display = "No Match"
            pb_entry = None

            if not master_record.empty:
                m_row = master_record.iloc[0]
                upc1_raw = str(m_row.get('UPC1', ''))
                upc2_raw = str(m_row.get('UPC2', ''))
                
                u1 = normalize_for_match(upc1_raw)
                u2 = normalize_for_match(upc2_raw)

                # Check UPC1
                if u1 and u1 in pb_data_map:
                    final_upc_raw = u1
                    final_upc_display = upc1_raw # Use original from master
                    pb_entry = pb_data_map[u1]
                # Check UPC2
                elif u2 and u2 in pb_data_map:
                    final_upc_raw = u2
                    final_upc_display = upc2_raw
                    pb_entry = pb_data_map[u2]
            
            # Format the UPC for the Goal Sheet if match found
            if final_upc_raw:
                 # User requested using the matched UPC. We'll standard formatting if needed, 
                 # but here we stick to the raw match or normalized version.
                 # Usually POS needs 12 digits.
                 final_upc_display = normalize_output_upc(final_upc_raw)

            # --- Calculations ---
            # PACK (#/UM from PDF), COST (UM_P from PDF)
            pack = float(inv_row['PACK'])
            cost = float(inv_row['COST'])
            
            # UNIT = COST / PACK
            unit = cost / pack if pack else 0
            
            # RETAIL = UNIT * 2
            retail = unit * 2
            
            # NOW = cents / .01
            # DELTA = UNIT - (cost_cents / cost_qty / .01)
            now_price = 0.0
            delta = 0.0
            
            if pb_entry:
                try:
                    # NOW
                    cents = float(pb_entry.get('cents', 0))
                    now_price = cents / 100.0
                    
                    # DELTA
                    # Watch out for strings or NaNs in pricebook numbers
                    pb_cost_cents = float(pb_entry.get('cost_cents', 0))
                    pb_cost_qty = float(pb_entry.get('cost_qty', 1))
                    if pb_cost_qty == 0: pb_cost_qty = 1
                    
                    pb_unit_cost = (pb_cost_cents / pb_cost_qty) / 100.0
                    delta = unit - pb_unit_cost
                except ValueError:
                    pass

                # Add to POS update list
                # We assume one match per normalized UPC in pricebook for simplicity,
                # or grab the specific index we mapped earlier
                if final_upc_raw in pb_map_idx:
                     pos_update_indices.append(pb_map_idx[final_upc_raw])

            parsed_rows.append({
                "UPC": final_upc_display,
                "DESCRIPTION": inv_row['DESCRIPTION'],
                "PACK": int(pack),
                "COST": round(cost, 2),
                "UNIT": round(unit, 2),
                "RETAIL": round(retail, 2),
                "NOW": round(now_price, 2),
                "DELTA": round(delta, 2)
            })

        # --- Build Goal Sheet DataFrame ---
        parsed_df = pd.DataFrame(parsed_rows)
        
        # --- Build POS Update DataFrame ---
        if pos_update_indices:
            # Select only the matching rows from original pricebook
            # Use set() to remove duplicates if multiple items map to same UPC
            unique_indices = list(set(pos_update_indices))
            pos_update_df = pb_df.loc[unique_indices].copy()
            
            # Update cost_qty and cost_cents
            # Create a map from normalized UPC -> {pack, cost}
            updates = {}
            for _, row in parsed_df.iterrows():
                # Re-normalize output UPC to match key
                # The output UPC might be formatted (12 digits), so we normalize it back
                u = normalize_for_match(row['UPC'])
                if u and u != "":
                    updates[u] = {
                        'pack': row['PACK'],
                        'cost': row['COST']
                    }
            
            # Apply updates
            def apply_qty(row):
                u = row['match_upc']
                if u in updates:
                    return updates[u]['pack']
                return row['cost_qty']

            def apply_cents(row):
                u = row['match_upc']
                if u in updates:
                    return int(updates[u]['cost'] * 100)
                return row['cost_cents']

            pos_update_df['cost_qty'] = pos_update_df.apply(apply_qty, axis=1)
            pos_update_df['cost_cents'] = pos_update_df.apply(apply_cents, axis=1)
            
            # Drop helper column
            pos_update_df = pos_update_df.drop(columns=['match_upc'])
            
        else:
            # Return empty DF with correct columns if no matches
            pos_update_df = pd.DataFrame(columns=pb_df.columns).drop(columns=['match_upc'])

        return parsed_df, pos_update_df

    def _extract_pdf_data(self, pdf_file):
        """
        Extracts line items from JC Sales PDF.
        Logic: Looks for lines starting with Item Number digits.
        Extracts: ITEM, DESCRIPTION, PACK (#/UM), COST (UM_P)
        """
        items = []
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text: continue
                
                lines = text.split('\n')
                for line in lines:
                    # Tokenize line
                    parts = line.split()
                    if len(parts) < 5: continue
                    
                    # Candidate for Item #: First token must be digits (e.g., "14158")
                    # And typically 3+ digits long
                    if parts[0].isdigit() and len(parts[0]) >= 3:
                        item_num = parts[0]
                        
                        try:
                            # Identify the three floating point numbers at the end
                            # We walk backwards from the end of the line
                            float_indices = []
                            for i in range(len(parts)-1, -1, -1):
                                # Clean commas from numbers
                                clean_s = parts[i].replace(',', '')
                                if re.match(r'^\d+(\.\d+)?$', clean_s):
                                    float_indices.append(i)
                                    if len(float_indices) == 3:
                                        break
                            
                            if len(float_indices) == 3:
                                # Indices are reversed: [Ext_P_idx, UM_P_idx, Unit_P_idx]
                                um_p_idx = float_indices[1]
                                cost = float(parts[um_p_idx].replace(',', ''))
                                
                                # The PACK (#/UM) is usually the integer immediately preceding the UNIT_P
                                # UNIT_P is at float_indices[2]
                                unit_p_idx = float_indices[2]
                                
                                # Look at the token before Unit Price. 
                                # Sometimes there is a "1" or multiplier there.
                                pack = 1
                                desc_end_idx = unit_p_idx
                                
                                # Scan backwards from Unit Price to find Pack
                                found_pack = False
                                for k in range(unit_p_idx - 1, 0, -1):
                                    tok = parts[k]
                                    if tok.isdigit():
                                        if not found_pack:
                                            val = int(tok)
                                            if val == 1:
                                                # Check one more to left for actual pack
                                                prev = parts[k-1] if k>0 else ""
                                                if prev.isdigit():
                                                    pack = int(prev)
                                                    desc_end_idx = k - 1
                                                    found_pack = True
                                                    break
                                                else:
                                                    pack = 1
                                                    desc_end_idx = k
                                            else:
                                                pack = val
                                                desc_end_idx = k
                                                found_pack = True
                                                break
                                    elif tok in ['PK', 'CS', 'EA', 'DZ', 'LB', 'CF']:
                                        desc_end_idx = k
                                        break
                                
                                # Description is everything between Item# (index 0) and desc_end_idx
                                description = " ".join(parts[1:desc_end_idx])
                                
                                items.append({
                                    'ITEM': item_num,
                                    'DESCRIPTION': description,
                                    'PACK': pack,
                                    'COST': cost
                                })
                        except Exception:
                            continue
                            
        return items
