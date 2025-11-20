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
    if len(s) > 0:
        return s.zfill(12)[-12:]
    return ""

class JCSalesParser:
    name = "JC Sales"

    def parse(self, invoice_pdf, master_file, pricebook_file):
        # 1. Parse PDF - returns list of dicts with '_order' key
        invoice_items = self._extract_pdf_data(invoice_pdf)
        invoice_df = pd.DataFrame(invoice_items)
        
        if invoice_df.empty:
            return pd.DataFrame(), pd.DataFrame()

        # 2. Load Master
        if master_file.name.endswith('.csv'):
            master_df = pd.read_csv(master_file, dtype=str)
        else:
            master_df = pd.read_excel(master_file, dtype=str)
        
        # Normalize Master Item for lookup
        master_df['match_item'] = master_df['ITEM'].apply(normalize_for_match)
        # Drop duplicates, keeping first occurrence
        master_unique = master_df.drop_duplicates(subset=['match_item'])
        master_map = master_unique.set_index('match_item').to_dict('index')

        # 3. Load Pricebook
        pb_df = pd.read_csv(pricebook_file, dtype=str)
        pb_df['match_upc'] = pb_df['Upc'].apply(normalize_for_match)
        
        # Lookup dictionaries
        pb_map_idx = dict(zip(pb_df['match_upc'], pb_df.index))
        # Drop duplicates for data lookup
        pb_unique = pb_df.drop_duplicates(subset=['match_upc'])
        pb_data_map = pb_unique.set_index('match_upc').to_dict('index')

        # 4. Logic Loop
        parsed_rows = []
        pos_update_indices = [] 

        # Iterate through invoice items in the order they were extracted
        # We assume invoice_df is already in order 0..N from _extract_pdf_data
        for idx, inv_row in invoice_df.iterrows():
            item_id_raw = str(inv_row['ITEM'])
            item_id = normalize_for_match(item_id_raw)
            
            final_upc_raw = None
            final_upc_display = "No Match"
            pb_entry = None

            # Master Lookup
            if item_id in master_map:
                m_row = master_map[item_id]
                upc1_raw = str(m_row.get('UPC1', ''))
                upc2_raw = str(m_row.get('UPC2', ''))
                
                u1 = normalize_for_match(upc1_raw)
                u2 = normalize_for_match(upc2_raw)

                # Check Pricebook
                if u1 and u1 in pb_data_map:
                    final_upc_raw = u1
                    final_upc_display = upc1_raw 
                    pb_entry = pb_data_map[u1]
                elif u2 and u2 in pb_data_map:
                    final_upc_raw = u2
                    final_upc_display = upc2_raw
                    pb_entry = pb_data_map[u2]
            
            # Format Output UPC
            if final_upc_raw:
                 final_upc_display = normalize_output_upc(final_upc_raw)

            # Calculations
            pack = float(inv_row['PACK'])
            cost = float(inv_row['COST'])
            
            unit = cost / pack if pack else 0
            retail = unit * 2
            
            now_price = 0.0
            delta = 0.0
            
            if pb_entry:
                try:
                    cents = float(pb_entry.get('cents', 0))
                    now_price = cents / 100.0
                    
                    pb_cost_cents = float(pb_entry.get('cost_cents', 0))
                    pb_cost_qty = float(pb_entry.get('cost_qty', 1))
                    if pb_cost_qty == 0: pb_cost_qty = 1
                    
                    pb_unit_cost = (pb_cost_cents / pb_cost_qty) / 100.0
                    delta = unit - pb_unit_cost
                except ValueError:
                    pass

                if final_upc_raw in pb_map_idx:
                     pos_update_indices.append(pb_map_idx[final_upc_raw])

            parsed_rows.append({
                "_order": inv_row['_order'], # Keep original order
                "UPC": final_upc_display,
                "DESCRIPTION": inv_row['DESCRIPTION'],
                "PACK": int(pack),
                "COST": round(cost, 2),
                "UNIT": round(unit, 2),
                "RETAIL": round(retail, 2),
                "NOW": round(now_price, 2),
                "DELTA": round(delta, 2)
            })

        # Build Final DF and Sort by Original Order
        parsed_df = pd.DataFrame(parsed_rows)
        if not parsed_df.empty:
            parsed_df = parsed_df.sort_values('_order').drop(columns=['_order'])
        
        # Build POS Update
        if pos_update_indices:
            unique_indices = list(set(pos_update_indices))
            pos_update_df = pb_df.loc[unique_indices].copy()
            
            # Update Logic
            updates = {}
            for _, row in parsed_df.iterrows():
                u = normalize_for_match(row['UPC'])
                if u and u != "":
                    updates[u] = {
                        'pack': row['PACK'],
                        'cost': row['COST']
                    }
            
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
            
            pos_update_df = pos_update_df.drop(columns=['match_upc'])
        else:
            pos_update_df = pd.DataFrame(columns=pb_df.columns).drop(columns=['match_upc'])

        return parsed_df, pos_update_df

    def _extract_pdf_data(self, pdf_file):
        """
        Extracts items preserving exact invoice order.
        Refined to handle 'T ' artifact in description.
        """
        items = []
        order_counter = 0
        
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text: continue
                
                lines = text.split('\n')
                for line in lines:
                    parts = line.split()
                    if len(parts) < 5: continue
                    
                    # Check if line starts with Item Number (digits)
                    # Must be 3-6 digits.
                    if parts[0].isdigit() and 3 <= len(parts[0]) <= 6:
                        item_num = parts[0]
                        
                        # Parse from RIGHT to LEFT to find prices
                        # Expected tail: ... [PACK] [?] [UNIT_P] [UM_P] [EXT_P]
                        # Example: ... PK 12 1 2.39 28.68 28.68
                        
                        try:
                            # Find indices of the last 3 numbers (UnitP, CaseCost, ExtCost)
                            float_indices = []
                            for i in range(len(parts)-1, -1, -1):
                                clean_s = parts[i].replace(',', '')
                                # Regex for price-like number (1.99, 20.40, 0.85)
                                if re.match(r'^\d+(\.\d+)?$', clean_s):
                                    float_indices.append(i)
                                    if len(float_indices) == 3:
                                        break
                            
                            if len(float_indices) == 3:
                                # Indices are reversed: [Ext_P_idx, UM_P_idx, Unit_P_idx]
                                um_p_idx = float_indices[1]
                                cost = float(parts[um_p_idx].replace(',', ''))
                                
                                unit_p_idx = float_indices[2]
                                
                                # Find PACK to the left of Unit Price
                                # Scan backwards from unit_p_idx
                                pack = 1
                                desc_end_idx = unit_p_idx
                                
                                found_pack = False
                                for k in range(unit_p_idx - 1, 0, -1):
                                    tok = parts[k]
                                    if tok.isdigit():
                                        val = int(tok)
                                        # If it's '1', it might be a multiplier. Check one more left.
                                        if val == 1 and k > 1 and parts[k-1].isdigit():
                                             pack = int(parts[k-1])
                                             desc_end_idx = k - 1
                                             found_pack = True
                                             break
                                        elif val > 1:
                                             pack = val
                                             desc_end_idx = k
                                             found_pack = True
                                             break
                                    # Stop if we hit unit measure text
                                    elif tok in ['PK', 'CS', 'EA', 'DZ', 'LB', 'CF']:
                                        desc_end_idx = k
                                        break
                                
                                # EXTRACT DESCRIPTION
                                # parts[0] is Item Num.
                                # parts[1] might be "T" or part of desc.
                                # Description is parts[1:desc_end_idx]
                                raw_desc = parts[1:desc_end_idx]
                                
                                # Clean artifacts: Remove leading "T" if present as standalone token
                                if raw_desc and raw_desc[0] == "T":
                                    raw_desc = raw_desc[1:]
                                
                                description = " ".join(raw_desc)
                                
                                items.append({
                                    '_order': order_counter,
                                    'ITEM': item_num,
                                    'DESCRIPTION': description,
                                    'PACK': pack,
                                    'COST': cost
                                })
                                order_counter += 1
                        except Exception:
                            continue
                            
        return items
