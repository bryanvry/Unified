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
        # 1. Parse PDF with strict layout preservation
        invoice_items = self._extract_pdf_data(invoice_pdf)
        
        if not invoice_items:
            return pd.DataFrame(), pd.DataFrame()

        # Create DataFrame and ensure we preserve the extraction order
        invoice_df = pd.DataFrame(invoice_items)
        
        # 2. Load Master
        if master_file.name.endswith('.csv'):
            master_df = pd.read_csv(master_file, dtype=str)
        else:
            master_df = pd.read_excel(master_file, dtype=str)
        
        # Normalize Master Item for lookup
        master_df['match_item'] = master_df['ITEM'].apply(normalize_for_match)
        
        # Create efficient lookup map: match_item -> row dict
        # Drop duplicates to avoid explosion (keep first found)
        master_unique = master_df.drop_duplicates(subset=['match_item'])
        master_map = master_unique.set_index('match_item').to_dict('index')

        # 3. Load Pricebook
        pb_df = pd.read_csv(pricebook_file, dtype=str)
        pb_df['match_upc'] = pb_df['Upc'].apply(normalize_for_match)
        
        # Lookup dictionaries
        # Map normalized UPC -> Index in PB dataframe
        pb_map_idx = dict(zip(pb_df['match_upc'], pb_df.index))
        
        # Map normalized UPC -> Data row
        pb_unique = pb_df.drop_duplicates(subset=['match_upc'])
        pb_data_map = pb_unique.set_index('match_upc').to_dict('index')

        # 4. Logic Loop (Iterate Invoice Rows in Order)
        parsed_rows = []
        pos_update_indices = [] 

        for _, inv_row in invoice_df.iterrows():
            item_id = normalize_for_match(inv_row['ITEM'])
            
            final_upc_raw = None
            final_upc_display = "No Match"
            pb_entry = None

            # Lookup in Master Map
            if item_id in master_map:
                m_row = master_map[item_id]
                upc1_raw = str(m_row.get('UPC1', ''))
                upc2_raw = str(m_row.get('UPC2', ''))
                
                u1 = normalize_for_match(upc1_raw)
                u2 = normalize_for_match(upc2_raw)

                # Priority Check: UPC1 then UPC2
                if u1 and u1 in pb_data_map:
                    final_upc_raw = u1
                    final_upc_display = upc1_raw 
                    pb_entry = pb_data_map[u1]
                elif u2 and u2 in pb_data_map:
                    final_upc_raw = u2
                    final_upc_display = upc2_raw
                    pb_entry = pb_data_map[u2]
            
            # If matched, format for output
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

                # Collect index for POS Update file
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

        # Build Final DataFrames
        parsed_df = pd.DataFrame(parsed_rows)
        
        # Build POS Update DF
        if pos_update_indices:
            unique_indices = list(set(pos_update_indices))
            pos_update_df = pb_df.loc[unique_indices].copy()
            
            # Map Invoice Data to Updates
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
        Extracts line items preserving strict order from PDF using PDFPlumber text lines.
        """
        items = []
        
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                # Get all words/lines with their positions
                # We use 'text' mode but we process line by line carefully
                text = page.extract_text()
                if not text: continue
                
                lines = text.split('\n')
                
                for line in lines:
                    # CLEANUP: Remove likely header/footer noise
                    if "PAGE" in line.upper() or "INVOICE" in line.upper() or "SUBTOTAL" in line.upper():
                        continue
                        
                    parts = line.split()
                    if len(parts) < 5: continue
                    
                    # START ANCHOR: Must start with Item # (3-6 digits)
                    # Example: "14158 AXION DISH..."
                    if not (parts[0].isdigit() and 3 <= len(parts[0]) <= 6):
                        continue
                        
                    item_num = parts[0]
                    
                    # END ANCHOR: Look for the pricing block at the end of the line
                    # Pattern: [PACK] [1?] [UNIT_P] [CASE_COST] [EXT_COST]
                    # Example: ... PK 12 1 2.39 28.68 28.68
                    
                    # Scan from right to find floats
                    float_indices = []
                    for i in range(len(parts)-1, -1, -1):
                        clean_s = parts[i].replace(',', '')
                        # Regex for price-like number (1.99, 20.40, 0.85)
                        if re.match(r'^\d+\.\d+$', clean_s):
                            float_indices.append(i)
                    
                    # We need typically 3 prices at end: Ext, UM_P (Cost), Unit_P
                    # Sometimes Ext is missing or broken, but Cost and Unit are usually there.
                    # Let's assume at least 2 floats found.
                    if len(float_indices) >= 2:
                        # Indices are reversed: [Last, 2nd Last, ...]
                        # UM_P (Cost) is usually the 2nd numeric value from right if 3 exist, or 1st if 2?
                        # Standard Layout: Unit_P   UM_P    Ext_P
                        #                  2.39     28.68   28.68
                        
                        # If 3 floats found:
                        if len(float_indices) >= 3:
                            um_p_idx = float_indices[1] # 28.68
                            unit_p_idx = float_indices[2] # 2.39
                        else:
                            # Fallback logic? Maybe only 2 prices printed?
                            continue

                        cost = float(parts[um_p_idx].replace(',', ''))
                        
                        # FIND PACK
                        # Look to the left of Unit Price
                        # There might be a '1' (multiplier?) and 'T' (tax)
                        search_idx = unit_p_idx - 1
                        pack = 1
                        desc_end_idx = unit_p_idx # Default end of desc
                        
                        # Walk backwards
                        while search_idx > 0:
                            tok = parts[search_idx]
                            
                            # If we hit "PK", "CS", "EA", stop.
                            if tok in ['PK', 'CS', 'EA', 'DZ', 'LB', 'CF']:
                                # The number we just passed (to the right) was likely the pack?
                                # Wait, format is usually "PK 12". 
                                # If we are at PK, check to right? No, we scanned left.
                                # In "PK 12 1 2.39":
                                # 2.39 is unit_p.
                                # 1 is at unit_p - 1.
                                # 12 is at unit_p - 2.
                                # PK is at unit_p - 3.
                                break
                            
                            if tok.isdigit():
                                val = int(tok)
                                # Heuristic: 1 is likely a multiplier if we find another number.
                                # If we find > 1, it's definitely pack.
                                if val > 1:
                                    pack = val
                                    # Check if token to left is PK/CS
                                    if parts[search_idx-1] in ['PK', 'CS', 'EA', 'DZ', 'LB', 'CF']:
                                        desc_end_idx = search_idx - 1
                                    else:
                                        desc_end_idx = search_idx
                                    break
                                elif val == 1:
                                    # Keep looking left for the real pack
                                    pass
                            
                            search_idx -= 1
                        
                        # If we exited loop without setting desc_end_idx firmly, use search_idx
                        if desc_end_idx == unit_p_idx: 
                             desc_end_idx = search_idx

                        # Extract Description
                        # parts[0] is Item Num
                        # parts[1] to desc_end_idx is description + noise
                        raw_desc_parts = parts[1:desc_end_idx]
                        
                        # CLEANUP: Remove "T", "N", "1" artifacts from description
                        clean_desc = []
                        for p in raw_desc_parts:
                            if p not in ['T', 'N', '1']:
                                clean_desc.append(p)
                        
                        description = " ".join(clean_desc)
                        
                        items.append({
                            'ITEM': item_num,
                            'DESCRIPTION': description,
                            'PACK': pack,
                            'COST': cost
                        })

        return items
