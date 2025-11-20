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
        # 1. Parse PDF with strict coordinate-based extraction
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
        master_unique = master_df.drop_duplicates(subset=['match_item'])
        master_map = master_unique.set_index('match_item').to_dict('index')

        # 3. Load Pricebook
        pb_df = pd.read_csv(pricebook_file, dtype=str)
        pb_df['match_upc'] = pb_df['Upc'].apply(normalize_for_match)
        
        # Lookup dictionaries
        pb_map_idx = dict(zip(pb_df['match_upc'], pb_df.index))
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
        Advanced Coordinate-Based Extraction logic.
        1. Extract all words with coordinates.
        2. Sort by Page -> Top -> Left.
        3. Group words into lines based on 'top' alignment.
        4. Parse lines based on X-position of key columns.
        """
        items = []
        
        with pdfplumber.open(pdf_file) as pdf:
            for page_num, page in enumerate(pdf.pages):
                # Extract words with positions
                words = page.extract_words(x_tolerance=3, y_tolerance=3)
                
                # Sort words: Top-to-bottom, then Left-to-right
                # This is crucial for fixing the order issue!
                words.sort(key=lambda w: (int(w['top']), int(w['x0'])))
                
                # Group words into lines
                lines = []
                if words:
                    current_line = [words[0]]
                    for w in words[1:]:
                        # If word is on the same horizontal line (within tolerance)
                        if abs(w['top'] - current_line[-1]['top']) < 5:
                            current_line.append(w)
                        else:
                            lines.append(current_line)
                            current_line = [w]
                    lines.append(current_line)
                
                for line_words in lines:
                    # Reconstruct the text line
                    line_text = " ".join([w['text'] for w in line_words])
                    
                    # HEADER SKIP Logic
                    # If line is clearly header junk, skip it
                    if "PAGE" in line_text.upper() and "OF" in line_text.upper(): continue
                    if "INVOICE" in line_text.upper(): continue
                    if "SUBTOTAL" in line_text.upper(): continue

                    # --- PARSING LOGIC ---
                    # Valid item line must:
                    # 1. Start with numeric Item Code
                    # 2. End with numeric prices
                    
                    first_word = line_words[0]['text']
                    if not (first_word.isdigit() and len(first_word) >= 3):
                        continue
                        
                    item_num = first_word
                    
                    # Find the pricing block at the end
                    # We look for the last 3 numbers that look like prices
                    float_indices = []
                    for i in range(len(line_words)-1, -1, -1):
                        clean_s = line_words[i]['text'].replace(',', '')
                        if re.match(r'^\d+\.\d+$', clean_s):
                            float_indices.append(i)
                            if len(float_indices) == 3:
                                break
                    
                    if len(float_indices) >= 2:
                        # Indices are reversed: [Ext_P_idx, UM_P_idx, Unit_P_idx]
                        # UM_P (Cost) is usually the 2nd to last price
                        if len(float_indices) >= 3:
                            um_p_idx = float_indices[1]
                            unit_p_idx = float_indices[2]
                        else:
                            # If only 2 prices found (rare), assume Case & Unit?
                            continue

                        try:
                            cost = float(line_words[um_p_idx]['text'].replace(',', ''))
                        except:
                            continue
                            
                        # FIND PACK
                        # Look to the left of Unit Price
                        # Scan backwards from unit_p_idx
                        search_idx = unit_p_idx - 1
                        pack = 1
                        desc_end_idx = unit_p_idx 
                        
                        found_pack = False
                        while search_idx > 0:
                            tok = line_words[search_idx]['text']
                            
                            # Stop at UM code
                            if tok in ['PK', 'CS', 'EA', 'DZ', 'LB', 'CF']:
                                break
                            
                            if tok.isdigit():
                                val = int(tok)
                                if val > 1:
                                    pack = val
                                    # Check left for "PK"
                                    if line_words[search_idx-1]['text'] in ['PK', 'CS', 'EA', 'DZ', 'LB', 'CF']:
                                        desc_end_idx = search_idx - 1
                                    else:
                                        desc_end_idx = search_idx
                                    found_pack = True
                                    break
                                elif val == 1:
                                    # Skip "1" if it's a multiplier, keep looking left
                                    pass
                            
                            search_idx -= 1
                        
                        if not found_pack:
                            # Fallback: sometimes pack is not explicitly listed as integer > 1
                            # Just grab text up to the known unit measure
                            pass
                            
                        # DESCRIPTION
                        # From word 1 (after Item#) to desc_end_idx
                        raw_desc_words = line_words[1:desc_end_idx]
                        
                        # CLEANUP ARTIFACTS
                        # Remove "T", "N", "1" only if they are standalone tokens
                        clean_desc = []
                        for w in raw_desc_words:
                            t = w['text']
                            if t not in ['T', 'N', '1']:
                                clean_desc.append(t)
                        
                        description = " ".join(clean_desc)
                        
                        items.append({
                            'ITEM': item_num,
                            'DESCRIPTION': description,
                            'PACK': pack,
                            'COST': cost
                        })

        return items
