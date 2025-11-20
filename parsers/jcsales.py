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
        Extracts line items preserving strict order from PDF.
        Logic: 
        1. Read text line-by-line (pdfplumber handles basic layout).
        2. Look for rows starting with ITEM # (digits).
        3. Use Regex to cleanly separate Description, Pack, and Pricing.
        """
        items = []
        
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                # Extract text preserving physical layout
                text = page.extract_text(layout=True)
                if not text: continue
                
                lines = text.split('\n')
                for line in lines:
                    line = line.strip()
                    if not line: continue

                    # Regex Strategy:
                    # 1. ITEM (Start of line, digits)
                    # 2. DESCRIPTION (Text block)
                    # 3. OPTIONAL: "T" flag (Tax?)
                    # 4. OPTIONAL: "1" (Crv or Quantity?)
                    # 5. UNIT/MEASURE (PK/CS/EA) -- Key Anchor!
                    # 6. PACK (Integer)
                    # 7. Prices...
                    
                    # Invoice layout example:
                    # 118815 TOY DOCTOR PLAY SET ASST COLOR T 1 PK 24 1 0.85 20.40 20.40
                    # Item: 118815
                    # Desc: TOY DOCTOR PLAY SET ASST COLOR
                    # Pack: 24
                    # Cost: 20.40 (UM_P)
                    
                    # We split by whitespace
                    parts = line.split()
                    
                    # Must start with Item# (3-6 digits)
                    if len(parts) < 6 or not (parts[0].isdigit() and 3 <= len(parts[0]) <= 6):
                        continue
                        
                    item_num = parts[0]
                    
                    # SCAN FROM THE RIGHT (End of line) to find the numeric columns
                    # Expected End: ... [PACK] [Multiplier?] [UNIT_PRICE] [CASE_COST] [EXT_COST]
                    # Example: ... 24 1 0.85 20.40 20.40
                    
                    try:
                        # Identify the pricing floats at the end
                        # We look for the last 3 numbers that look like prices
                        float_indices = []
                        for i in range(len(parts)-1, -1, -1):
                            s = parts[i].replace(',', '')
                            if re.match(r'^\d+\.\d+$', s): # Strict float match (X.XX)
                                float_indices.append(i)
                        
                        # We need at least 2 price columns (UnitP, UM_P) or 3 (UnitP, UM_P, ExtP)
                        if len(float_indices) < 2:
                            continue
                            
                        # UM_P (Case Cost) is usually the 2nd to last price
                        # Ext_P is last price.
                        # Unit_P is 3rd to last.
                        
                        # If we have 3 prices [Ext, Case, Unit] (indices reversed)
                        if len(float_indices) >= 3:
                            um_p_idx = float_indices[1]
                            unit_p_idx = float_indices[2]
                            cost = float(parts[um_p_idx].replace(',', ''))
                            
                            # PACK is usually the integer before the Unit Price
                            # But sometimes there is a multiplier "1" in between
                            
                            # Look immediately left of Unit Price
                            search_idx = unit_p_idx - 1
                            pack = 1
                            
                            # Consume any "1"s or "T"s between Pack and Unit Price
                            while search_idx > 1:
                                tok = parts[search_idx]
                                if tok.isdigit() and int(tok) > 1:
                                    # Found the pack!
                                    pack = int(tok)
                                    # Everything before this pack (and after Item#) is description
                                    desc_end = search_idx
                                    
                                    # BUT: We need to handle the "UM" text (PK, CS) which comes BEFORE pack
                                    # Check left one more time for "PK"
                                    if parts[search_idx-1] in ['PK', 'CS', 'EA', 'DZ']:
                                        desc_end = search_idx - 1
                                    
                                    break
                                elif tok in ['PK', 'CS', 'EA', 'DZ']:
                                    # Found the Unit Measure, Pack must be to the right? 
                                    # Wait, usually it is: PK 24
                                    # If we hit 'PK', the number to the RIGHT is pack. 
                                    # But we are scanning left.
                                    # If we hit PK at search_idx, then Pack was search_idx+1 (which we just passed)
                                    if parts[search_idx+1].isdigit():
                                         pack = int(parts[search_idx+1])
                                         desc_end = search_idx
                                         break
                                search_idx -= 1
                            
                            # Clean Description
                            # Everything from parts[1] to desc_end
                            raw_desc_parts = parts[1:desc_end]
                            
                            # Filter out specific artifacts like standalone "T" (Tax flag) or "1"
                            clean_desc_parts = []
                            for p in raw_desc_parts:
                                if p not in ['T', '1', 'N']: # Common noise flags
                                    clean_desc_parts.append(p)
                                    
                            description = " ".join(clean_desc_parts)

                            items.append({
                                'ITEM': item_num,
                                'DESCRIPTION': description,
                                'PACK': pack,
                                'COST': cost
                            })

                    except Exception:
                        continue

        return items
