# parsers/costco.py
import re
import pandas as pd
import numpy as np

class CostcoParser:
    name = "Costco (Text Paste)"

    def parse(self, text_input: str) -> pd.DataFrame:
        """
        Parses pasted Costco receipt text.
        Extracts: Item Number, Description, Total Price.
        Ignores lines ending in '-' (discounts).
        """
        if not text_input:
            return pd.DataFrame()

        lines = text_input.strip().splitlines()
        rows = []

        # Regex to capture:
        # Optional 'E' at start
        # Item Number (digits)
        # Description (text)
        # Price (number.decimals)
        # Flag (N or Y) at end
        # Example: E  428051  WHOLE WHEAT  17.97 N
        # Example: 1988113  FABULOSO  32.97 Y
        line_pattern = re.compile(
            r"^\s*(?:E\s+)?(?P<item>\d+)\s+(?P<desc>.+?)\s+(?P<price>\d+\.\d{2})\s+[NY]\s*$",
            re.IGNORECASE
        )

        for line in lines:
            line = line.strip()
            # Skip empty lines
            if not line:
                continue
            
            # Skip discounts (lines ending in -)
            if line.endswith("-"):
                continue

            match = line_pattern.match(line)
            if match:
                d = match.groupdict()
                rows.append({
                    "Item Number": d["item"],
                    "Item Name": d["desc"].strip(),
                    "Receipt Price": float(d["price"])
                })
        
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        # Initialize Quantity to 1 (User will edit this later)
        df["Quantity"] = 1
        return df
