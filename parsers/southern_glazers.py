# parsers/southern_glazers.py
import re
import pandas as pd

def _normalize_upc_keep_zeros(u: str) -> str:
    """
    Normalize to 12-digit UPC-A while preserving leading zeros:
    - Strip non-digits.
    - If 13 digits starting with '0', drop the first digit (EAN-13 UPC-A).
    - If >12, take RIGHTMOST 12 digits.
    - If <12, left-pad with zeros.
    """
    digits = re.sub(r"\D", "", str(u))
    if len(digits) == 13 and digits.startswith("0"):
        digits = digits[1:]
    if len(digits) > 12:
        digits = digits[-12:]
    if len(digits) < 12:
        digits = digits.zfill(12)
    return digits

class SouthernGlazersParser:
    """
    Parse Southern Glazer's PDF/XLSX/CSV into rows with columns:
      [UPC, Item Name, Cost, Cases]
    - Cost = Unit Net Amount (3rd number in price triplet on item line)
    - Cases = delivered (second number in ORD/DLV; e.g., 12/8 -> 8)
    - Preserves invoice order for the output CSV
    """

    name = "Southern Glazer's"

    def _is_pdf(self, f) -> bool:
        # Best-effort PDF sniff
        try:
            mt = (getattr(f, "type", "") or "").lower()
            if "pdf" in mt:
                return True
        except Exception:
            pass
        pos = None
        try:
            pos = f.tell()
            f.seek(0)
            head = f.read(5)
            return isinstance(head, (bytes, bytearray)) and head.startswith(b"%PDF")
        except Exception:
            return False
        finally:
            try:
                f.seek(0 if pos is None else pos)
            except Exception:
                pass

    def _read_lines_pdf(self, f):
        import pdfplumber
        f.seek(0)
        lines = []
        with pdfplumber.open(f) as pdf:
            for page in pdf.pages:
                txt = page.extract_text() or ""
                for ln in txt.splitlines():
                    ln = ln.strip()
                    if ln:
                        lines.append(ln)
        return lines

    def _read_lines_table(self, f):
        # Fallback if user uploads CSV/XLS(X)
        name = (getattr(f, "name", "") or "").lower()
        f.seek(0)
        if name.endswith(".csv"):
            df = pd.read_csv(f, header=None, dtype=str, keep_default_na=False)
        else:
            df = pd.read_excel(f, header=None, dtype=str)
        df = df.fillna("")
        lines = df.apply(lambda r: " ".join([c for c in r if str(c).strip() != ""]).strip(), axis=1)
        return [x for x in lines.tolist() if x]

    def parse(self, uploaded_file) -> pd.DataFrame:
        # Get text lines
        try:
            if self._is_pdf(uploaded_file):
                lines = self._read_lines_pdf(uploaded_file)
            else:
                lines = self._read_lines_table(uploaded_file)
        except Exception:
            return pd.DataFrame(columns=["UPC", "Item Name", "Cost", "Cases"])

        item_re = re.compile(
            r"^(\d+)\s*/\s*(\d+)\s+(.+?)\s+([0-9\.,]+\s+[0-9\.,]+\s+[0-9\.,]+)\s+[0-9\.,]+\s+[0-9\.,]+$"
        )

        out = []
        order_idx = 0
        i = 0
        n = len(lines)

        while i < n:
            ln = lines[i]
            m = item_re.match(ln)
            if m:
                ordered = int(m.group(1))
                delivered = int(m.group(2))
                name = m.group(3).strip()
                triplet = m.group(4)
                nums = re.findall(r"[0-9]+(?:\.[0-9]+)?", triplet)
                unit_net = float(nums[2]) if len(nums) >= 3 else None

                # Find UPC nearby
                upc = None
                for j in range(1, 6):
                    k = i + j
                    if k < n and lines[k].upper().startswith("UPC:"):
                        upc_raw = lines[k].split(":", 1)[1]
                        upc = _normalize_upc_keep_zeros(upc_raw)
                        break

                # Drop non-merch like Delivery Charge
                if upc and unit_net is not None and "DELIVERY CHARGE" not in name.upper():
                    out.append({
                        "_order": order_idx,
                        "UPC": upc,
                        "Item Name": name,
                        "Cost": unit_net,
                        "Cases": delivered
                    })
                    order_idx += 1
            i += 1

        if not out:
            return pd.DataFrame(columns=["UPC", "Item Name", "Cost", "Cases"])

        df = pd.DataFrame(out)
        df = df.sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)
        # Ensure textual UPC so leading zeros persist in CSV
        df["UPC"] = df["UPC"].astype(str)
        return df
