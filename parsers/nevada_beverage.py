# parsers/nevada_beverage.py
import re
import pandas as pd

def _normalize_upc_keep_zeros(u: str) -> str:
    digits = re.sub(r"\D", "", str(u))
    if len(digits) == 13 and digits.startswith("0"):
        digits = digits[1:]
    if len(digits) > 12:
        digits = digits[-12:]
    if len(digits) < 12:
        digits = digits.zfill(12)
    return digits

class NevadaBeverageParser:
    """
    Parse Nevada Beverage invoices (PDF/CSV/XLSX) into:
      [UPC, Item Name, Cost, Cases]
    Rules:
      - Find header line that includes 'ITEM#' and 'QTY' and 'U.P.C.' (case-insensitive).
      - Extract rows until a totals/summary line appears.
      - Cases = QTY (int).
      - Cost = last numeric on the item line (unit/case net commonly sits at end).
      - Preserve invoice order and UPC leading zeros.
      - Size column intentionally ignored per prior instruction.
    """

    name = "Nevada Beverage"

    def _is_pdf(self, f) -> bool:
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
        # Read
        try:
            if self._is_pdf(uploaded_file):
                lines = self._read_lines_pdf(uploaded_file)
            else:
                lines = self._read_lines_table(uploaded_file)
        except Exception:
            return pd.DataFrame(columns=["UPC","Item Name","Cost","Cases"])

        # find header row index
        hdr_idx = -1
        for idx, ln in enumerate(lines):
            l = ln.lower()
            if ("item#" in l or "item #" in l) and "qty" in l and ("u.p.c" in l or "upc" in l):
                hdr_idx = idx
                break
        if hdr_idx == -1:
            # fallback: parse all lines as potential items (best-effort)
            start_idx = 0
        else:
            start_idx = hdr_idx + 1

        out = []
        order_idx = 0
        stop_terms = ("subtotal", "total", "payment", "summary", "tax", "thank you")

        for ln in lines[start_idx:]:
            low = ln.lower()
            if any(t in low for t in stop_terms):
                break

            # Extract UPC: prefer an explicit 12-13 digit chunk or something after 'upc:'
            upc = None
            if "upc:" in low:
                try:
                    upc_raw = ln.split("UPC:", 1)[1]
                except Exception:
                    try:
                        upc_raw = ln.split("upc:", 1)[1]
                    except Exception:
                        upc_raw = ""
                upc = _normalize_upc_keep_zeros(upc_raw)
            else:
                # look for a long digit run (>= 11) as candidate UPC
                cand = re.findall(r"\d{11,14}", ln)
                if cand:
                    upc = _normalize_upc_keep_zeros(cand[-1])

            if not upc:
                continue

            # Extract QTY (cases)
            qty = 0
            # try "... ITEM# <id>  QTY <num>  DESCRIPTION ..." or any ' qty ' pattern
            m_qty = re.search(r"\bqty\b\s*[:\-]?\s*(\d+)", low)
            if m_qty:
                qty = int(m_qty.group(1))
            else:
                # heuristic: look for an isolated small int near start
                ints = [int(x) for x in re.findall(r"\b\d+\b", ln)]
                if ints:
                    qty = ints[0]

            # Extract name (description): remove obvious tokens
            name = ln
            name = re.sub(r"\b(item#|item #)\b.*?\bqty\b", "", name, flags=re.I)
            name = re.sub(r"\bqty\b\s*[:\-]?\s*\d+", "", name, flags=re.I)
            name = re.sub(r"UPC:\s*[\d\-\s]+", "", name, flags=re.I)
            # strip trailing numeric cluster (prices)
            tail_nums = re.findall(r"[0-9]+(?:\.[0-9]+)?", name)
            if tail_nums:
                last_num = tail_nums[-1]
            else:
                last_num = None
            # Cost: take last numeric token on line (typical NV formatting)
            cost = None
            nums_all = re.findall(r"[0-9]+(?:\.[0-9]+)?", ln)
            if nums_all:
                try:
                    cost = float(nums_all[-1].replace(",", ""))
                except Exception:
                    cost = None

            # refine name: drop all trailing numbers
            name = re.sub(r"\s*[0-9\.,]+\s*$", "", name).strip()

            if upc and cost is not None and qty >= 0:
                out.append({
                    "_order": order_idx,
                    "UPC": upc,
                    "Item Name": name,
                    "Cost": float(cost),
                    "Cases": int(qty)
                })
                order_idx += 1

        if not out:
            return pd.DataFrame(columns=["UPC","Item Name","Cost","Cases"])

        df = pd.DataFrame(out)
        df = df.sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)
        df["UPC"] = df["UPC"].astype(str)
        return df
