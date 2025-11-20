Unified Invoice Processor (Streamlit)

One app to normalize vendor invoices (Unified/SVMERCH, Southern Glazer’s, Nevada Beverage, Breakthru), reconcile against your Master and Pricebook, and export clean update files for your POS.

✨ What this app does

Parses raw invoices (XLS/XLSX/CSV/PDF depending on vendor) into a normalized invoice_items table:
UPC • Item Name • Cost • Cases (plus vendor-specific extras when needed).

Updates your Master workbook (adds Cases, Total = Pack × Cases, updates Cost $/¢) and lists:

Cost Changes (items whose cost changed),

Not in Master (invoice items missing from Master),

Missing Pack (invoice items added where Pack == 0 in Master).

Builds a POS update CSV (keeps Pricebook’s columns) for just the items on the invoice:

addstock = Master Total

cost_cents = Master Cost ¢

Keeps leading zeros in UPCs, removes duplicates, and applies vendor-specific rules.

This README matches the logic implemented in your current parsers/ and utils.py.

📦 Supported vendors & file types
Vendor	Upload Type(s)	Notes
Unified (SVMERCH)	XLS/XLSX/CSV	Uses Item UPC; normalizes SVMERCH’s odd UPCs (extra leading zeros + chopped final digit). Ignores Case Qty = 0. Keeps latest invoice per UPC. Has an ignore list.
Southern Glazer’s	PDF (preferred) or CSV/XLSX as plain-text fallback	Extracts order/delivered, finds nearby UPC: lines, uses Unit Net Amount as Cost, preserves invoice order & leading zeros. Skips non-merch like Delivery Charge.
Nevada Beverage	PDF only	Strict line parser: ITEM#(5–6 digits) QTY DESCRIPTION … UPC … D.PRICE …. QTY immediately after ITEM# is Cases; D.PRICE is first non-zero price after UPC. Skips QTY=0. Preserves order. Has table/word-grid fallbacks.
Breakthru	CSV only	Cost per case = Net Value at Header Level / Quantity. Normalizes UPC (may be blank). Also outputs Item Number to enable app-level fallback.
🧠 Key normalization & matching rules
UPC handling

Unified invoices: normalize_invoice_upc(raw) → take rightmost 11 digits + computed check digit (UPC-A). Fixes SVMERCH’s “extra zeros + missing last digit.”

POS/Pricebook: normalize_pos_upc(raw) → canonical 12-digit UPC-A (preserve leading zeros).

Southern/Nevada/Breakthru: keep 12-digit UPC (preserve leading zeros); if 13 digits starting with 0, drop the leading 0; if longer, use rightmost 12; if shorter, left-pad zeros. (See each parser.)

Duplicate handling & ignores (Unified)

Ignore Case Qty = 0 lines (did not arrive).

Deduplicate by UPC, keeping latest invoice date.

Ignore list: 000000000000, 003760010302, 023700052551.

Southern Glazer’s specifics

Cases = delivered (from ORD/DLV, take delivered side; e.g., 12/8 → 8).

Cost = the item’s Unit Net Amount.

UPC: read from the nearest following line for that item.

Nevada Beverage specifics

Each line begins with 5–6 digit ITEM#, the next integer is Cases.

Cost = first non-zero price after the UPC on the same line (D.PRICE).

Out of stock (QTY=0) is skipped.

Breakthru specifics

Cost per case = Net Value at Header Level / Quantity.

If UPC Number(Each) is blank, the app tries Item Number → Master.Invoice UPC → Master.Full Barcode to emit a download-only UPC, or inserts Item Number in the UPC column for you to fix in Master later (parser includes Item Number).

📂 Inputs

Master workbook (XLSX): columns include Full Barcode, Invoice UPC, 0, Name, Size, Pack, Cases, Total, Cost $, Cost ¢, Company. The app updates:

Cases (from invoice),

Total = Pack × Cases,

Cost $ (from invoice),

Cost ¢ (pennies from Cost $).

Pricebook CSV: includes Upc, addstock, cost_cents (plus your other columns). App updates:

addstock = Master Total,

cost_cents = Master Cost ¢.

Pricebook update keeps all Pricebook columns, but filters to items found on the invoice for Southern/Nevada/Breakthru. Unified has its own long-standing rules/UI.

📤 Outputs

Per vendor run:

*_invoice_items.csv – in invoice order, UPC written as text (leading zeros preserved).

Updated_Master.xlsx – Master with Cases/Total/Cost updated for items on the invoice.

POS_Update.csv – Pricebook rows for just the invoice items, with addstock and cost_cents updated.

Pricebook_Missing.csv – invoice items not found in Pricebook (Full Barcode ↔ Upc).

Cost_Changes.csv – diffs for items whose Cost $ changed vs Master.

Missing_Pack.csv – items added (Cases > 0) where Master Pack == 0.

🛠️ How it’s built

Parsers module:

unified_parser.py (SVMERCH) – header sniff, case-qty filter, invoice date, UPC normalization.

southern_glazers.py – PDF text walker with UPC proximity & price-triplet parse.

nevada_beverage.py – strict ITEM#/QTY line regex + robust fallbacks.

breakthru.py – CSV normalizer with cost per case and Item Number passthrough.

utils.py – UPC math, column finders, sanitizers, ignore list, etc.

base.py – base interface & shared schema notes.

parsers/__init__.py – exposes available parsers to the app.

▶️ Running locally

Requirements: Python 3.11, plus packages in requirements.txt (Streamlit Cloud also uses 3.11 via runtime.txt).

# in project root
python -m venv .venv
# Windows PowerShell:
. .venv/Scripts/Activate.ps1
# macOS/Linux:
source .venv/bin/activate

python -m pip install --upgrade pip
pip install -r requirements.txt

streamlit run app.py


Open http://localhost:8501

If PowerShell blocks activation: powershell -ExecutionPolicy Bypass -NoProfile.

☁️ Deploying to Streamlit Cloud

Push this repo to GitHub.

Create a new Streamlit app from the repo, set Python version to 3.11 (matches runtime.txt).

Streamlit installs from requirements.txt and runs app.py.

🧪 How to use the app

Pick the vendor tab.

Upload Master (XLSX) and Pricebook (CSV) once.

Upload one or more invoice files for that vendor.

Click Process. Wait for the preview tables, then download the files you need.

You can download multiple outputs without re-processing (state is preserved).

Unified has its own UI & rules; the other vendors follow the normalized flow above.

🔍 Troubleshooting

Leading zeros “missing” in CSV viewer → viewers often coerce to numbers; we write UPC as text. Import as Text to see zeros.

addstock is 0 in POS_Update → Master.Full Barcode ↔ Pricebook.Upc didn’t match. Confirm normalization and that the item exists in the Pricebook.

Item not in Master → see Not_in_Master.csv. For Breakthru rows missing UPC, use Item Number fallback to populate Invoice UPC → Full Barcode, then re-run.

No rows parsed (PDF) → ensure it’s an invoice (not a statement). For NV, line should look like ITEM# QTY DESC … UPC … D.PRICE ….

🧩 Extending to new vendors

Create parsers/<vendor>.py with .parse(uploaded_file) -> pandas.DataFrame returning [UPC, Item Name, Cost, Cases] (plus extras if needed).

Add the class to parsers/__init__.py and wire a new tab in app.py (copy an existing tab).

Reuse helpers in utils.py (UPC normalization, column finding, sanitizing).
