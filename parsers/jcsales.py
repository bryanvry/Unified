from __future__ import annotations

from typing import Optional, Tuple
import re

import numpy as np
import pandas as pd
import pdfplumber

WANT_COLS = ["ITEM", "DESCRIPTION", "PACK", "COST", "UNIT"]
_MONEY = r"\$?\d[\d,]*\.\d{2}"
_MONEY_RE = re.compile(_MONEY)
_HEAD_RE = re.compile(
    r"^\s*(?P<linenum>\d+)\s+(?:[A-Z]\s+)?(?P<item>\d{4,8})\s+(?P<rest>.+?)\s*$",
    re.IGNORECASE,
)

_LINE_PATTERNS = [
    re.compile(
        rf"""
        ^\s*
        (?P<linenum>\d+)\s+
        (?:[A-Z]\s+)?
        (?P<item>\d{{4,8}})\s+
        (?P<desc>.+?)\s+
        (?P<rqty>\d+)\s+
        (?P<sqty>\d+)\s+
        (?P<um>[A-Z]{{1,4}})\s+
        (?:(?P<pack>\d+)\s+)?
        (?P<unit>{_MONEY})\s+
        (?P<cost>{_MONEY})\s+
        (?P<ext>{_MONEY})
        (?:\s+(?P<pack2>\d+))?
        \s*$
        """,
        re.IGNORECASE | re.VERBOSE,
    ),
    re.compile(
        rf"""
        ^\s*
        (?P<linenum>\d+)\s+
        (?:[A-Z]\s+)?
        (?P<item>\d{{4,8}})\s+
        (?P<desc>.+?)\s+
        (?P<rqty>\d+)\s+
        (?P<sqty>\d+)\s+
        (?P<um>[A-Z]{{1,4}})\s+
        (?P<pack>\d+)\s+
        (?P<total>\d+)\s+
        (?P<unit>{_MONEY})\s+
        (?P<cost>{_MONEY})\s+
        (?P<ext>{_MONEY})
        \s*$
        """,
        re.IGNORECASE | re.VERBOSE,
    ),
    re.compile(
        rf"""
        ^\s*
        (?P<linenum>\d+)\s+
        (?:[A-Z]\s+)?
        (?P<item>\d{{4,8}})\s+
        (?P<desc>.+?)\s+
        (?P<pack>\d+)\s+
        (?P<total>\d+)\s+
        (?P<unit>{_MONEY})\s+
        (?P<cost>{_MONEY})\s+
        (?P<ext>{_MONEY})
        \s*$
        """,
        re.IGNORECASE | re.VERBOSE,
    ),
]

_DESC_PATTERNS = [
    re.compile(r"^(?P<desc>.+?)\s+\d+\s+\d+\s+[A-Z]{1,4}\s+(?P<pack>\d+)$", re.IGNORECASE),
    re.compile(r"^(?P<desc>.+?)\s+\d+\s+\d+\s+[A-Z]{1,4}$", re.IGNORECASE),
    re.compile(r"^(?P<desc>.+?)\s+\d+\s+[A-Z]{1,4}\s+(?P<pack>\d+)$", re.IGNORECASE),
    re.compile(r"^(?P<desc>.+?)\s+\d+\s+[A-Z]{1,4}$", re.IGNORECASE),
    re.compile(r"^(?P<desc>.+?)\s+(?P<pack>\d+)\s+\d+$", re.IGNORECASE),
]


def _to_float(value) -> float:
    if value is None:
        return np.nan

    text = str(value).replace("$", "").replace(",", "").strip()
    try:
        return float(text)
    except Exception:
        return np.nan


def _to_int(value, default: int = 0) -> int:
    if value is None:
        return default

    text = str(value).replace(",", "").strip()
    if not text:
        return default

    try:
        return int(round(float(text)))
    except Exception:
        return default


def _normalize_text(text: str) -> str:
    text = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\u00a0", " ").replace("\x00", " ")
    text = re.sub(r"(?<=[A-Za-z])(?=\d)", " ", text)
    text = re.sub(r"(?<=\d)(?=[A-Za-z]{1,4}\b)", " ", text)
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def _extract_text(uploaded_file) -> str:
    start_pos = None
    try:
        start_pos = uploaded_file.tell()
    except Exception:
        start_pos = None

    try:
        uploaded_file.seek(0)
        pages = []
        with pdfplumber.open(uploaded_file) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text() or ""
                if page_text.strip():
                    pages.append(page_text)
        return "\n".join(pages)
    finally:
        try:
            uploaded_file.seek(0 if start_pos is None else start_pos)
        except Exception:
            pass


def _find_invoice_number(text: str) -> Optional[str]:
    patterns = [
        re.compile(r"\b(OSI\d{5,})\b", re.IGNORECASE),
        re.compile(r"\bInvoice(?:\s*No\.?|\s*#)?\s*[:#]?\s*([A-Z0-9-]{5,})\b", re.IGNORECASE),
    ]
    for pattern in patterns:
        match = pattern.search(text)
        if match:
            return match.group(1).upper()
    return None


def _infer_pack(unit: float, cost: float) -> int:
    if np.isnan(unit) or np.isnan(cost) or unit <= 0 or cost <= 0:
        return 1

    ratio = cost / unit
    rounded = int(round(ratio))
    if 1 < rounded <= 96 and abs(ratio - rounded) <= 0.2:
        return rounded
    return 1


def _clean_prices(pack: int, unit: float, cost: float) -> Tuple[int, float, float]:
    if not np.isnan(unit) and not np.isnan(cost) and cost < unit:
        unit, cost = cost, unit

    if pack <= 0:
        pack = _infer_pack(unit, cost)
    if pack <= 0:
        pack = 1

    return pack, float(cost), float(unit)


def _build_row(item: str, desc: str, pack: int, unit: float, cost: float, order_idx: int):
    if np.isnan(unit) or np.isnan(cost):
        return None

    pack, cost, unit = _clean_prices(pack, unit, cost)
    desc = re.sub(r"\s+", " ", str(desc)).strip(" -")
    if not item or not desc:
        return None

    return {
        "ITEM": str(item).strip(),
        "DESCRIPTION": desc,
        "PACK": int(pack),
        "COST": float(cost),
        "UNIT": float(unit),
        "_order": order_idx,
    }


def _parse_with_patterns(line: str, order_idx: int):
    for pattern in _LINE_PATTERNS:
        match = pattern.match(line)
        if not match:
            continue

        data = match.groupdict()
        pack = max(_to_int(data.get("pack")), _to_int(data.get("pack2")))
        return _build_row(
            item=data["item"],
            desc=data["desc"],
            pack=pack,
            unit=_to_float(data.get("unit")),
            cost=_to_float(data.get("cost")),
            order_idx=order_idx,
        )
    return None


def _split_desc_and_pack(pre_money: str, post_money: str) -> Tuple[str, int]:
    for pattern in _DESC_PATTERNS:
        match = pattern.match(pre_money)
        if match:
            data = match.groupdict()
            return data["desc"].strip(), _to_int(data.get("pack"))

    pack = _to_int(post_money)
    cleaned = re.sub(
        r"\s+\d+\s+\d+\s+[A-Z]{1,4}(?:\s+\d+)?$",
        "",
        pre_money,
        flags=re.IGNORECASE,
    ).strip()
    return (cleaned or pre_money.strip()), pack


def _parse_with_fallback(line: str, order_idx: int):
    head_match = _HEAD_RE.match(line)
    if not head_match:
        return None

    rest = head_match.group("rest")
    money_matches = list(_MONEY_RE.finditer(rest))
    if len(money_matches) < 3:
        return None

    unit = _to_float(money_matches[-3].group(0))
    cost = _to_float(money_matches[-2].group(0))
    pre_money = rest[: money_matches[-3].start()].strip()
    post_money = rest[money_matches[-1].end() :].strip()
    desc, pack = _split_desc_and_pack(pre_money, post_money)

    return _build_row(
        item=head_match.group("item"),
        desc=desc,
        pack=pack,
        unit=unit,
        cost=cost,
        order_idx=order_idx,
    )


class JCSalesParser:
    name = "JC Sales"

    def parse(self, uploaded_file) -> Tuple[pd.DataFrame, Optional[str]]:
        raw_text = _extract_text(uploaded_file)
        invoice_number = _find_invoice_number(raw_text)
        clean_text = _normalize_text(raw_text)

        rows = []
        for order_idx, line in enumerate(clean_text.splitlines()):
            row = _parse_with_patterns(line, order_idx)
            if row is None:
                row = _parse_with_fallback(line, order_idx)
            if row is not None:
                rows.append(row)

        if not rows:
            return pd.DataFrame(columns=WANT_COLS), invoice_number

        df = pd.DataFrame(rows).sort_values("_order").reset_index(drop=True)
        return df[WANT_COLS], invoice_number
