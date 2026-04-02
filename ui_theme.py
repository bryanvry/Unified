from __future__ import annotations

import base64
from html import escape
from pathlib import Path

import streamlit as st


ROOT = Path(__file__).resolve().parent
LOGO_CANDIDATES = [
    ROOT / "logo.png",
    ROOT / "assets" / "logo.png",
    ROOT / "assets" / "brand-logo.png",
]


def get_brand_logo_path() -> Path | None:
    for path in LOGO_CANDIDATES:
        if path.exists():
            return path
    return None


def get_brand_logo_data_uri() -> str | None:
    logo_path = get_brand_logo_path()
    if logo_path is None:
        return None

    encoded = base64.b64encode(logo_path.read_bytes()).decode("ascii")
    suffix = logo_path.suffix.lower().lstrip(".") or "png"
    mime = "jpeg" if suffix == "jpg" else suffix
    return f"data:image/{mime};base64,{encoded}"


def render_sidebar_navigation(page_options: list[str], store_options: list[str]) -> tuple[str, str]:
    sidebar = st.sidebar
    logo_uri = get_brand_logo_data_uri()

    if logo_uri is not None:
        sidebar.markdown(
            f"""
            <div class="sidebar-brand-card">
                <img src="{logo_uri}" alt="La Favorita Market" />
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        sidebar.markdown(
            """
            <div class="sidebar-brand-card sidebar-brand-fallback">
                <span>LFM Process</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    sidebar.markdown('<div class="sidebar-nav-label">Navigation</div>', unsafe_allow_html=True)
    current_page = sidebar.radio(
        "Navigation",
        page_options,
        key="main_navigation",
        label_visibility="collapsed",
    )
    sidebar.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)
    sidebar.markdown('<div class="sidebar-nav-label">Store</div>', unsafe_allow_html=True)
    selected_store = sidebar.segmented_control(
        "Store",
        store_options,
        default=store_options[0],
        key="store_selector",
        label_visibility="collapsed",
        width="stretch",
    )
    sidebar.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)
    return current_page or page_options[0], selected_store or store_options[0]


def render_workspace_header(title: str, subtitle: str, chips: list[tuple[str, str]]) -> None:
    visible_chips = [(label, value) for label, value in chips if value]
    title_col, chip_col = st.columns([1.35, 0.95], gap="large")

    with title_col:
        st.markdown(
            f"""
            <div class="page-header-main">
                <div class="page-kicker">LFM Process</div>
                <h1 class="page-title">{escape(title)}</h1>
                <p class="page-subtitle">{escape(subtitle)}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with chip_col:
        if visible_chips:
            label, value = visible_chips[0]
            st.markdown(
                f"""
                <div class="header-chip header-chip-compact">
                    <div class="header-chip-label">{escape(label)}</div>
                    <div class="header-chip-value">{escape(value)}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def apply_brand_theme() -> None:
    style = """
<style>
:root {
    --bg: #f5f7fb;
    --bg-soft: #eef1f5;
    --surface: rgba(255, 255, 255, 0.94);
    --surface-strong: rgba(255, 255, 255, 0.98);
    --surface-muted: rgba(240, 243, 248, 0.92);
    --surface-deep: rgba(231, 236, 242, 0.96);
    --ink: #171a1f;
    --ink-soft: #2f3747;
    --muted: #667085;
    --muted-soft: #8a93a5;
    --line: rgba(18, 24, 40, 0.08);
    --line-soft: rgba(18, 24, 40, 0.05);
    --red: #b81120;
    --red-strong: #dc3135;
    --red-soft: rgba(184, 17, 32, 0.10);
    --olive: #627d3a;
    --gold: #ca9848;
    --success-soft: rgba(98, 125, 58, 0.12);
    --warning-soft: rgba(202, 152, 72, 0.16);
    --danger-soft: rgba(184, 17, 32, 0.12);
    --shadow-lg: 0 18px 48px rgba(15, 23, 42, 0.08);
    --shadow-sm: 0 10px 26px rgba(15, 23, 42, 0.05);
    --radius-xl: 28px;
    --radius-lg: 22px;
    --radius-md: 16px;
    --radius-pill: 999px;
}

html, body, [class*="css"] {
    font-family: Inter, "Segoe UI", "Helvetica Neue", Arial, sans-serif;
    color: var(--ink);
}

h1, h2, h3, h4, h5, h6 {
    color: var(--ink);
    font-family: Inter, "Segoe UI", "Helvetica Neue", Arial, sans-serif !important;
    letter-spacing: -0.03em;
    font-weight: 700 !important;
}

code {
    font-family: "Cascadia Code", "SFMono-Regular", Consolas, monospace;
}

.stApp {
    background:
        radial-gradient(circle at top left, rgba(184, 17, 32, 0.08), transparent 20%),
        radial-gradient(circle at top right, rgba(202, 152, 72, 0.10), transparent 18%),
        linear-gradient(180deg, #f8fafc 0%, #f3f6fa 48%, #eef2f6 100%);
}

[data-testid="stHeader"] {
    background: transparent;
}

.block-container {
    max-width: 1480px;
    padding-top: 1.15rem;
    padding-bottom: 2.4rem;
}

[data-testid="stSidebar"] {
    background:
        linear-gradient(180deg, rgba(247, 249, 252, 0.88), rgba(238, 242, 247, 0.94));
    border-right: 1px solid var(--line);
    box-shadow: 18px 0 42px rgba(15, 23, 42, 0.06);
    backdrop-filter: blur(22px);
    color: var(--ink);
}

[data-testid="stSidebarNav"] {
    display: none;
}

[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] {
    padding-top: 0.55rem;
}

[data-testid="stSidebar"] label,
[data-testid="stSidebar"] .stCaption,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3,
[data-testid="stSidebar"] h4 {
    color: var(--ink-soft) !important;
}

[data-testid="stSidebar"] .stCaption {
    color: var(--muted) !important;
}

[data-testid="stSidebar"] .sidebar-brand-card {
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 94px;
    margin: 0 0 1rem 0;
    padding: 0.9rem 1rem;
    border-radius: 24px;
    border: 1px solid var(--line);
    background: linear-gradient(180deg, rgba(255, 255, 255, 0.99), rgba(243, 246, 250, 0.96));
    box-shadow:
        inset 0 1px 0 rgba(255, 255, 255, 0.94),
        0 12px 30px rgba(15, 23, 42, 0.08);
}

[data-testid="stSidebar"] .sidebar-brand-card img {
    display: block;
    width: 100%;
    max-width: 210px;
    height: auto;
}

[data-testid="stSidebar"] .sidebar-brand-fallback span {
    color: var(--ink);
    font-size: 1.05rem;
    font-weight: 800;
    letter-spacing: -0.03em;
}

[data-testid="stSidebar"] .sidebar-nav-label {
    margin: 0.15rem 0 0.55rem 0.1rem;
    color: var(--muted);
    font-size: 0.72rem;
    font-weight: 800;
    letter-spacing: 0.12em;
    text-transform: uppercase;
}

[data-testid="stSidebar"] div[data-testid^="stPageLink"] {
    margin-bottom: 0.38rem;
}

[data-testid="stSidebar"] div[data-testid^="stPageLink"] a {
    display: flex;
    align-items: center;
    width: 100%;
    min-height: 3rem;
    padding: 0.72rem 0.88rem;
    border-radius: 18px;
    border: 1px solid transparent;
    background: rgba(255, 255, 255, 0.72);
    color: var(--ink-soft) !important;
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.84);
    transition:
        background 0.14s ease,
        border-color 0.14s ease,
        color 0.14s ease,
        transform 0.14s ease,
        box-shadow 0.14s ease;
}

[data-testid="stSidebar"] div[data-testid^="stPageLink"] a:hover {
    background: rgba(255, 255, 255, 0.94);
    border-color: var(--line-soft);
    color: var(--ink) !important;
    box-shadow:
        inset 0 1px 0 rgba(255, 255, 255, 0.92),
        0 10px 22px rgba(15, 23, 42, 0.06);
    transform: translateY(-1px);
}

[data-testid="stSidebar"] div[data-testid^="stPageLink"] a[aria-current="page"] {
    background: rgba(255, 255, 255, 0.98);
    border-color: rgba(184, 17, 32, 0.14);
    color: var(--red) !important;
    box-shadow:
        inset 3px 0 0 var(--red),
        0 12px 24px rgba(15, 23, 42, 0.06);
}

[data-testid="stSidebar"] .stRadio [role="radiogroup"] {
    display: flex;
    flex-direction: column;
    gap: 0.38rem;
}

[data-testid="stSidebar"] .stRadio [role="radiogroup"] label {
    display: flex;
    align-items: center;
    min-height: 3rem;
    margin: 0;
    padding: 0.72rem 0.88rem !important;
    border-radius: 18px;
    border: 1px solid transparent;
    background: rgba(255, 255, 255, 0.72);
    color: var(--ink-soft) !important;
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.84);
    transition:
        background 0.14s ease,
        border-color 0.14s ease,
        color 0.14s ease,
        transform 0.14s ease,
        box-shadow 0.14s ease;
}

[data-testid="stSidebar"] .stRadio [role="radiogroup"] label:hover {
    background: rgba(255, 255, 255, 0.94);
    border-color: var(--line-soft);
    color: var(--ink) !important;
    box-shadow:
        inset 0 1px 0 rgba(255, 255, 255, 0.92),
        0 10px 22px rgba(15, 23, 42, 0.06);
    transform: translateY(-1px);
}

[data-testid="stSidebar"] .stRadio [role="radiogroup"] label:has(input:checked) {
    background: linear-gradient(90deg, rgba(184, 17, 32, 0.06) 0%, rgba(255, 255, 255, 0.98) 100%);
    border-color: rgba(184, 17, 32, 0.18);
    box-shadow:
        inset 3px 0 0 var(--red),
        0 12px 24px rgba(15, 23, 42, 0.06);
}

[data-testid="stSidebar"] .stRadio [role="radiogroup"] label > div:first-child {
    display: none;
}

[data-testid="stSidebar"] .stRadio [role="radiogroup"] label p {
    margin: 0;
    color: var(--ink-soft) !important;
    font-size: 0.95rem;
    font-weight: 600;
}

[data-testid="stSidebar"] .stRadio [role="radiogroup"] label:has(input:checked) p {
    color: var(--red) !important;
}

[data-testid="stSidebar"] .sidebar-divider {
    height: 1px;
    margin: 1.05rem 0 1.15rem 0;
    background: linear-gradient(90deg, transparent, rgba(18, 24, 40, 0.14), transparent);
}

[data-testid="stSidebar"] .sidebar-session-card {
    margin: 0.2rem 0 0.8rem 0;
    border-radius: 18px;
    padding: 0.85rem 0.9rem;
    background: rgba(255, 255, 255, 0.86);
    border: 1px solid var(--line-soft);
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.88);
}

[data-testid="stSidebar"] .sidebar-session-label {
    color: var(--muted);
    font-size: 0.72rem;
    font-weight: 800;
    letter-spacing: 0.1em;
    text-transform: uppercase;
}

[data-testid="stSidebar"] .sidebar-session-user {
    margin-top: 0.34rem;
    color: var(--ink);
    font-size: 0.96rem;
    font-weight: 800;
}

[data-testid="stSidebarNav"] ul {
    gap: 0.35rem;
}

[data-testid="stSidebarNav"] li {
    margin-bottom: 0.3rem;
}

[data-testid="stSidebarNav"] a {
    border-radius: 18px;
    padding: 0.24rem 0.36rem;
    background: transparent;
    border: 1px solid transparent;
    color: var(--ink-soft) !important;
    transition: background 0.14s ease, border-color 0.14s ease, color 0.14s ease;
}

[data-testid="stSidebarNav"] a:hover {
    background: rgba(255, 255, 255, 0.78);
    border-color: var(--line-soft);
    color: var(--ink) !important;
}

[data-testid="stSidebarNav"] a[aria-current="page"] {
    background: rgba(255, 255, 255, 0.96);
    border-color: rgba(184, 17, 32, 0.12);
    color: var(--red) !important;
    box-shadow:
        inset 3px 0 0 var(--red),
        0 8px 22px rgba(15, 23, 42, 0.05);
}

[data-testid="stSidebar"] .stTextInput input,
[data-testid="stSidebar"] .stMultiSelect div[data-baseweb="select"],
[data-testid="stSidebar"] .stSelectbox div[data-baseweb="select"],
[data-testid="stSidebar"] .stNumberInput div[data-baseweb="input"] {
    background: rgba(255, 255, 255, 0.95);
    border-radius: 16px;
    border: 1px solid var(--line) !important;
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.88);
}

[data-testid="stSidebar"] [data-baseweb="tag"] {
    background: rgba(240, 244, 249, 0.98) !important;
    border-radius: 12px !important;
    border: 1px solid var(--line-soft) !important;
}

[data-testid="stSidebar"] [data-baseweb="tag"] *,
[data-testid="stSidebar"] [data-baseweb="tag"] span,
[data-testid="stSidebar"] [data-baseweb="tag"] svg {
    color: var(--ink-soft) !important;
    fill: var(--ink-soft) !important;
    -webkit-text-fill-color: var(--ink-soft) !important;
}

[data-testid="stSidebar"] .stTextInput input,
[data-testid="stSidebar"] [data-baseweb="input"] input,
[data-testid="stSidebar"] [data-baseweb="base-input"] input,
[data-testid="stSidebar"] [data-baseweb="select"] input,
[data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] span,
[data-testid="stSidebar"] .stMultiSelect [data-baseweb="select"] span {
    color: var(--ink) !important;
    -webkit-text-fill-color: var(--ink) !important;
    caret-color: var(--ink) !important;
}

[data-testid="stSidebar"] .stTextInput input::placeholder {
    color: var(--muted) !important;
}

[data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] svg,
[data-testid="stSidebar"] .stMultiSelect [data-baseweb="select"] svg,
div[data-baseweb="popover"] *,
div[data-baseweb="popover"] [role="option"],
[role="listbox"] * {
    color: var(--ink) !important;
    fill: var(--ink) !important;
    -webkit-text-fill-color: var(--ink) !important;
}

.page-header {
    display: grid;
    grid-template-columns: minmax(0, 1.35fr) minmax(320px, 0.95fr);
    gap: 1rem 1.2rem;
    align-items: start;
    margin: 0.1rem 0 1.4rem 0;
}

.page-header-main {
    display: flex;
    flex-direction: column;
    gap: 0.7rem;
    padding-top: 0.25rem;
}

.page-kicker,
.section-kicker,
.eyebrow {
    display: inline-flex;
    align-items: center;
    gap: 0.42rem;
    width: fit-content;
    border-radius: var(--radius-pill);
    padding: 0.42rem 0.8rem;
    font-size: 0.71rem;
    font-weight: 800;
    letter-spacing: 0.11em;
    text-transform: uppercase;
    color: var(--red);
    background: rgba(255, 255, 255, 0.88);
    border: 1px solid rgba(184, 17, 32, 0.10);
}

.page-title,
.hero-title,
.upload-title {
    margin: 0;
    font-size: clamp(2.35rem, 3.6vw, 4.1rem);
    line-height: 0.94;
    letter-spacing: -0.055em;
    color: var(--ink);
    max-width: 11ch;
}

.page-subtitle,
.hero-copy,
.upload-copy,
.surface-copy,
.panel-note,
.table-caption,
.note-bar,
.notice-bar,
.insight-card p {
    margin: 0;
    color: var(--muted);
    font-size: 0.98rem;
    line-height: 1.62;
}

.page-subtitle,
.hero-copy,
.upload-copy {
    max-width: 760px;
}

.header-meta-row,
.hero-facts,
.meta-list,
.tile .tile-stats {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 0.72rem;
}

.header-chip,
.fact-chip,
.meta-chip {
    position: relative;
    overflow: hidden;
    min-height: 92px;
    border-radius: var(--radius-lg);
    padding: 0.92rem 0.98rem;
    background: linear-gradient(180deg, rgba(255, 255, 255, 0.92), rgba(245, 248, 252, 0.94));
    box-shadow: var(--shadow-sm);
    border: 1px solid var(--line-soft);
}

.header-chip::before,
.fact-chip::before,
.meta-chip::before {
    content: "";
    position: absolute;
    inset: 0 0 auto 0;
    height: 3px;
    background: linear-gradient(90deg, var(--red) 0%, var(--gold) 52%, var(--olive) 100%);
    opacity: 0.85;
}

.header-chip-compact {
    width: min(100%, 320px);
    min-height: 0;
    margin-left: auto;
}

.header-chip-label,
.fact-label,
.meta-label,
.metric-label,
.tile .tile-stat-label {
    font-size: 0.7rem;
    font-weight: 800;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--muted);
}

.header-chip-value,
.fact-value,
.meta-value,
.tile .tile-stat-value {
    margin-top: 0.38rem;
    color: var(--ink);
    font-size: 0.98rem;
    line-height: 1.38;
    font-weight: 700;
}

.section-intro {
    margin-bottom: 1rem;
}

.section-title {
    margin: 0.22rem 0 0 0;
    font-size: 1.28rem;
    color: var(--ink);
}

.section-copy {
    margin: 0.34rem 0 0 0;
    max-width: 760px;
    color: var(--muted);
    line-height: 1.6;
}

.section-label {
    margin: 0.12rem 0 0.78rem 0;
    color: var(--ink);
    font-size: 1.14rem;
}

.surface,
.tile,
.insight-card,
.hero-shell,
.upload-shell {
    position: relative;
    overflow: hidden;
    border-radius: var(--radius-xl);
    padding: 1.05rem 1.1rem 1.08rem 1.1rem;
    background:
        linear-gradient(180deg, rgba(255, 255, 255, 0.95), rgba(246, 248, 252, 0.94));
    box-shadow: var(--shadow-sm);
    border: 1px solid var(--line-soft);
}

.auth-shell {
    display: flex;
    justify-content: center;
    margin: 0 auto 1.2rem auto;
}

.auth-card {
    width: min(100%, 700px);
}

.auth-logo-wrap {
    display: flex;
    justify-content: center;
    margin: 0.2rem auto 1rem auto;
}

.auth-logo-wrap img {
    width: min(100%, 300px);
    height: auto;
    display: block;
}

.auth-form-shell {
    max-width: 460px;
    margin: 0 auto;
}

.surface::before,
.tile::before,
.insight-card::before,
.hero-shell::before,
.upload-shell::before {
    content: "";
    position: absolute;
    inset: 0 0 auto 0;
    height: 4px;
    background: linear-gradient(90deg, var(--red) 0%, var(--gold) 52%, var(--olive) 100%);
    opacity: 0.9;
}

.tile h3,
.surface h3,
.insight-card h3 {
    margin: 0;
}

.tile .tile-kicker {
    margin-top: 0.28rem;
    color: var(--muted);
    font-size: 0.91rem;
}

.metric-card {
    position: relative;
    overflow: hidden;
    min-height: 132px;
    border-radius: var(--radius-lg);
    padding: 1rem 1rem 0.96rem 1rem;
    border: 1px solid var(--line-soft);
    background:
        linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(244, 247, 251, 0.95));
    box-shadow: var(--shadow-sm);
}

.metric-card::before {
    content: "";
    position: absolute;
    inset: 0 auto 0 0;
    width: 4px;
    background: linear-gradient(180deg, var(--gold) 0%, rgba(202, 152, 72, 0.4) 100%);
}

.metric-card.tone-success::before {
    background: var(--olive);
}

.metric-card.tone-danger::before {
    background: var(--red);
}

.metric-card.tone-muted::before {
    background: var(--gold);
}

.metric-card.tone-success {
    background:
        linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(243, 248, 239, 0.95));
}

.metric-card.tone-danger {
    background:
        linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(253, 245, 245, 0.95));
}

.metric-card.tone-muted {
    background:
        linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(251, 247, 239, 0.95));
}

.metric-value {
    margin-top: 0.42rem;
    color: var(--ink);
    font-size: 2rem;
    line-height: 1;
    font-weight: 800;
    font-variant-numeric: tabular-nums;
}

.metric-detail {
    margin-top: 0.5rem;
    color: var(--muted);
    font-size: 0.92rem;
    line-height: 1.48;
}

.pill-row {
    display: flex;
    flex-wrap: wrap;
    gap: 0.55rem;
    margin-top: 0.9rem;
}

.order-workspace-pills {
    justify-content: flex-end;
    margin-top: 0.1rem;
}

.pill {
    border-radius: var(--radius-pill);
    padding: 0.44rem 0.78rem;
    background: rgba(255, 255, 255, 0.92);
    border: 1px solid var(--line-soft);
    color: var(--ink-soft);
    font-size: 0.78rem;
    font-weight: 700;
}

.pill strong {
    color: var(--ink);
}

.pill.status-pill {
    border-color: transparent;
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.18);
}

.pill.status-pill,
.pill.status-pill strong {
    color: #ffffff;
}

.pill.status-pill.status-in-stock {
    background: linear-gradient(180deg, #738f49 0%, #627d3a 100%);
}

.pill.status-pill.status-out-of-stock {
    background: linear-gradient(180deg, #dc3135 0%, #b81120 100%);
}

.pill.status-pill.status-discontinued {
    background: linear-gradient(180deg, #d4a256 0%, #c28d3c 100%);
    color: #34240d;
}

.pill.status-pill.status-discontinued strong {
    color: #34240d;
}

.legend-row {
    display: flex;
    flex-wrap: wrap;
    gap: 0.8rem;
    margin: 0.1rem 0 0.9rem 0;
}

.legend-item {
    display: inline-flex;
    align-items: center;
    gap: 0.45rem;
    color: var(--muted);
    font-size: 0.84rem;
    font-weight: 700;
}

.legend-dot {
    width: 0.62rem;
    height: 0.62rem;
    border-radius: 50%;
    flex: 0 0 auto;
    box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.3);
}

.note-bar,
.notice-bar {
    margin: 0.85rem 0 1rem 0;
    border-radius: 20px;
    padding: 0.95rem 1rem;
    background: linear-gradient(90deg, rgba(255, 255, 255, 0.92), rgba(246, 248, 252, 0.94));
    border: 1px solid var(--line-soft);
    box-shadow: var(--shadow-sm);
}

.hero-grid {
    display: grid;
    grid-template-columns: 1.2fr 0.95fr;
    gap: 0.9rem;
    align-items: end;
}

.download-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.8rem;
    margin-bottom: 0.75rem;
}

.empty-state {
    border-radius: 22px;
    padding: 2.4rem 1.05rem;
    background: rgba(255, 255, 255, 0.72);
    border: 1px dashed rgba(18, 24, 40, 0.10);
    color: var(--muted);
    text-align: center;
    font-size: 0.95rem;
    line-height: 1.6;
}

div[data-testid="stVerticalBlockBorderWrapper"] {
    background:
        linear-gradient(180deg, rgba(255, 255, 255, 0.82), rgba(245, 248, 252, 0.78));
    border: 1px solid var(--line-soft);
    border-radius: 28px;
    box-shadow: var(--shadow-sm);
    padding: 1rem 1.05rem 1.08rem 1.05rem;
}

div[data-testid="stVerticalBlockBorderWrapper"] > div {
    gap: 0.85rem;
}

div[data-testid="stMetric"] {
    border-radius: 18px;
    border: 1px solid var(--line-soft);
    background: linear-gradient(180deg, rgba(255, 255, 255, 0.92), rgba(242, 245, 249, 0.92));
    box-shadow: none;
    padding: 0.68rem 0.82rem;
}

div[data-testid="stAlert"] {
    border-radius: 20px;
    border: 1px solid var(--line-soft);
    box-shadow: none;
}

div[data-testid="stAlert"][kind="success"] {
    background: linear-gradient(180deg, rgba(243, 249, 239, 0.95), rgba(236, 245, 230, 0.95));
}

div[data-testid="stAlert"][kind="error"] {
    background: linear-gradient(180deg, rgba(253, 244, 244, 0.95), rgba(250, 236, 236, 0.95));
}

div[data-testid="stAlert"][kind="warning"] {
    background: linear-gradient(180deg, rgba(252, 247, 239, 0.95), rgba(248, 240, 227, 0.95));
}

.stButton > button,
.stDownloadButton > button {
    min-height: 3rem;
    border-radius: 18px;
    border: 1px solid var(--line);
    background: linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(240, 244, 248, 0.96));
    color: var(--ink);
    font-weight: 700;
    box-shadow: var(--shadow-sm);
    transition: transform 0.14s ease, box-shadow 0.14s ease, border-color 0.14s ease;
}

.stButton > button:hover,
.stDownloadButton > button:hover {
    transform: translateY(-1px);
    border-color: rgba(184, 17, 32, 0.16);
    box-shadow: 0 14px 28px rgba(15, 23, 42, 0.08);
}

.stButton > button[kind="primary"],
.stDownloadButton > button[kind="primary"] {
    background: linear-gradient(180deg, var(--red-strong) 0%, var(--red) 100%);
    color: #fff;
    border-color: rgba(131, 10, 22, 0.62);
    box-shadow: 0 16px 32px rgba(184, 17, 32, 0.18);
}

.stButton > button[kind="primary"]:hover,
.stDownloadButton > button[kind="primary"]:hover {
    background: linear-gradient(180deg, #e83d41 0%, #c01828 100%);
    border-color: rgba(131, 10, 22, 0.7);
}

.stTabs [data-baseweb="tab-list"] {
    gap: 0.32rem;
    width: fit-content;
    padding: 0.3rem;
    border-radius: var(--radius-pill);
    background: rgba(236, 240, 245, 0.9);
    border: 1px solid var(--line-soft);
    box-shadow: var(--shadow-sm);
    margin-bottom: 1.1rem;
}

.stTabs [data-baseweb="tab"] {
    height: auto;
    padding: 0.56rem 0.95rem;
    border-radius: var(--radius-pill);
    color: var(--muted);
    font-weight: 700;
}

.stTabs [aria-selected="true"] {
    background: rgba(255, 255, 255, 0.96);
    color: var(--red);
    box-shadow: inset 0 0 0 1px rgba(184, 17, 32, 0.08);
}

div[data-baseweb="button-group"] {
    background: rgba(236, 240, 245, 0.9);
    border-radius: var(--radius-pill);
    padding: 0.25rem;
    border: 1px solid var(--line-soft);
    width: fit-content;
}

div[data-baseweb="button-group"] button {
    border-radius: var(--radius-pill) !important;
    border: 0 !important;
    background: transparent !important;
    color: var(--muted) !important;
    font-weight: 700 !important;
    box-shadow: none !important;
    cursor: pointer !important;
    transition:
        background 0.14s ease,
        color 0.14s ease,
        box-shadow 0.14s ease,
        transform 0.14s ease !important;
}

div[data-baseweb="button-group"] button:hover {
    background: rgba(255, 255, 255, 0.78) !important;
    color: var(--ink) !important;
    box-shadow: inset 0 0 0 1px rgba(18, 24, 40, 0.07) !important;
    transform: translateY(-1px) !important;
}

div[data-baseweb="button-group"] button[aria-pressed="true"] {
    background: rgba(255, 255, 255, 0.98) !important;
    color: var(--red) !important;
    box-shadow:
        inset 0 0 0 1px rgba(184, 17, 32, 0.08),
        inset 0 -2px 0 var(--red) !important;
}

div[data-baseweb="button-group"] button[aria-pressed="true"]:hover {
    background: rgba(255, 255, 255, 1) !important;
    color: var(--red) !important;
    box-shadow:
        inset 0 0 0 1px rgba(184, 17, 32, 0.12) !important,
        0 6px 14px rgba(15, 23, 42, 0.06) !important;
}

.stRadio [role="radiogroup"] {
    display: flex;
    flex-wrap: wrap;
    gap: 0.55rem;
}

.stRadio [role="radiogroup"] label {
    border-radius: var(--radius-pill);
    padding: 0.2rem 0.1rem;
}

.stTextInput input,
.stNumberInput [data-baseweb="input"] > div,
.stSelectbox [data-baseweb="select"] > div,
.stMultiSelect [data-baseweb="select"] > div,
.stDateInput [data-baseweb="input"] > div {
    border-radius: 16px !important;
    border: 1px solid var(--line) !important;
    background: rgba(255, 255, 255, 0.94) !important;
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.88);
}

.stTextInput input,
.stNumberInput input,
.stSelectbox [data-baseweb="select"] span,
.stMultiSelect [data-baseweb="select"] span,
.stDateInput input {
    color: var(--ink) !important;
    -webkit-text-fill-color: var(--ink) !important;
}

.stTextInput input::placeholder {
    color: var(--muted-soft) !important;
}

[data-baseweb="tag"] {
    border-radius: 12px !important;
    border: 1px solid var(--line-soft) !important;
    background: rgba(240, 243, 248, 0.95) !important;
}

[data-baseweb="tag"] *,
[data-baseweb="tag"] span,
[data-baseweb="tag"] svg {
    color: var(--ink-soft) !important;
    fill: var(--ink-soft) !important;
    -webkit-text-fill-color: var(--ink-soft) !important;
}

div[data-testid="stFileUploader"] {
    padding: 0;
}

div[data-testid="stFileUploaderDropzone"] {
    border-radius: 24px;
    border: 1px dashed var(--line);
    background: linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(245, 248, 252, 0.94));
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.92);
    padding: 0.85rem 0.95rem;
    transition: border-color 0.15s ease, background 0.15s ease;
}

div[data-testid="stFileUploaderDropzone"]:hover {
    border-color: rgba(184, 17, 32, 0.30);
    background: linear-gradient(180deg, rgba(255, 255, 255, 0.99), rgba(253, 245, 245, 0.92));
}

div[data-testid="stFileUploaderDropzone"] section,
div[data-testid="stFileUploaderDropzoneInstructions"] *,
div[data-testid="stFileUploaderDropzone"] small {
    color: var(--muted) !important;
}

div[data-testid="stFileUploaderDropzone"] button {
    border-radius: 16px !important;
    border: 1px solid var(--line) !important;
    background: rgba(255, 255, 255, 0.98) !important;
    color: var(--ink) !important;
}

div[data-testid="stVegaLiteChart"],
div[data-testid="stDataFrame"],
div[data-testid="stDataEditor"] {
    background: transparent;
    border: 0;
    box-shadow: none;
    padding: 0;
}

div[data-testid="stVegaLiteChart"] .vega-embed,
div[data-testid="stVegaLiteChart"] .vega-embed > div,
div[data-testid="stVegaLiteChart"] canvas,
div[data-testid="stVegaLiteChart"] svg {
    background: transparent !important;
}

div[data-testid="stDataFrame"] [data-testid="stDataFrameResizable"],
div[data-testid="stDataEditor"] [data-testid="stDataFrameResizable"] {
    border-radius: 20px;
    overflow: hidden;
    border: 1px solid var(--line-soft);
    background: linear-gradient(180deg, rgba(255, 255, 255, 0.95), rgba(246, 248, 252, 0.95));
}

div[data-testid="stDataFrame"] [role="columnheader"],
div[data-testid="stDataEditor"] [role="columnheader"] {
    background: rgba(238, 242, 247, 0.92) !important;
}

div[data-testid="stDataFrame"] [role="row"]:nth-child(odd) [role="gridcell"],
div[data-testid="stDataEditor"] [role="row"]:nth-child(odd) [role="gridcell"] {
    background: rgba(255, 255, 255, 0.96) !important;
}

div[data-testid="stDataFrame"] [role="row"]:nth-child(even) [role="gridcell"],
div[data-testid="stDataEditor"] [role="row"]:nth-child(even) [role="gridcell"] {
    background: rgba(241, 245, 250, 0.96) !important;
}

div[data-testid="stDataFrame"] [role="row"]:hover [role="gridcell"],
div[data-testid="stDataEditor"] [role="row"]:hover [role="gridcell"] {
    background: rgba(232, 238, 246, 0.98) !important;
}

div[data-testid="stDataEditor"] [role="row"] [role="gridcell"]:nth-child(1),
div[data-testid="stDataEditor"] [role="row"] [role="gridcell"]:nth-child(2) {
    color: var(--ink) !important;
    -webkit-text-fill-color: var(--ink) !important;
    opacity: 1 !important;
}

div[data-testid="stDataEditor"] [role="columnheader"]:last-child {
    background: linear-gradient(180deg, rgba(255, 243, 243, 0.98), rgba(253, 236, 236, 0.98)) !important;
    color: var(--red) !important;
}

div[data-testid="stDataEditor"] [role="row"] [role="gridcell"]:last-child {
    background: rgba(184, 17, 32, 0.08) !important;
}

div[data-testid="stDataEditor"] [role="row"]:hover [role="gridcell"]:last-child {
    background: rgba(184, 17, 32, 0.14) !important;
}

.search-results-table-wrap {
    overflow: auto;
    border-radius: 20px;
    border: 1px solid var(--line-soft);
    background: linear-gradient(180deg, rgba(255, 255, 255, 0.95), rgba(246, 248, 252, 0.95));
    box-shadow: var(--shadow-sm);
}

.search-results-table {
    width: 100%;
    min-width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    table-layout: fixed;
}

.search-results-table th,
.search-results-table td {
    padding: 0.68rem 0.5rem;
    border-right: 1px solid var(--line-soft);
    border-bottom: 1px solid var(--line-soft);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    font-size: 0.9rem;
}

.search-results-table th {
    position: sticky;
    top: 0;
    z-index: 1;
    background: rgba(238, 242, 247, 0.98);
    color: var(--muted);
    text-align: left;
    font-size: 0.8rem;
    font-weight: 700;
}

.search-results-table tbody tr:nth-child(odd) td {
    background: rgba(255, 255, 255, 0.96);
}

.search-results-table tbody tr:nth-child(even) td {
    background: rgba(248, 250, 252, 0.96);
}

.search-results-table tbody tr:hover td {
    background: rgba(232, 238, 246, 0.98);
}

.search-results-table .search-results-num {
    text-align: right;
    font-variant-numeric: tabular-nums;
}

.search-results-table th:last-child,
.search-results-table td:last-child {
    border-right: 0;
}

.search-results-table tbody tr:last-child td {
    border-bottom: 0;
}

details[data-testid="stExpander"] {
    border: 1px solid var(--line-soft);
    border-radius: 22px;
    background: linear-gradient(180deg, rgba(255, 255, 255, 0.94), rgba(245, 248, 252, 0.92));
    overflow: hidden;
}

details[data-testid="stExpander"] summary {
    padding: 0.85rem 1rem;
}

[data-testid="stDialog"] > div {
    background: rgba(250, 252, 255, 0.98) !important;
    border: 1px solid var(--line) !important;
    border-radius: 28px !important;
    box-shadow: 0 30px 80px rgba(15, 23, 42, 0.18) !important;
}

@media (max-width: 1100px) {
    .page-header,
    .hero-grid {
        grid-template-columns: 1fr;
    }

    .header-meta-row,
    .hero-facts,
    .meta-list,
    .tile .tile-stats {
        grid-template-columns: repeat(2, minmax(0, 1fr));
    }
}

@media (max-width: 720px) {
    .header-meta-row,
    .hero-facts,
    .meta-list,
    .tile .tile-stats {
        grid-template-columns: 1fr;
    }

    .page-title,
    .hero-title,
    .upload-title {
        max-width: none;
    }

    .block-container {
        padding-top: 0.8rem;
    }
}
</style>
"""
    st.markdown(style, unsafe_allow_html=True)
