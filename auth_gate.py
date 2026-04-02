from __future__ import annotations

from html import escape
import tomllib
from collections.abc import Mapping
from pathlib import Path

import streamlit as st

from ui_theme import get_brand_logo_data_uri


ROOT = Path(__file__).resolve().parent
SECRETS_CANDIDATES = [
    ROOT / ".streamlit" / "secrets.toml",
    ROOT / "secrets.toml",
]
AUTH_SHARED_KEYS = {"redirect_uri", "cookie_secret"}
AUTH_PROVIDER_KEYS = {"client_id", "client_secret", "server_metadata_url"}


def _read_local_secret_data() -> Mapping[str, object]:
    for path in SECRETS_CANDIDATES:
        if not path.exists():
            continue
        with path.open("rb") as handle:
            data = tomllib.load(handle)
        if isinstance(data, Mapping):
            return data
    return {}


def _get_secrets_mapping() -> Mapping[str, object]:
    try:
        secrets_mapping = st.secrets
    except Exception:
        secrets_mapping = {}

    if isinstance(secrets_mapping, Mapping) and secrets_mapping:
        return secrets_mapping
    return _read_local_secret_data()


def _get_auth_mapping() -> Mapping[str, object]:
    data = _get_secrets_mapping()
    auth_section = data.get("auth")
    if isinstance(auth_section, Mapping):
        return auth_section
    return {}


def _get_access_mapping() -> Mapping[str, object]:
    data = _get_secrets_mapping()
    access_section = data.get("access")
    if isinstance(access_section, Mapping):
        return access_section
    return {}


def is_access_configured() -> bool:
    access_mapping = _get_access_mapping()
    allowed_emails = _normalize_string_list(access_mapping.get("allowed_emails"))
    allowed_domains = _normalize_string_list(access_mapping.get("allowed_domains"))
    return bool(allowed_emails or allowed_domains)


def _get_auth_providers() -> list[tuple[str | None, str]]:
    auth_mapping = _get_auth_mapping()
    if not auth_mapping:
        return []

    providers: list[tuple[str | None, str]] = []

    has_single_provider = all(auth_mapping.get(key) for key in AUTH_PROVIDER_KEYS)
    if has_single_provider:
        providers.append((None, "Sign in"))

    for key, value in auth_mapping.items():
        if key in AUTH_SHARED_KEYS:
            continue
        if not isinstance(value, Mapping):
            continue
        if all(value.get(field) for field in AUTH_PROVIDER_KEYS):
            label = str(value.get("label") or key.replace("_", " ").replace("-", " ").title())
            providers.append((str(key), label))

    return providers


def is_auth_configured() -> bool:
    auth_mapping = _get_auth_mapping()
    if not auth_mapping:
        return False

    if not auth_mapping.get("redirect_uri") or not auth_mapping.get("cookie_secret"):
        return False

    return bool(_get_auth_providers())


def _get_user_display_name() -> str:
    for key in ("name", "email", "preferred_username", "nickname", "sub"):
        value = st.user.get(key)
        if value:
            return str(value)
    return "Authorized User"


def _normalize_string_list(values: object) -> set[str]:
    if not isinstance(values, (list, tuple, set)):
        return set()
    normalized: set[str] = set()
    for value in values:
        text = str(value).strip().lower()
        if text:
            normalized.add(text)
    return normalized


def _get_user_email() -> str:
    email = st.user.get("email")
    return str(email or "").strip().lower()


def is_user_authorized() -> bool:
    if not st.user.is_logged_in:
        return False

    access_mapping = _get_access_mapping()
    allowed_emails = _normalize_string_list(access_mapping.get("allowed_emails"))
    allowed_domains = _normalize_string_list(access_mapping.get("allowed_domains"))

    if not allowed_emails and not allowed_domains:
        return False

    email = _get_user_email()
    if not email:
        return False

    if email in allowed_emails:
        return True

    if "@" in email:
        domain = email.split("@", 1)[1]
        if domain in allowed_domains:
            return True

    return False


def render_session_sidebar() -> None:
    if not st.user.is_logged_in:
        return

    username = _get_user_display_name()
    st.sidebar.markdown('<div class="sidebar-nav-label">Session</div>', unsafe_allow_html=True)
    st.sidebar.markdown(
        f"""
        <div class="sidebar-session-card">
            <div class="sidebar-session-label">Signed in</div>
            <div class="sidebar-session-user">{username}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if st.sidebar.button("Log out", width="stretch", key="sidebar_logout"):
        st.logout()


def _render_auth_setup_notice(page_name: str) -> None:
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"],
        [data-testid="collapsedControl"] {
            display: none !important;
        }
        .block-container {
            max-width: 860px;
            padding-top: 3rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        f"""
        <div class="auth-shell">
            <div class="surface auth-card">
                <div class="page-kicker">Authentication Required</div>
                <h1 class="page-title">Auth is not configured yet</h1>
                <p class="page-subtitle">
                    This app now requires Streamlit OIDC login before anyone can open <strong>{page_name}</strong>.
                </p>
                <div class="panel-note" style="margin-top:1rem;">
                    Add Streamlit auth settings to your secrets, then reload the app.
                </div>
                <div class="note-bar" style="margin-top:1rem;">
                    <strong>Required keys:</strong> <code>[auth]</code> with <code>redirect_uri</code> and
                    <code>cookie_secret</code>, plus either direct provider keys or one or more named
                    <code>[auth.provider]</code> blocks.
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_access_setup_notice(page_name: str) -> None:
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"],
        [data-testid="collapsedControl"] {
            display: none !important;
        }
        .block-container {
            max-width: 860px;
            padding-top: 3rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        f"""
        <div class="auth-shell">
            <div class="surface auth-card">
                <div class="page-kicker">Access Required</div>
                <h1 class="page-title">Allowlist is not configured yet</h1>
                <p class="page-subtitle">
                    This app now requires an email or domain allowlist before anyone can open
                    <strong>{page_name}</strong>.
                </p>
                <div class="panel-note" style="margin-top:1rem;">
                    Add an <code>[access]</code> section to your secrets, then reload the app.
                </div>
                <div class="note-bar" style="margin-top:1rem;">
                    <strong>Example:</strong> <code>allowed_emails = ["bryanvrya@gmail.com"]</code>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_login_screen(page_name: str) -> None:
    providers = _get_auth_providers()

    st.markdown(
        """
        <style>
        [data-testid="stSidebar"],
        [data-testid="collapsedControl"] {
            display: none !important;
        }
        .block-container {
            max-width: 920px;
            padding-top: 3rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    logo_uri = get_brand_logo_data_uri()
    if logo_uri:
        st.markdown(
            f"""
            <div class="auth-logo-wrap">
                <img src="{logo_uri}" alt="La Favorita Market" />
            </div>
            """,
            unsafe_allow_html=True,
        )

    _, form_col, _ = st.columns([0.8, 1.5, 0.8])
    with form_col:
        with st.container(border=True):
            st.markdown(
                f"""
                <div class="section-intro" style="margin-bottom:1rem;">
                    <div class="section-kicker">Secure Access</div>
                    <h3 class="section-title">{page_name}</h3>
                </div>
                """,
                unsafe_allow_html=True,
            )

            if len(providers) == 1:
                provider_name, label = providers[0]
                if st.button(label, type="primary", width="stretch", key="auth_login_single"):
                    if provider_name is None:
                        st.login()
                    else:
                        st.login(provider_name)
            else:
                for provider_name, label in providers:
                    button_key = f"auth_login_{provider_name or 'default'}"
                    if st.button(label, width="stretch", key=button_key):
                        if provider_name is None:
                            st.login()
                        else:
                            st.login(provider_name)


def _render_unauthorized_screen(page_name: str) -> None:
    user_email = escape(_get_user_email() or "Unknown user")

    st.markdown(
        """
        <style>
        [data-testid="stSidebar"],
        [data-testid="collapsedControl"] {
            display: none !important;
        }
        .block-container {
            max-width: 920px;
            padding-top: 3rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    logo_uri = get_brand_logo_data_uri()
    if logo_uri:
        st.markdown(
            f"""
            <div class="auth-logo-wrap">
                <img src="{logo_uri}" alt="La Favorita Market" />
            </div>
            """,
            unsafe_allow_html=True,
        )

    _, form_col, _ = st.columns([0.8, 1.5, 0.8])
    with form_col:
        with st.container(border=True):
            st.markdown(
                f"""
                <div class="section-intro" style="margin-bottom:1rem;">
                    <div class="section-kicker">Access Restricted</div>
                    <h3 class="section-title">{page_name}</h3>
                    <div class="section-copy">
                        This Google account is signed in, but it is not approved to open this app.
                    </div>
                </div>
                <div class="panel-note" style="margin-bottom:1rem;">
                    Signed in as <strong>{user_email}</strong>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if st.button("Log out", type="primary", width="stretch", key="auth_logout_unauthorized"):
                st.logout()


def require_login(page_name: str) -> None:
    if not is_auth_configured():
        _render_auth_setup_notice(page_name)
        st.stop()

    if not is_access_configured():
        _render_access_setup_notice(page_name)
        st.stop()

    if st.user.is_logged_in and is_user_authorized():
        return

    if st.user.is_logged_in:
        _render_unauthorized_screen(page_name)
        st.stop()

    _render_login_screen(page_name)
    st.stop()
