"""
LOOMI Monthly Report — Multi-page Streamlit + DuckDB dashboard.

Entrypoint: configures the app and routes to pages.
Run with: python -m streamlit run moonwalk_dashboard.py
"""

import hmac
import streamlit as st

st.set_page_config(
    page_title="LOOMI Monthly Report",
    page_icon="\U0001f9fc",
    layout="wide",
)


def _check_password():
    """Gate dashboard behind a shared password from st.secrets."""
    if st.session_state.get("_auth_ok"):
        return True
    pwd = st.secrets.get("DASHBOARD_PASSWORD", "")
    if not pwd:
        return True

    def _on_submit():
        if hmac.compare_digest(st.session_state.get("_pwd_input", ""), pwd):
            st.session_state["_auth_ok"] = True
        else:
            st.session_state["_auth_fail"] = True

    st.markdown("### LOOMI Monthly Report")
    st.text_input("Password", type="password", key="_pwd_input", on_change=_on_submit)
    if st.session_state.get("_auth_fail"):
        st.error("Incorrect password")
    st.stop()


if not _check_password():
    st.stop()

executive_pulse = st.Page("pages/executive_pulse.py", title="Executive Pulse", icon=":material/speed:", default=True)
customer_analytics = st.Page("pages/customer_analytics.py", title="Customer Analytics", icon=":material/people:")
operations_center = st.Page(
    "pages/operations_center.py", title="Operations Center", icon=":material/local_laundry_service:"
)
financial_performance = st.Page(
    "pages/financial_performance.py", title="Financial Performance", icon=":material/account_balance:"
)

page = st.navigation(
    [
        executive_pulse,
        customer_analytics,
        operations_center,
        financial_performance,
    ]
)
page.run()
