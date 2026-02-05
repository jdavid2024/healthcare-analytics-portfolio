"""
REDCap -> Snowflake Loader (Portfolio Example)

What it does:
- Pulls records from REDCap using an API token
- Loads the resulting DataFrame into Snowflake using write_pandas
- Provides a simple Streamlit UI to fetch + load

Security note:
- Secrets are read from Streamlit secrets or environment variables.
- Do NOT hardcode API keys/passwords in code.
"""

import os
import streamlit as st
import pandas as pd
from redcap import Project
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


# -----------------------------
# Secrets / configuration
# -----------------------------
def get_setting(name: str) -> str:
    """
    Read a setting from Streamlit secrets first, then environment variables.
    """
    if hasattr(st, "secrets") and name in st.secrets:
        return str(st.secrets[name])
    return os.getenv(name, "")


REDCAP_API_URL = get_setting("REDCAP_API_URL")
REDCAP_API_TOKEN = get_setting("REDCAP_API_TOKEN")

SNOWFLAKE_ACCOUNT = get_setting("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = get_setting("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = get_setting("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = get_setting("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = get_setting("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = get_setting("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = get_setting("SNOWFLAKE_TABLE") or "REDCAP_EXPORT"


# -----------------------------
# Connections
# -----------------------------
def get_snowflake_connection():
    """Create a Snowflake connection."""
    missing = [k for k in [
        "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA"
    ] if not get_setting(k)]
    if missing:
        raise ValueError(f"Missing Snowflake settings: {', '.join(missing)}")

    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


def fetch_redcap_data() -> pd.DataFrame:
    """Fetch records from REDCap as a DataFrame."""
    if not REDCAP_API_URL or not REDCAP_API_TOKEN:
        raise ValueError("Missing REDCap settings (REDCAP_API_URL / REDCAP_API_TOKEN).")

    project = Project(REDCAP_API_URL, REDCAP_API_TOKEN)
    df = project.export_records(format="df")
    return df


def load_to_snowflake(df: pd.DataFrame, table_name: str) -> None:
    """
    Load a DataFrame to Snowflake.
    Portfolio demo uses 'replace' behavior (truncate/reload) for simplicity.

    In production: use staging + MERGE for upserts, plus schema management.
    """
    if df is None or df.empty:
        st.warning("No data to load.")
        return

    # Snowflake identifiers: keep uppercase by convention
    table_name = table_name.upper()

    with get_snowflake_connection() as conn:
        with conn.cursor() as cur:
            # Simple table creation demo (replace with a proper schema in real use)
            cur.execute(f'CREATE TABLE IF NOT EXISTS {table_name} (RAW_VARIANT VARIANT)')

            # Truncate for demo purposes
            cur.execute(f"TRUNCATE TABLE {table_name}")

        # Load: write_pandas writes columns; here we store raw records as columns directly
        # If you want RAW_VARIANT only, you’d reshape df before loading.
        success, nchunks, nrows, _ = write_pandas(conn, df, table_name)
        if not success:
            raise RuntimeError("write_pandas failed.")

    st.success(f"Loaded {len(df):,} records into {table_name}.")


# -----------------------------
# App
# -----------------------------
def main():
    st.title("REDCap → Snowflake Data Loader (Portfolio Example)")

    st.caption("Demo app showing API extraction from REDCap and loading into Snowflake.")

    if st.button("Fetch data from REDCap"):
        with st.spinner("Fetching..."):
            try:
                df = fetch_redcap_data()
                st.session_state["df"] = df
                st.success(f"Fetched {len(df):,} rows.")
                st.dataframe(df.head(50))
            except Exception as e:
                st.error(f"REDCap fetch failed: {e}")

    if "df" in st.session_state:
        st.subheader("Load to Snowflake")
        table = st.text_input("Target table", value=SNOWFLAKE_TABLE)

        if st.button("Load to Snowflake"):
            with st.spinner("Loading..."):
                try:
                    load_to_snowflake(st.session_state["df"], table)
                except Exception as e:
                    st.error(f"Snowflake load failed: {e}")


if __name__ == "__main__":
    main()
