# REDCap → Snowflake Loader (Python)

This folder contains a portfolio example showing how survey data can be extracted from REDCap via API and loaded into Snowflake for automated reporting and Tableau-ready analytics.

## What it does
- Connects to REDCap using an API URL + token
- Exports records as a Pandas DataFrame
- Connects to Snowflake
- Loads the DataFrame to a target Snowflake table using `write_pandas`
- Includes a simple Streamlit UI to fetch and load data

## Security / secrets
Do **not** hardcode credentials in code. This project reads secrets from either:
- Streamlit secrets (`.streamlit/secrets.toml`) OR
- Environment variables

## How to run
1) Install dependencies:
```bash
pip install -r requirements.txt
