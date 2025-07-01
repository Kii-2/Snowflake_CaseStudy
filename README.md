# Snowflake Extract Configuration Tool

A Streamlit-based web application for creating and managing data extraction configurations from Snowflake databases. This tool provides a user-friendly interface to build SQL queries with filters and rules, preview results, and save configurations as Snowflake views.

# Prerequisites

- Python 3.11 
- Snowflake account with appropriate permissions
- Access to create views and tables in Snowflake

## Running the Application

1. Navigate to the extraction_app directory
   ```powershell
   cd extraction_app
   ```

2. Start the Streamlit application
   ```powershell
   streamlit run app.py
   ```

3. Open your browser and go to `http://localhost:8501`

# Dependencies

- streamlit: Web application framework
- snowflake-connector-python: Snowflake database connectivity
