import streamlit as st
import snowflake.connector
import uuid
import json
from datetime import datetime

# Establish Connection to Snowflake
@st.cache_resource
def get_connection():
    creds = st.secrets["snowflake"]
    return snowflake.connector.connect(
        user=creds["user"],
        password=creds["password"],
        account=creds["account"],
        warehouse=creds["warehouse"],
        database=creds["database"],
        schema=creds["schema"]
    )

conn = get_connection()
cursor = conn.cursor()

#  Page Configuration 
st.title(" Extract Configuration")

# Source Selection 
source_name = st.selectbox("Select Source Name", ["user_data","TEST_SOURCE", "CUSTOMERS", "ORDERS"])

if st.button("Run and Preview Source Data"):
    try:
        cursor.execute(f"SELECT * FROM {source_name} LIMIT 10")
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        st.dataframe(data, use_container_width=True)
    except Exception as e:
        st.error(f"Error: {e}")

# Config Management 
cursor.execute("SELECT CONFIG_NAME FROM CONFIGURATIONS ORDER BY CREATED_AT DESC")
existing_configs = [row[0] for row in cursor.fetchall()]
selected_config = st.selectbox("Select Existing Configuration:", existing_configs)
new_config = st.text_input("Enter New Configuration Name:")


# Global Extract Filters
st.subheader(" Global Extract Filters")

if "filters" not in st.session_state:
    st.session_state.filters = []

if st.button("Add Filter"):
    st.session_state.filters.append({})

# Store index of filter to remove
filter_to_remove = None

filters = []
for i in range(len(st.session_state.filters)):
    st.markdown(f"**Filter {i+1}**")
    column = st.selectbox(f"Column {i}", ["ADDRESS_ID", "CITY", "PINCODE", "COUNTRY"], key=f"f_col{i}")
    condition = st.selectbox(f"Condition {i}", ["=", "<", ">", "!=", "LIKE"], key=f"f_cond{i}")
    value = st.text_input(f"Value {i}", key=f"f_val{i}")
    case_sensitive = st.checkbox(f"Case sensitive {i}", key=f"f_cs{i}")
    combiner = st.selectbox(f"Combine with next filter using", ["AND", "OR", ""], key=f"f_comb{i}")
    filters.append({
        "column": column,
        "condition": condition,
        "value": value,
        "case_sensitive": case_sensitive,
        "combiner": combiner
    })

    if st.button(f" Remove Filter {i+1}", key=f"remove_filter_{i}"):
        filter_to_remove = i

# Remove filter from session state (after loop to avoid list mutation issues)
if filter_to_remove is not None:
    st.session_state.filters.pop(filter_to_remove)

#  Attributes (Rules Engine) 
st.subheader(" Attributes (Rules Engine)")

if "rules" not in st.session_state:
    st.session_state.rules = []

if st.button("Add Simple Rule"):
    st.session_state.rules.append({})

rules = []
for i in range(len(st.session_state.rules)):
    st.markdown(f"**Rule {i+1}**")
    rule_name = st.text_input(f"Rule Name {i}", key=f"r_name{i}")
    description = st.text_input(f"Description {i}", key=f"r_desc{i}")
    column = st.selectbox(f"Column {i}", ["ADDRESS_ID", "PINCODE", "CITY", "COUNTRY"], key=f"r_col{i}")
    operator = st.selectbox(f"Operator {i}", ["=", "!=", "<", ">", "LIKE"], key=f"r_op{i}")
    value = st.text_input(f"Value {i}", key=f"r_val{i}")
    then = st.text_input(f"THEN {i}", key=f"r_then{i}")
    els = st.text_input(f"ELSE {i}", key=f"r_else{i}")
    new_col = st.text_input(f"New Column Name {i}", key=f"r_newcol{i}")

    rules.append({
        "column": column,
        "operator": operator,
        "value": value,
        "then": then,
        "else": els,
        "as": new_col
    })

#SQL Preview 
if st.button("Preview SQL Results"):
    try:
        st.success("SQL preview generated successfully!")
        # SELECT Part
        select_base = "*"
        case_statements = []
        for r in rules:
            case = f"CASE WHEN {r['column']} {r['operator']} '{r['value']}' THEN '{r['then']}' ELSE '{r['else']}' END AS {r['as']}"
            case_statements.append(case)
        if case_statements:
            select_base += ", " + ", ".join(case_statements)

        # WHERE Part
        where_clause = ""
        for f in filters:
            clause = f"{f['column']} {f['condition']} '{f['value']}'"
            if f['condition'] == "LIKE" and not f['case_sensitive']:
                clause = f"LOWER({f['column']}) LIKE LOWER('{f['value']}')"
            if where_clause:
                where_clause += f" {f['combiner']} {clause}"
            else:
                where_clause = clause

        final_sql = f"WITH base AS (SELECT * FROM {source_name})\nSELECT {select_base} FROM base"
        if where_clause:
            final_sql += f"\nWHERE {where_clause}"

        st.session_state.final_query = final_sql

        st.subheader("Generated SQL")
        st.code(final_sql)

        cursor.execute(final_sql)
        preview_rows = cursor.fetchall()
        preview_cols = [desc[0] for desc in cursor.description]
        st.subheader("Latest SQL Preview")
        st.dataframe(preview_rows, use_container_width=True)
    except Exception as e:
        st.error(f"Query failed: {e}")

#  Save Configuration 
if st.button("Save Configuration"):
    try:
        config_version = new_config or selected_config + "_V_" + datetime.now().strftime("%Y%m%d%H%M%S")
        view_name = config_version.lower().replace(" ", "_")

        final_sql = st.session_state.get("final_query", None)
        if not final_sql:
            st.warning("Please preview the SQL before saving.")

        # 1. Create View in Snowflake
        cursor.execute(f"CREATE OR REPLACE VIEW {view_name} AS {final_sql}")

        # 2. Prepare config data
        config_data = {
            "config_id": str(uuid.uuid4()),
            "config_name": config_version,
            "source_name": source_name,
            "view_name": view_name,
            "filters": json.dumps(filters),
            "rules": json.dumps(rules),
            "sql_text": final_sql,
            "created_at": datetime.now().isoformat()
        }

        # 3. Insert into CONFIGURATIONS table
        insert_query = """
            INSERT INTO CONFIGURATIONS (CONFIG_ID, CONFIG_NAME, SOURCE_NAME, VIEW_NAME, FILTERS, RULES, SQL_TEXT, CREATED_AT)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            config_data["config_id"],
            config_data["config_name"],
            config_data["source_name"],
            config_data["view_name"],
            config_data["filters"],
            config_data["rules"],
            config_data["sql_text"],
            config_data["created_at"]
        ))

        st.success(f" View `{view_name}` created and configuration saved for final SQL:\n\n{final_sql}")
    except Exception as e:
        st.error(f"Failed to create view or save config: {e}")
