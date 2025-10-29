import streamlit as st
import pandas as pd
import requests
import json
import oracledb
import psycopg2
import mysql.connector
import docx2txt
import PyPDF2

# --- ADDED: Imports for new services ---
import snowflake.connector
import google.generativeai as genai
from langchain_google_genai import ChatGoogleGenerativeAI
# ----------------------------------------

from st_aggrid import AgGrid, GridOptionsBuilder

from io import BytesIO
from reportlab.lib.pagesizes import letter
# --- FIXED: Corrected import typo ---
from reportlab.platypus import BaseDocTemplate, Frame, PageTemplate, Table, TableStyle, Paragraph, Spacer, ListFlowable, ListItem
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.lib import colors

from bs4 import BeautifulSoup

from PIL import Image
import pytesseract
from pdf2image import convert_from_bytes

import base64
import pickle


import pyodbc
import re
import smtplib
import io
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import requests
import json

# --- REMOVED: Old LangChain import ---
# from langchain_openai import AzureChatOpenAI 
from langchain.chains import ConversationChain
from langchain.memory import ConversationBufferMemory

# --- CHANGED: Replaced Azure config with Google Gemini Config ---
# --- Google Gemini Config (Replace with secure secrets in prod) --
# You must get your API key from Google AI Studio (makersuite.google.com)
# --- WARNING: The key you provided is visible. Please revoke it and use a new one. ---
GOOGLE_API_KEY = "AIzaSyCVv9qt3QJR-ODHAx1CICRVmRxcjAnkDNo"  # <-- REVOKE THIS KEY
GEMINI_MODEL_NAME = "gemini-2.5-flash" # For text gen and multimodal (ER diagrams)
GEMINI_CHAT_MODEL_NAME = "gemini-2.5-flash"        # For text-only chat agent

# Configure the generative AI library
if GOOGLE_API_KEY:
    genai.configure(api_key=GOOGLE_API_KEY)
else:
    st.warning("Google API Key not set. AI features will not work.")

# --- REMOVED: Image.open("tata_logo.png") ---
# image = Image.open("tata_logo.png") 
# -------------------------------------------
# Set professional page config and CSS styles
st.set_page_config(page_title="MetadataGenbot Enterprise AI",
                   layout="wide", initial_sidebar_state="expanded")

def add_logo(logo_path):
    # --- ADDED: Check if file exists before opening ---
    import os
    if not os.path.exists(logo_path):
        st.warning(f"Logo file not found: {logo_path}")
        return

    file_ = open(logo_path, "rb")
    contents = file_.read()
    import base64
    encoded = base64.b64encode(contents).decode()
    file_.close()

    return st.markdown(
        f"""
        <style>
            [data-testid="stSidebarNav"] {{
                background-image: url("data:image/png;base64,{encoded}");
                background-repeat: no-repeat;
                background-position: 20px 20px;
                background-size: 120px 60px;
                padding-top: 80px;
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )



st.markdown("""
<style>
    body, div, input, label, textarea, button {
        font-family: 'Segoe UI', Arial, sans-serif !important;
    }
    .block-container {
        padding-top: 36px;
        padding-bottom: 20px;
        background: #f6f8fa;
        border-radius: 10px;
        box-shadow: 0 0 15px rgba(0,0,0,0.04);
    }
    header, footer, #MainMenu {visibility: hidden;}
    .dashboard-title {color: #005baa; font-size: 30px; font-weight: 600;}
    .analytix-logo {height: 38px; vertical-align: middle;}
    .footer {
        position: fixed; left: 0; bottom: 0;
        width: 100%; background: #f6f8fa;
        color: #333; font-size: 14px;
        text-align: right; padding: 10px 20px;
    }
    .stTabs [data-baseweb="tab"] {font-size: 18px; font-weight: 450;}
</style>
""", unsafe_allow_html=True)

# --- REMOVED: add_logo("tata_logo.png") ---
# add_logo("tata_logo.png")

# st.set_page_config(page_title="My App", page_icon=image, layout="centered")


# Branding Header
st.markdown(
    """
    <div style="display: flex; justify-content: space-between; align-items: center;">
        <div>
            <span class='dashboard-title'>MetadataGenbot Enterprise AI</span>
        </div>
    </div>
    <hr style="margin:5px 0 20px 0; border:1px solid #d2e2f7;"/>
    """,
    unsafe_allow_html=True
)

# ----- Database connection functions -----
def connect_to_database(db_type, host, port, user, password, dbname):
    try:
        if db_type == "MySQL":
            return mysql.connector.connect(host=host, port=port, user=user, password=password, database=dbname)
        elif db_type == "PostgreSQL":
            return psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
        elif db_type == "Oracle":
            dsn = f"{host}:{port}/{dbname}"
            return oracledb.connect(user=user, password=password, dsn=dsn)
        # --- ADDED: Snowflake connection logic ---
        elif db_type == "Snowflake":
            # Note: For Snowflake, 'host' is the account identifier, 'dbname' is the database.
            # Port is typically 443 (default) and not always needed.
            return snowflake.connector.connect(
                user=user,
                password=password,
                account=host,
                database=dbname,
                # You may need to add warehouse and schema if not set by default for the user
                # warehouse="YOUR_WAREHOUSE", 
                # schema="YOUR_SCHEMA"
            )
        # ----------------------------------------
        else:
            st.error("Unsupported DB type")
            return None
    except Exception as e:
        st.error(f"Connection failed: {e}")
        return None

def list_schemas(conn, db_type):
    try:
        if db_type == "PostgreSQL":
            df = pd.read_sql("SELECT schema_name FROM information_schema.schemata", conn)
            return df["schema_name"].tolist()
        elif db_type == "Oracle":
            df = pd.read_sql("SELECT username AS schema_name FROM all_users", conn)
            return df["schema_name"].tolist()
        # --- ADDED: Snowflake schema logic ---
        elif db_type == "Snowflake":
            df = pd.read_sql("SELECT schema_name FROM information_schema.schemata WHERE schema_name != 'INFORMATION_SCHEMA'", conn)
            return df["SCHEMA_NAME"].tolist() # Snowflake usually returns uppercase
        # ----------------------------------------
        else:
            # MySQL doesn't use schemas in the same way, return default
            return ["default"]
    except Exception as e:
        st.error(f"Failed to fetch schemas: {e}")
        return []

def list_tables(conn, db_type, schema):
    try:
        if db_type == "MySQL":
            df = pd.read_sql("SHOW TABLES", conn)
            return df.iloc[:, 0].tolist()
        elif db_type == "PostgreSQL":
            query = f"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = '{schema}'"
            df = pd.read_sql(query, conn)
            return df["tablename"].tolist()
        elif db_type == "Oracle":
            query = "SELECT table_name FROM all_tables WHERE owner = :schema"
            df = pd.read_sql(query, conn, params={"schema": schema.upper()})
            return df["table_name"].tolist()
        # --- ADDED: Snowflake list tables logic ---
        elif db_type == "Snowflake":
            # Use UPPER for schema as Snowflake identifiers are typically uppercase
            query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = UPPER('{schema}')"
            df = pd.read_sql(query, conn)
            return df["TABLE_NAME"].tolist()
        # -----------------------------------------
        else:
            return []
    except Exception as e:
        st.error(f"Failed to fetch tables: {e}")
        return []

def get_columns(conn, db_type, schema, table):
    try:
        if db_type == "MySQL":
            query = f"SHOW COLUMNS FROM `{table}`"
            df = pd.read_sql(query, conn)
            return [{"name": r["Field"], "type": r["Type"]} for _, r in df.iterrows()]
        elif db_type == "PostgreSQL":
            query = f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{table}' AND table_schema = '{schema}'
                ORDER BY ordinal_position
            """
            df = pd.read_sql(query, conn)
            return [{"name": r["column_name"], "type": r["data_type"]} for _, r in df.iterrows()]
        elif db_type == "Oracle":
            query = f"""
                SELECT column_name, data_type
                FROM all_tab_columns
                WHERE table_name = UPPER('{table}') AND owner = UPPER('{schema}')
                ORDER BY column_id
            """
            df = pd.read_sql(query, conn)
            return [{"name": r["COLUMN_NAME"], "type": r["DATA_TYPE"]} for _, r in df.iterrows()]
        # --- ADDED: Snowflake get columns logic ---
        elif db_type == "Snowflake":
            query = f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = UPPER('{schema}') AND table_name = UPPER('{table}')
                ORDER BY ordinal_position
            """
            df = pd.read_sql(query, conn)
            # Snowflake columns are often uppercase, match the case from the DB
            return [{"name": r["COLUMN_NAME"], "type": r["DATA_TYPE"]} for _, r in df.iterrows()]
        # ------------------------------------------
        else:
            return []
    except Exception as e:
        st.error(f"Failed to fetch columns: {e}")
        return []

def get_table_comment(conn, db_type, schema, table):
    # This function is misnamed in the original; it fetches COLUMN comments for PostgreSQL.
    # I will replicate this (more useful) behavior for Snowflake.
    try:
        if db_type == "PostgreSQL":
            query = f"""select a.column_name as column_name, pgd.description as comment from information_schema.columns  a left join pg_catalog.pg_statio_all_tables b on a.table_schema=b.schemaname and a.table_name=b.relname     left join pg_catalog.pg_description pgd on b.relid = pgd.objoid and a.ordinal_position=pgd.objsubid where table_schema='{schema}' and table_name='{table[0]}';"""
            df = pd.read_sql(query, conn)
            return df
        # --- ADDED: Snowflake get column comments logic ---
        elif db_type == "Snowflake":
            # This query gets column comments, matching the (more useful) PG implementation
            query = f"""
                SELECT column_name, comment
                FROM information_schema.columns
                WHERE table_schema = UPPER('{schema}') AND table_name = UPPER('{table}')
                ORDER BY ordinal_position
            """
            df = pd.read_sql(query, conn)
            # Rename columns to match the PG output for consistency
            df.rename(columns={"COLUMN_NAME": "column_name", "COMMENT": "comment"}, inplace=True)
            return df
        # --- CHANGED: Kept original MySQL/Oracle behavior (gets table comment) ---
        elif db_type == "MySQL":
            query = f"SHOW TABLE STATUS WHERE Name = '{table}'"
            df = pd.read_sql(query, conn)
            return df['Comment'].iloc[0] if not df.empty and 'Comment' in df.columns else ""
        elif db_type == "Oracle":
            query = f"""
            SELECT comments FROM all_tab_comments
            WHERE owner = UPPER('{schema}') AND table_name = UPPER('{table}')
            """
            df = pd.read_sql(query, conn)
            return df['COMMENTS'].iloc[0] if not df.empty else ""
        else:
            return ""
    except Exception as e:
        st.error(f"Failed to fetch table/column comment: {e}")
        return ""

# --- FIXED: Added 'key' argument to the function signature ---
def preview_table_with_comments(conn, db_type, schema, table, col_comments, key, max_rows=10):
    try:
        # --- CHANGED: Added Snowflake to this query block ---
        if db_type in ["PostgreSQL", "Oracle", "Snowflake"]:
            # Quoting identifiers is safer for these DBs
            query = f'SELECT * FROM "{schema}"."{table}" LIMIT {max_rows}'
        else:
            # MySQL
            query = f'SELECT * FROM `{table}` LIMIT {max_rows}'
        df_sample = pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Failed to fetch sample data for {table}: {e}")
        return

    gb = GridOptionsBuilder.from_dataframe(df_sample)

    column_defs = []
    for col in df_sample.columns:
        col_def = {
            "field": col,
            # Handle case-insensitivity (e.g., Snowflake returns uppercase)
            "headerTooltip": col_comments.get(col, col_comments.get(col.upper(), col_comments.get(col.lower(), ""))),
        }
        column_defs.append(col_def)

    gb.configure_columns(column_defs)
    gb.configure_grid_options(enableBrowserTooltips=True)
    grid_options = gb.build()

    st.markdown(f"### Preview of `{table}` (up to {max_rows} rows)")
    # --- FIXED: Passed the unique 'key' to AgGrid ---
    AgGrid(df_sample, gridOptions=grid_options, allow_unsafe_jscode=True, theme="alpine", key=key)

# --- CHANGED: Replaced Azure OpenAI with Google Gemini ---
def generate_descriptions_with_gemini(table_name, columns, user_extra_prompt=""):
    
    if not GOOGLE_API_KEY or not genai:
        st.error("Google Gemini is not configured. Please set the GOOGLE_API_KEY.")
        return {}

    col_defs = "\n".join([f"- {col['name']} ({col['type']})" for col in columns])
    prompt = f"""
You are a database documentation assistant.
Given this table structure, write detailed descriptions for each column.

Table: {table_name}
Columns:
{col_defs}

{user_extra_prompt}

Return strictly valid JSON in the format: {{"column_name": "description"}}
"""

    with st.expander(f"🔍 Prompt sent to Gemini for table `{table_name}`"):
        st.code(prompt, language="markdown")

    try:
        model = genai.GenerativeModel(GEMINI_MODEL_NAME)
        
        # Use JSON mode for reliable output
        generation_config = genai.types.GenerationConfig(response_mime_type="application/json")
        
        response = model.generate_content(prompt, generation_config=generation_config)
        
        # The response.text will be a valid JSON string
        return json.loads(response.text)

    except Exception as e:
        st.error(f"⚠️ Gemini API error: {e}")
        # Try to parse partial JSON from error message if possible
        try:
            raw_text = str(e)
            return json.loads(raw_text[raw_text.find("{"): raw_text.rfind("}") + 1])
        except:
            return {}
# -----------------------------------------------------------

def generate_update_column_comment_sql(db_type, schema, table, column, desc, col_type=None):
    desc_esc = desc.replace("'", "''")
    if db_type == "PostgreSQL":
        return f'COMMENT ON COLUMN "{schema}"."{table}"."{column}" IS \'{desc_esc}\';'
    elif db_type == "Oracle":
        return f'COMMENT ON COLUMN {schema}.{table}.{column} IS \'{desc_esc}\';'
    elif db_type == "MySQL":
        return f"ALTER TABLE `{table}` MODIFY COLUMN `{column}` {col_type} COMMENT '{desc_esc}';"
    # --- ADDED: Snowflake update comment SQL ---
    elif db_type == "Snowflake":
        # Quoting identifiers is safer
        return f'COMMENT ON COLUMN "{schema}"."{table}"."{column}" IS \'{desc_esc}\';'
    # -------------------------------------------
    else:
        raise ValueError("Unsupported DB type")

def update_column_comment(conn, db_type, schema, table, column, desc, col_type=None):
    try:
        sql = generate_update_column_comment_sql(db_type, schema, table, column, desc, col_type)
        cur = conn.cursor()
        cur.execute(sql)
        # --- CHANGED: Snowflake .commit() is not needed ---
        if db_type != "Snowflake":
            conn.commit()
        return True, None
    except Exception as e:
        return False, str(e)

# --- Document and ER Diagram Processing ---
def extract_text_from_doc(uploaded_doc):
    ext = uploaded_doc.name.split(".")[-1].lower()
    doc_text = ""
    if ext == "txt":
        doc_text = uploaded_doc.read().decode("utf-8")
    elif ext == "docx":
        doc_text = docx2txt.process(uploaded_doc)
    elif ext == "pdf":
        reader = PyPDF2.PdfReader(uploaded_doc)
        doc_text = "\n".join([p.extract_text() or "" for p in reader.pages])
    elif ext == "html":
        soup = BeautifulSoup(uploaded_doc, features="html.parser")
        for script in soup(["script", "style"]):
            script.extract()
        doc_text = soup.get_text()
    return doc_text

# --- CHANGED: Replaced Azure with Gemini for ERD (multimodal) processing ---
def extract_text_from_erd(uploaded_erd):
    
    if not GOOGLE_API_KEY or not genai:
        st.error("Google Gemini is not configured. Please set the GOOGLE_API_KEY.")
        return ""

    prompt = (
        "You are an AI assistant specialized in database design.\n"
        "Analyze the following ER diagram image(s).\n"
        "Describe only the tables, columns, primary keys, foreign keys, and relationships depicted.\n"
        "Provide a clear textual summary."
    )
    
    try:
        # Initialize the multimodal model
        model = genai.GenerativeModel(GEMINI_MODEL_NAME)
        
        content_parts = [prompt]

        # Handle PDF (can have multiple pages)
        if uploaded_erd.name.lower().endswith('.pdf'):
            pdf_bytes = uploaded_erd.read()
            # User had a hardcoded poppler path, keeping it.
            # You may need to remove or update this path.
            # --- NOTE: This Poppler path may cause errors on your machine ---
            try:
                images = convert_from_bytes(pdf_bytes, dpi=300, poppler_path=r'D:\poppler-25.07.0\Library\bin')
            except Exception as poppler_error:
                st.error(f"Poppler error: {poppler_error}. Make sure Poppler is installed and the path is correct.")
                images = []

            if not images:
                return "Could not extract any images from the PDF."

            for img in images:
                content_parts.append(img)
        
        # Handle single image files
        else:
            img = Image.open(uploaded_erd)
            content_parts.append(img)

        # Send the prompt and image(s) to Gemini
        response = model.generate_content(content_parts)
        return response.text

    except Exception as e:
        return f"Gemini API error: {str(e)}"
# -------------------------------------------------------------------------

# ---- Export Functions ----
def dfs_to_excel_bytes(df_dict):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        for tbl_name, df in df_dict.items():
            # Excel sheet name limit 31 chars
            df.to_excel(writer, sheet_name=tbl_name[:31], index=False)
    output.seek(0)
    return output.getvalue()

def add_header_footer(canvas, doc):
    canvas.saveState()
    width, height = letter

    header_text = "MetadataGenbot Documentation Report"
    canvas.setFont('Helvetica-Bold', 12)
    header_width = canvas.stringWidth(header_text, 'Helvetica-Bold', 12)
    canvas.drawString((width - header_width) / 2.0, height - 0.75 * inch, header_text)

    footer_text = f"Page {doc.page}"
    canvas.setFont('Helvetica', 10)
    footer_width = canvas.stringWidth(footer_text, 'Helvetica', 10)
    canvas.drawString(width - inch - footer_width, 0.75 * inch, footer_text)

    canvas.restoreState()

def generate_pdf_report(desc_results):
    buffer = BytesIO()
    doc = BaseDocTemplate(buffer, pagesize=letter)

    frame = Frame(inch, inch, letter[0] - 2 * inch, letter[1] - 2 * inch - 36, id='normal')

    template = PageTemplate(id='test', frames=frame, onPage=add_header_footer)
    doc.addPageTemplates([template])

    elements = []
    styles = getSampleStyleSheet()
    styleH = styles['Heading1']
    styleN = styles['Normal']

    for table_name, df in desc_results.items():
        elements.append(Paragraph(f"Table: {table_name}", styleH))
        elements.append(Spacer(1, 12))

        bullet_items = []
        for _, row in df.iterrows():
            column_name = f"<b>{row['Column']}</b> ({row.get('Type', '')}):"
            description = row['Description']

            item_elements = [
                Paragraph(column_name, styleN),
                Paragraph(description, styleN),
                Spacer(1, 6)
            ]
            bullet_items.append(ListItem(item_elements))

        elements.append(ListFlowable(bullet_items, bulletType='bullet'))
        elements.append(Spacer(1, 24))

    doc.build(elements)
    pdf = buffer.getvalue()
    buffer.close()
    return pdf

# ==== Chat Bot ===========


# ---------------- Database Schema Functions ----------------
def get_sql_server_schema(conn):
    schema = "Database schema (SQL Server):\n\n"
    tables_query = """
    SELECT t.name AS table_name, c.name AS column_name, ty.name AS data_type
    FROM sys.tables t
    JOIN sys.columns c ON t.object_id = c.object_id
    JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    ORDER BY t.name, c.column_id
    """
    tables_df = pd.read_sql(tables_query, conn)
    for table in tables_df['table_name'].unique():
        schema += f"Table {table}: " + ", ".join(tables_df[tables_df['table_name'] == table]['column_name']) + "\n"
    return schema

def get_mysql_schema(conn):
    schema = "Database schema (MySQL):\n\n"
    tables_query = """
    SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
    ORDER BY TABLE_NAME, ORDINAL_POSITION
    """
    tables_df = pd.read_sql(tables_query, conn)
    for table in tables_df['TABLE_NAME'].unique():
        schema += f"Table {table}: " + ", ".join(tables_df[tables_df['TABLE_NAME'] == table]['COLUMN_NAME']) + "\n"
    return schema

def get_postgresql_schema(conn):
    schema = "Database schema (PostgreSQL):\n\n"
    # --- CHANGED: Made query more generic, removed hardcoded schemas ---
    tables_query = """
    SELECT table_schema||'.'||table_name as table_name, column_name
    FROM information_schema.columns
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    ORDER BY table_name, ordinal_position
    """
    tables_df = pd.read_sql(tables_query, conn)
    for table in tables_df['table_name'].unique():
        schema += f"Table {table}: " + ", ".join(tables_df[tables_df['table_name'] == table]['column_name']) + "\n"
    return schema

# --- ADDED: Snowflake schema function ---
def get_snowflake_schema(conn):
    schema = "Database schema (Snowflake):\n\n"
    tables_query = """
    SELECT table_schema||'.'||table_name as table_name, column_name
    FROM information_schema.columns
    WHERE table_schema <> 'INFORMATION_SCHEMA'
    ORDER BY table_name, ordinal_position
    """
    tables_df = pd.read_sql(tables_query, conn)
    # Snowflake results are often uppercase, use the correct column name
    for table in tables_df['TABLE_NAME'].unique():
        schema += f"Table {table}: " + ", ".join(tables_df[tables_df['TABLE_NAME'] == table]['COLUMN_NAME']) + "\n"
    return schema
# ----------------------------------------

def get_schema_description(conn, db_type):
    if db_type == "SQL Server":
        return get_sql_server_schema(conn)
    elif db_type == "MySQL":
        return get_mysql_schema(conn)
    elif db_type == "PostgreSQL":
        return get_postgresql_schema(conn)
    # --- ADDED: Snowflake schema call ---
    elif db_type == "Snowflake":
        return get_snowflake_schema(conn)
    # ------------------------------------
    return "Database schema not available."

# ---------------- Email Function ----------------
# Note: You need to set EMAIL_ADDRESS and EMAIL_PASSWORD as environment variables
# or define them here for this to work.
EMAIL_ADDRESS = "" 
EMAIL_PASSWORD = ""

def send_analytical_email(df, recipient, subject, insight=None, fig=None):
    if not EMAIL_ADDRESS or not EMAIL_PASSWORD:
        st.error("Email credentials (EMAIL_ADDRESS, EMAIL_PASSWORD) not set.")
        return False
        
    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = EMAIL_ADDRESS
    msg["To"] = recipient
    body = f"{insight}\n\nAttached report." if insight else "Attached report."
    msg.attach(MIMEText(body, "plain"))

    # CSV attachment
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    msg.attach(MIMEApplication(csv_bytes, Name="report.csv"))

    # Optional visualization
    if fig:
        buf = io.BytesIO()
        fig.savefig(buf, format='png')
        buf.seek(0)
        msg.attach(MIMEApplication(buf.read(), Name="visualization.png"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, recipient, msg.as_string())
        return True
    except Exception as e:
        st.error(f"Email send failed: {e}")
        return False

# ---------------- LangChain Setup ----------------
def init_conversation_with_schema(conn, db_type):
    
    if not GOOGLE_API_KEY:
        st.error("Google API Key not set. AI Assistant cannot be initialized.")
        return None

    schema = get_schema_description(conn, db_type)

    # --- CHANGED: Replaced AzureChatOpenAI with ChatGoogleGenerativeAI ---
    try:
        llm = ChatGoogleGenerativeAI(
            model=GEMINI_CHAT_MODEL_NAME,
            google_api_key=GOOGLE_API_KEY,
            convert_system_message_to_human=True # Important for Gemini
        )
    except Exception as e:
        st.error(f"Failed to initialize Gemini (LangChain): {e}")
        return None
    # ------------------------------------------------------------------

    memory = ConversationBufferMemory()
    conversation = ConversationChain(llm=llm, memory=memory, verbose=False)

    # Send schema context once
    conversation.predict(input=f"""
You are an expert {db_type} SQL generator.
Here is the database schema for all future queries:
{schema}
Only produce valid SELECT statements.
Format the output using triple backticks, e.g., ```sql\n...SQL_QUERY...\n```
Do NOT include explanations.
""")
    return conversation

# --- CHANGED: Replaced Azure chat with Gemini chat ---
def gemini_chat_completion(prompt):
    
    if not GOOGLE_API_KEY or not genai:
        st.error("Google Gemini is not configured. Please set the GOOGLE_API_KEY.")
        return "Gemini API not configured."

    try:
        model = genai.GenerativeModel(GEMINI_CHAT_MODEL_NAME)
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        raise Exception(f"Gemini API error: {e}")
# ------------------------------------------------------

# --- CHANGED: Renamed function to use Gemini ---
def generate_insight_with_gemini(df):
    if df.empty:
        return "No data to analyze."
    sample_data = df.head(20).to_csv(index=False)
    prompt = f"""
You are a business analyst. Analyze the following dataset:

{sample_data}

Provide a clear business insight (~100 words) covering:
- trends
- anomalies
- possible reasons
- recommendations
"""
    # --- CHANGED: Calls gemini_chat_completion ---
    return gemini_chat_completion(prompt)
# -------------------------------------------------

# ==== MAIN APP ====
def main():

    # Sidebar Inputs
    with st.sidebar:
        st.header("🔑 Database Connection")
        
        # --- CHANGED: Added Snowflake to dropdown ---
        db_type = st.selectbox("Choose Database", ["MySQL", "PostgreSQL", "Oracle", "Snowflake"], index=1)
        
        # --- CHANGED: Added Snowflake default port and updated label ---
        host_label = "Host"
        if db_type == "Snowflake":
            host_label = "Snowflake Account Identifier"
        host = st.text_input(host_label, "localhost")
        
        port_defaults = {"MySQL":"3306","PostgreSQL":"5432","Oracle":"1521", "Snowflake":"443"}
        port = st.text_input("Port", port_defaults[db_type])
        # ---------------------------------------------------------------
        
        dbname = st.text_input("Database / Service Name")
        user = st.text_input("User")
        password = st.text_input("Password", type="password")
        
        if st.button("🚀 Connect to Database"):
            conn = connect_to_database(db_type, host, port, user, password, dbname)
            if conn:
                st.session_state.db_conn = conn
                st.session_state.db_type = db_type
                st.session_state.connected = True
                st.success("✅ Database connection successful!")
                # --- CHANGED: Initialize Gemini chat conversation ---
                st.session_state.conversation = init_conversation_with_schema(conn, db_type)
                st.rerun()
            else:
                st.session_state.connected = False

        st.markdown("---")
        st.subheader("📖 About")
        st.info("MetadataGenbot Enterprise AI: Generate rich DB documentation enhanced by AI.")

    # Tabs for workflow
    tabs = st.tabs(["📊 Documentation", "🤖 AI Assistant", "🔍 Data Preview", "⚙️ Settings"])

    if "connected" not in st.session_state or not st.session_state.connected:
        st.warning("➡️ Please connect to a database from the sidebar to continue.")
        return

    conn = st.session_state.db_conn
    db_type = st.session_state.db_type

    # Documentation Tab
    with tabs[0]:
        st.header("📊 Database Documentation")

        # Schema and tables selection
        # --- CHANGED: Added Snowflake to schema selection logic ---
        if db_type in ["PostgreSQL", "Oracle", "Snowflake"]:
            schemas = list_schemas(conn, db_type)
            schemas.sort()
            schema = st.selectbox("Select Schema", schemas)
        else:
            schema = "default"
        
        if schema: # Ensure a schema is selected before listing tables
            tables = list_tables(conn, db_type, schema)
            tables.sort()
        else:
            tables = []

        mode = st.radio("Select Mode", ["Single Table", "Bulk Tables"], horizontal=True)
        if mode == "Single Table":
            selected_tables = [st.selectbox("Select Table", tables)]
        else:
            select_all = st.checkbox("Select All Tables")
            if select_all:
                selected_tables = tables
            else:
                selected_tables = st.multiselect("Select Tables", tables)

        # Show table comment for single
        if mode == "Single Table" and selected_tables and selected_tables[0]:
            comment = get_table_comment(conn, db_type, schema, selected_tables[0])
            st.subheader(f"Column comments for table: `{selected_tables[0]}`")
            # --- CHANGED: Handle DataFrame or string response from get_table_comment ---
            if isinstance(comment, pd.DataFrame):
                st.dataframe(comment, use_container_width=True)
            else:
                st.info(f"Table Comment: {comment}")
            
            col_comments = {}
            if "desc_results" in st.session_state and selected_tables[0] in st.session_state.desc_results:
                df_desc = st.session_state.desc_results[selected_tables[0]]
                col_comments = {row["Column"]: row["Description"] for _, row in df_desc.iterrows()}
            
            # --- FIXED: Passed a unique key for the "Documentation" tab preview ---
            preview_table_with_comments(conn, db_type, schema, selected_tables[0], col_comments, key=f"doc_preview_{selected_tables[0]}")

        # Upload supporting docs & ER diagrams
        st.markdown("### 📝 Upload Supporting Document (Optional)")
        uploaded_file = st.file_uploader("Doc files (TXT, DOCX, PDF, HTML)", type=["txt", "docx", "pdf", "html"])
        doc_text = ""
        if uploaded_file:
            doc_text = extract_text_from_doc(uploaded_file)
            if doc_text:
                st.success(f"✅ {uploaded_file.name} processed and added to AI context")
                with st.expander("📖 Document Preview"):
                    st.text(doc_text[:1500] + ("..." if len(doc_text) > 1500 else ""))

        st.markdown("### 📂 Upload ER Diagram (Optional)")
        uploaded_erd = st.file_uploader("ER Diagram image or PDF", type=["png", "jpg", "jpeg", "pdf"])
        erd_text = ""
        if uploaded_erd:
            with st.spinner("⏳ Processing ER Diagram (Gemini Vision)..."):
                erd_text = extract_text_from_erd(uploaded_erd)
            if erd_text.strip():
                st.success("✅ ER Diagram processed and added to AI context")
                with st.expander("📖 ER Diagram Text Preview"):
                    st.text(erd_text[:1500] + ("..." if len(erd_text) > 1500 else ""))
            else:
                st.warning("⚠️ No text could be extracted from the ER Diagram")

        # Compose final extra prompt for AI
        final_extra_prompt = ""
        if st.session_state.get('user_extra_prompt', None):
            final_extra_prompt += st.session_state.user_extra_prompt
        if doc_text:
            final_extra_prompt += "\n\nReference Document:\n" + doc_text[:4000]
        if erd_text:
            final_extra_prompt += "\n\nEntity Relationship Diagram Details:\n" + erd_text[:4000]

        user_extra_prompt = st.text_area("Additional AI Instructions (optional)", placeholder="Use business-friendly terms")
        st.session_state.user_extra_prompt = user_extra_prompt  # remember prompt

        # Generate AI descriptions button
        if st.button("✨ Generate Descriptions"):
            all_results = {}
            with st.spinner("🤖 Generating database documentation via Gemini..."):
                for tbl in selected_tables:
                    cols = get_columns(conn, db_type, schema, tbl)
                    if not cols:
                        continue
                    # --- CHANGED: Call gemini function ---
                    descs = generate_descriptions_with_gemini(tbl, cols, user_extra_prompt=final_extra_prompt)
                    df = pd.DataFrame(
                        [
                            {
                                "Table": tbl,
                                "Column": c["name"],
                                "Type": c["type"],
                                "Description": descs.get(c["name"], "")
                            }
                            for c in cols
                        ]
                    )
                    all_results[tbl] = df
                st.session_state.desc_results = all_results
            st.success("🎉 AI-generated descriptions ready!")

        # Show editable table descriptions with update checkbox
        if "desc_results" in st.session_state:
            for tbl, df in st.session_state.desc_results.items():
                st.subheader(f"📋 Table: {tbl}")
                df_edit = df.copy()
                if "Update?" not in df_edit.columns:
                    df_edit.insert(0, "Update?", True)  # Default all selected for update
                gb = GridOptionsBuilder.from_dataframe(df_edit)
                for col in df_edit.columns:
                    if col == "Description":
                        gb.configure_column(col, autosize=True, editable=True)
                    elif col == "Update?":
                        gb.configure_column(
                            col,
                            width=80,
                            editable=True,
                            cellRenderer='agCheckboxCellRenderer',
                            cellRendererParams={"checkbox": True, "clickable": True}
                        )
                    else:
                        gb.configure_column(col, autosize=True, editable=False) # --- CHANGED: Made other columns not editable
                grid_options = gb.build()

                col1, col2 = st.columns([1, 1])
                with col1:
                    if st.button(f"Select All - {tbl}"):
                        df_edit["Update?"] = True
                        st.session_state.desc_results[tbl] = df_edit
                        st.rerun() # --- ADDED: Rerun to refresh grid
                with col2:
                    if st.button(f"Unselect All - {tbl}"):
                        df_edit["Update?"] = False
                        st.session_state.desc_results[tbl] = df_edit
                        st.rerun() # --- ADDED: Rerun to refresh grid
                grid_resp = AgGrid(
                    df_edit,
                    gridOptions=grid_options,
                    editable=True,
                    allow_unsafe_jscode=True,
                    theme="alpine",
                    height=350,
                    fit_columns_on_grid_load=False,
                    # --- ADDED: Reload data to apply select/unselect all ---
                    key=f"grid_{tbl}_{datetime.datetime.now().timestamp()}" 
                )
                edited_df = pd.DataFrame(grid_resp["data"])
                st.session_state.desc_results[tbl] = edited_df

            # Update database comments and export options
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("⬆️ Update Selected Columns to Database"):
                    all_tables = list(st.session_state.desc_results.items())
                    total_updates = sum(len(df[df["Update?"] == True]) for _, df in all_tables)
                    if total_updates == 0:
                        st.warning("⚠️ No columns selected for update.")
                    else:
                        progress = st.progress(0)
                        status_area = st.empty()
                        successes = 0
                        errors = []
                        processed = 0
                        for tbl, df in all_tables:
                            df_to_update = df[df["Update?"] == True]
                            for _, row in df_to_update.iterrows():
                                col, desc, col_type = row["Column"], row["Description"], row["Type"]
                                success, err = update_column_comment(conn, db_type, schema, tbl, col, desc, col_type)
                                processed += 1
                                pct = int(processed / total_updates * 100)
                                progress.progress(pct)
                                status_area.write(f"Updating **{tbl}.{col}** → {pct}% complete")
                                if success:
                                    successes += 1
                                else:
                                    errors.append(f"{tbl}.{col}: {err}")
                        progress.empty()
                        status_area.empty()
                        if successes:
                            st.success(f"✅ Successfully updated {successes} columns.")
                        if errors:
                            st.error("❌ Errors occurred:\n" + "\n".join(errors))

            with col2:
                if "desc_results" in st.session_state and st.session_state.desc_results:
                    excel_bytes = dfs_to_excel_bytes(st.session_state.desc_results)
                    st.download_button(
                        label="📥 Download Documentation as Excel",
                        data=excel_bytes,
                        file_name="database_documentation.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

            with col3:
                if "desc_results" in st.session_state and st.session_state.desc_results:
                    pdf_bytes = generate_pdf_report(st.session_state.desc_results)
                    st.download_button(
                        label="📋 Download Documentation as PDF",
                        data=pdf_bytes,
                        file_name="database_documentation_report.pdf",
                        mime="application/pdf"
                    )

    # AI Assistant Tab
    with tabs[1]:
        st.subheader("🤖 Conversational AI Assistant")
        if not st.session_state.get("conversation"):
            st.warning("AI Assistant is not initialized. Please connect to a database first.")
        else:
            st.info("Ask questions about your database schema, relationships, and documentation using natural language.")
            ai_query = st.text_input("Enter your question here:")
            if st.button("Ask AI"):
                with st.spinner("Generating SQL..."):
                    sql_txt = st.session_state.conversation.predict(input=ai_query)
                    match = re.search(r"```(?:sql)?\n(.*?)```", sql_txt, re.DOTALL | re.IGNORECASE)
                    st.session_state.sql_query = match.group(1).strip() if match else sql_txt.strip()
                    if st.session_state.sql_query:
                        st.subheader("💾 Generated SQL")
                        st.code(st.session_state.sql_query, language="sql")
                with st.spinner("Running SQL..."):
                    st.session_state.query_result = pd.read_sql(st.session_state.sql_query, st.session_state.db_conn)
                with st.spinner("Generating Insight..."):
                    # --- CHANGED: Call gemini insight function ---
                    st.session_state.insight = generate_insight_with_gemini(st.session_state.query_result)
                    st.session_state.last_question = ai_query
                
                if st.session_state.query_result.empty:
                    st.warning("Query returned no results.")
                else:
                    st.subheader("📊 Query Results")
                    st.dataframe(st.session_state.query_result)

                col1, col2 = st.columns(2)
                with col1:
                    if "insight" in st.session_state:
                        st.subheader("💡 Insight")
                        st.write(st.session_state.insight)

                with col2:
                    st.subheader("📈 Visualization")
                    st.write("You can still call Gemini to generate chart code here.")

                with st.expander("📧 Email Report"):
                    email_to = st.text_input("Recipient Email")
                    subject = st.text_input("Subject", f"Data Report - {datetime.date.today()}")
                    if st.button("Send Email"):
                        if send_analytical_email(st.session_state.query_result, email_to, subject, insight=st.session_state.insight):
                            st.success("Email sent!")

    # Data Preview Tab
    with tabs[2]:
        st.subheader("🔍 Data Preview")
        # --- CHANGED: Added Snowflake to schema logic ---
        if db_type in ["PostgreSQL", "Oracle", "Snowflake"]:
            schemas = list_schemas(conn, db_type)
            schemas.sort()
            preview_schema = st.selectbox("Preview Schema", schemas, key="preview_schema")
        else:
            preview_schema = "default"
        
        if preview_schema:
            tables = list_tables(conn, db_type, preview_schema)
            tables.sort()
        else:
            tables = []

        preview_table = st.selectbox("Choose table to preview", tables, key="preview_table")
        if preview_table:
            col_comments = {}
            if "desc_results" in st.session_state and preview_table in st.session_state.desc_results:
                df_desc = st.session_state.desc_results[preview_table]
                col_comments = {row["Column"]: row["Description"] for _, row in df_desc.iterrows()}
            
            # --- FIXED: Passed a unique key for the "Data Preview" tab ---
            preview_table_with_comments(conn, db_type, preview_schema, preview_table, col_comments, key=f"data_preview_{preview_table}")

    # Settings Tab
    with tabs[3]:
        st.subheader("⚙️ Settings")
        st.info("Here you can add API keys, set model names, or configure other app settings.")
        # Example: Allow user to set API key in UI (less secure, but an option)
        # new_api_key = st.text_input("Set Google API Key", type="password")
        # if st.button("Save Key"):
        #     GOOGLE_API_KEY = new_api_key
        #     genai.configure(api_key=GOOGLE_API_KEY)
        #     st.success("API Key updated.")
            

# Application Footer
st.markdown("""
<div class='footer'>
    Powered by Data and Analytics Practice Analytix-Hub © 2025
</div>
""", unsafe_allow_html=True)



if __name__ == "__main__":
    main()