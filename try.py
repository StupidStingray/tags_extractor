
#%% imports
import tempfile
import fitz
import shutil
import openpyxl
import os
import pandas as pd
from io import BytesIO
from datetime import datetime
import pytz
import psycopg2
from psycopg2 import Error
from fastapi import FastAPI, File, UploadFile

app = FastAPI()


def create_or_overwrite_eqdb(connection, table_name, column_name, data_set):
    """
    Checks if a table exists. If it exists, it drops and recreates it 
    to effectively 'overwrite' the entire table structure and content. 
    It then inserts the unique values from the Python set.
    """
    
    # 1. Define the SQL to drop and recreate the table structure
    # Note: We use TEXT for the unique values, but this could be INTEGER if your set contains numbers.
    SQL_DROP = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
    SQL_RECREATE = f"""
    CREATE TABLE {table_name} (
        id SERIAL PRIMARY KEY,
        {column_name} VARCHAR(40) UNIQUE NOT NULL
    );
    """
    SQL_INSERT_ROW = f"INSERT INTO {table_name} ({column_name}) VALUES (%s);"
    
    # Prepare data for bulk insertion: list of tuples (required by executemany)
    # The data set elements must be wrapped in a tuple: [(value,), (value,), ...]
    data_for_insert = [(item,) for item in data_set]
    
    try:
        with connection.cursor() as cursor:
            # --- Drop Existing Table (The Overwrite Step) ---
            cursor.execute(SQL_DROP)
            print(f"\nTable '{table_name}' dropped (if it existed) to prepare for overwrite.")
            
            # --- Create New Table ---
            cursor.execute(SQL_RECREATE)
            print(f"Table '{table_name}' successfully recreated.")
            
            # --- Bulk Insert Data ---
            if data_for_insert:
                cursor.executemany(SQL_INSERT_ROW, data_for_insert)
                connection.commit()
                print(f"Successfully inserted {len(data_for_insert)} unique rows into '{table_name}'.")
            else:
                print("The input set was empty; no data inserted.")
                
    except Error as e:
        print(f"Error during table creation/insertion: {e}")
        # Rollback the transaction in case of an error
        connection.rollback()        
    
eqdb_sheet_name = "NFE1-ME-20829-A-PO-F-001"


#%% connect to DB

DB_HOST = "localhost"
DB_NAME = "heru4_staging"
DB_USER = "python_service"
DB_PASSWORD = "08082018"
DB_PORT = "5432"

SQL_CREATE_DOC_TABLE = """
CREATE TABLE IF NOT EXISTS document_versions (
    doc_id VARCHAR(25) PRIMARY KEY,
    revision_number VARCHAR(2) NOT NULL
);
"""

SQL_CREATE_CLDT_TABLE = """
CREATE TABLE IF NOT EXISTS cldt (
    id SERIAL PRIMARY KEY,
    doc_id VARCHAR(25) NOT NULL,
    doc_part VARCHAR(3) NOT NULL,
    revision_number VARCHAR(2) NOT NULL,
    link_level VARCHAR(15) NOT NULL,
    tag VARCHAR(40) NOT NULL
);
"""

SQL_CREATE_ERRORS_TABLE = """
CREATE TABLE IF NOT EXISTS errors (
    id SERIAL PRIMARY KEY,
    doc_id VARCHAR(25) NOT NULL,
    revision_number VARCHAR(2) NOT NULL,
    page INTEGER NOT NULL,
    wrong_tag VARCHAR(40) NOT NULL,
    right_tag VARCHAR(40) NOT NULL
);
"""

#%% EQDB export to Postgres
@app.post("/update_eqdb/")
async def update_eqdb(file: UploadFile = File(...)):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        tmp.write(await file.read())
        EQDB_file_path = tmp.name
    workbook = openpyxl.load_workbook(EQDB_file_path,data_only=True)
    sheet = workbook[eqdb_sheet_name]

    eqdb_tags = set([cell[0].value for cell in sheet.iter_rows(11,sheet.max_row,2,2)])
    eqdb_tags.remove(None)

    try:
    # Establish the connection
        connection = psycopg2.connect(
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME
                )

    # Cursor allows us to execute SQL commands
        cursor = connection.cursor()
        print("PostgreSQL database connection successful.")

        create_or_overwrite_eqdb(connection, "eqdb", "tag", eqdb_tags)

    except (Exception, Error) as error:
    # Catch connection and query errors
        print(f"Error while connecting to PostgreSQL or executing query: {error}")

    finally:
    # This block always executes, ensuring the connection is closed
        if connection:
            cursor.close()
            connection.close()
            print("\nPostgreSQL connection closed.")

#
