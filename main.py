#%% imports
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

#%% function definition

def append_with_pandas(file_path, sheet_name, new_df):
    """
    Appends a new DataFrame to an existing Excel sheet.
    This method involves reading the max row to know where to start writing.
    """
    if not os.path.exists(file_path):
        print(f"Error: The file '{file_path}' does not exist. Please create it first.")
        return

    try:
        # 1. Load the workbook to determine where the existing data ends
        # We need the max_row property, which Pandas doesn't automatically expose for append mode
        wb = openpyxl.load_workbook(file_path)
        ws = wb[sheet_name]
        
        # Calculate the starting row (max_row + 1). max_row is 1-based index.
        start_row = ws.max_row 
        
        # 2. Use ExcelWriter in 'append' mode ('a') with the openpyxl engine
        with pd.ExcelWriter(
            file_path, 
            engine='openpyxl', 
            mode='a', 
            if_sheet_exists='overlay' # Instructs writer to modify existing sheet
        ) as writer:
            
            # 3. Write the DataFrame
            # - startrow: tells pandas where to start writing
            # - header: False prevents writing column names again
            # - index: False prevents writing the DataFrame index numbers
            new_df.to_excel(
                writer,
                sheet_name=sheet_name,
                startrow=start_row,
                header=False,
                index=False
            )

        print("\n--- Operation Complete ---")
        print(f"{len(new_df)} rows successfully appended to '{file_path}' on sheet '{sheet_name}'.")

    except PermissionError:
        print(f"\nError: Permission denied. Please ensure '{file_path}' is closed.")
    except KeyError:
        print(f"\nError: Sheet '{sheet_name}' not found in workbook.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

def decompose_tag(tag):
    if tag[:3] == "68-":
        start_pos=3
    else:
        start_pos=2
    pos_1 = tag.find("1")
    equip_cat=tag[start_pos:pos_1].replace("-","")
    unit = tag[pos_1:pos_1+2]
    if len(tag[pos_1:].replace("-",""))>5:
        package_letter = tag[pos_1+2]
        tag_number = tag[pos_1+3:pos_1+6]
        suffix = tag[pos_1+6:].replace("-","")
    else:
        package_letter = ""
        tag_number = tag[pos_1+2:pos_1+4]
        suffix = tag[pos_1+4:].replace("-","")
    return (equip_cat, unit, tag_number, suffix)
    
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
    
def get_set_from_db(connection, table_name, column_name):
        # A. Define the SELECT query
    SQL_FETCH_CATEGORIES = f"SELECT {column_name} FROM {table_name};"
    
    try:
        with connection.cursor() as cursor:
            # B. Execute the query
            cursor.execute(SQL_FETCH_CATEGORIES)
            
            # C. Fetch all results (returns a list of tuples: [('electronics',), ('home',), ...])
            db_records = cursor.fetchall()
            
            # D. Transform the data into a set
            # This uses a generator expression inside the set() constructor for efficiency.
            # It iterates through the list of tuples (row), extracting the first element (column value) 
            # and feeding it directly to the set.
            eqdb_tags_set = {record[0] for record in db_records}
            
            return eqdb_tags_set
    except Error as e:
        print(f"Error fetching data to convert to set: {e}")
        connection.rollback()   
        
def insert_or_update_document_revision(connection, doc_id, new_revision):
    """
    Inserts a document revision. If the doc_id exists, 
    it updates the revision ONLY if the new_revision is greater than the existing one 
    based on the custom alphanumeric comparison rules (Alpha < Numeric).

    Returns True if the row was updated/inserted, False otherwise.
    """
    # SQL uses the ON CONFLICT clause targeting the PRIMARY KEY (doc_id)
    SQL_UPSERT_REVISION = """
    INSERT INTO document_versions (doc_id, revision_number)
    VALUES (%s, %s)
    ON CONFLICT (doc_id) 
    DO UPDATE SET 
        revision_number = EXCLUDED.revision_number 
    WHERE 
        -- --- Conditional Logic for Alphanumeric Comparison ---
        CASE 
            -- RULE 1: If CURRENT is Alpha AND NEW is Numeric, ALWAYS UPDATE (Numeric > Alpha)
            WHEN document_versions.revision_number ~ '^[A-Za-z]+$' AND EXCLUDED.revision_number ~ '^[0-9]+$' 
                THEN TRUE

            -- RULE 2: If CURRENT and NEW are BOTH Alpha (Compare alphabetically/lexicographically)
            WHEN document_versions.revision_number ~ '^[A-Za-z]+$' AND EXCLUDED.revision_number ~ '^[A-Za-z]+$' 
                THEN document_versions.revision_number < EXCLUDED.revision_number
                
            -- RULE 3: If CURRENT and NEW are BOTH Numeric (Compare numerically for correct natural sort: 2 < 10)
            -- We must safely cast to INTEGER for comparison.
            WHEN document_versions.revision_number ~ '^[0-9]+$' AND EXCLUDED.revision_number ~ '^[0-9]+$' 
                THEN document_versions.revision_number::INTEGER < EXCLUDED.revision_number::INTEGER
            
            -- RULE 4: If CURRENT is Numeric AND NEW is Alpha, NEVER UPDATE (Alpha < Numeric, so the existing numeric is higher)
            WHEN document_versions.revision_number ~ '^[0-9]+$' AND EXCLUDED.revision_number ~ '^[A-Za-z]+$' 
                THEN FALSE

            -- Default/Fallback: No update, or complex mixed alphanumeric strings not covered by above rules
            ELSE FALSE
        END;
    """
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(SQL_UPSERT_REVISION, (doc_id, new_revision))
            connection.commit()
            
            # Use rowcount to determine if any row was affected (inserted OR updated)
            return cursor.rowcount > 0
            
    except Error as e:
        print(f"Error during document revision upsert: {e}")
        connection.rollback()
        return False
    
    
excel_file_path = "CLDT.xlsx"
cldt_sheet_name = "Sheet1"

EQDB_file_path = "EQDB.xlsx"
eqdb_sheet_name = "NFE1-ME-20829-A-PO-F-001"

vdl_file_path = "VDL for CLDT.xlsm"
vdl_sheet_name = "Forecast List"




#%% connect to DB

DB_HOST = "localhost"
DB_NAME = "heru4"
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

### OUTPUT
#doc.save("output.pdf", garbage=4, deflate=True, clean=True)

#%% EQDB export to Postgres
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

#%% EQDB import from Postgres

try:
    # Establish the connection
    connection = psycopg2.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME
    )
    cursor = connection.cursor()
    eqdb_tags = get_set_from_db(connection, "eqdb", "tag")
except (Exception, Error) as error:
    # Catch connection and query errors
    print(f"Error while connecting to PostgreSQL or executing query: {error}")

finally:
    # This block always executes, ensuring the connection is closed
    if connection:
        cursor.close()
        connection.close()
        print("\nPostgreSQL connection closed.")
        
        
eqdb_dict = {decompose_tag(tag) : tag for tag in eqdb_tags}
eqdb_decomposed = set([decompose_tag(tag) for tag in eqdb_tags])
# --- 2. SQL Commands ---
# Example: Create a new table

#%%
#%% documents register
doc_reg = {}
document_revisions = {}

#%% file treatment
directory = "documents/"
all_entries = os.listdir(directory)
    
    # Filter the list to include only actual files
only_files = [
        entry for entry in all_entries 
        if os.path.isfile(os.path.join(directory, entry))
    ]

try:
    # Establish the connection
    connection = psycopg2.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME
    )
    cursor = connection.cursor()
    cursor.execute(SQL_CREATE_DOC_TABLE)
    cursor.execute(SQL_CREATE_CLDT_TABLE)
    cursor.execute(SQL_CREATE_ERRORS_TABLE)
    connection.commit()
    for file_name in only_files:
        file_path = os.path.join(directory,file_name)
        pdf_file = fitz.open(file_path)
        
        content_of_title_page = pdf_file[0].get_text("words",sort=False)
        doc_number_found = False
        revision_found = False
        for word in content_of_title_page:
            
            if ("3945_" in word[4]) and (len(word[4])==22):
                document_number = word[4]
                doc_number_found = True
            elif word[4]=="REV":
                rev_x_pos = word[0]
                rev_y_pos = word[1]
                received_y_pos = rev_y_pos - 200
                revision_found = True
        for word in content_of_title_page:
            if (word[0]> rev_x_pos-10) and (word[0] < rev_x_pos +10):
                if (word[1]<rev_y_pos) and (word[1]>received_y_pos):
                    rev_y_pos = word[1]
                    document_revisions[document_number] = word[4]
        if not(doc_number_found and revision_found):
            document_number = file_name[:22]
            document_revisions[document_number] = file_name[-8:-6]
        treat_document = insert_or_update_document_revision(connection, document_number, document_revisions[document_number])
        if not(treat_document):
            continue
        #% tags extraction
        
        SQL_DELETE_PREVIOUS_TAGS = "DELETE FROM cldt WHERE doc_id = %s;"
        cursor.execute(SQL_DELETE_PREVIOUS_TAGS, (document_number,))
        connection.commit()
        
        tags_found = set()
        list_suspect = []
        page_num = 1
        for page in pdf_file:
            content_of_page = page.get_text("words",sort=False)
            if page_num == 1:
                matrix = page.rotation_matrix
            for word in content_of_page:
                if not(word[4] in tags_found):
                    word_decomposed = decompose_tag(word[4])
                    if (word[4] in eqdb_tags):
                        tags_found.add(word[4])
                    elif word_decomposed in eqdb_decomposed:
                        tags_found.add(eqdb_dict[word_decomposed])
                        list_suspect.append([document_number, document_revisions[document_number], page_num,  word[4] , eqdb_dict[word_decomposed]])
                    elif (len(word[4]) in {4, 5, 6}) and (word[4][:2]=="68"):
                        if page.rotation_matrix == matrix:
                            ending_coord = [word[0]-5,word[3],word[2]+5,2*word[3]-word[1]]                    
                        else:
                            ending_coord = [word[2],word[1]-5,2*word[2]-word[0],word[3]+5]                                
                        ending = page.get_textbox(ending_coord)
                        if ending[-2:]=="\n+":
                            ending = ending[:-2]
                        instrum_word = word[4]+ending
                        instrum_tag_decomposed=decompose_tag(instrum_word)
                        if instrum_word in eqdb_tags:
                            tags_found.add(instrum_word)
                        elif instrum_tag_decomposed in eqdb_decomposed:
                            tags_found.add(eqdb_dict[instrum_tag_decomposed])
                            list_suspect.append([document_number, document_revisions[document_number], page_num,  instrum_word , eqdb_dict[instrum_tag_decomposed]])                                   
            page_num += 1
        cldt_list = [[document_number, "000", document_revisions[document_number],"Tag",item] for item in tags_found]
        SQL_INSERT_CLDT = """
        INSERT INTO cldt (doc_id, doc_part, revision_number, link_level, tag)
        VALUES (%s, %s, %s, %s, %s);"""
        cursor.executemany(SQL_INSERT_CLDT, cldt_list)
        connection.commit()
        
        SQL_INSERT_ERRORS = """
        INSERT INTO errors (doc_id, revision_number, page, wrong_tag, right_tag)
        VALUES (%s, %s, %s, %s, %s);"""
        cursor.executemany(SQL_INSERT_ERRORS, list_suspect)
        connection.commit()
        
        doc_reg[document_number] = tags_found
    
    
    
    # #%% convert doc register to list
    # cldt_list = []
    # for key in doc_reg.keys():
    #     for item in doc_reg[key]:
    #         cldt_list.append(["",key,"000",document_revisions[key],"Tag",item])
            
    # cldt_df = pd.DataFrame(cldt_list,columns = ["","Document","Level","revision","idk","Tag"])
    
    # list_suspect_df = pd.DataFrame(list_suspect,columns = ["document number","revision","page number", "wrong tag", "to be replaced with..."])


except (Exception, Error) as error:
    # Catch connection and query errors
    print(f"Error while connecting to PostgreSQL or executing query: {error}")

finally:
    # This block always executes, ensuring the connection is closed
    if connection:
        cursor.close()
        connection.close()
        print("\nPostgreSQL connection closed.")
#%% copy existing CLDT to old

current_time = datetime.now(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d_%H_%M_%S")
shutil.copy2(excel_file_path,os.path.join('old/',"cldt_"+current_time+".xlsx"))
#%% CLDT append

append_with_pandas(excel_file_path, cldt_sheet_name, cldt_df)
list_suspect_df.to_excel("tag_errors_in_docs.xlsx", sheet_name="errors",index=False)
# cldt_wb = openpyxl.load_workbook(excel_file_path, data_only = True)
# sheet = cldt_wb[cldt_sheet_name]
# for row in cldt_list:
#     sheet.append(row)
# cldt_wb.save("cldt_output.xlsx")
#%% export to xlsx




def export_table_to_excel(table_name, output_file):
    """
    Connects to PostgreSQL, fetches an entire table into a DataFrame,
    and saves it to an Excel file.
    """
    connection = None
    try:
        # Connect to the database
        connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME
        )
        print("Database connection successful.")

        # Construct the SQL query
        sql_query = f"SELECT * FROM {table_name};"
        
        # Use pandas.read_sql to execute the query and load results directly into a DataFrame
        df = pd.read_sql(sql_query, connection)
        
        # Save the DataFrame to an Excel file
        # index=False prevents writing the DataFrame's numerical index to the file
        df.to_excel(output_file, index=False)
        
        print(f"\nSuccessfully exported table '{table_name}' to '{output_file}'.")

    except (Exception, Error) as error:
        print(f"Error while exporting table: {error}")
    
    finally:
        # Close the database connection
        if connection:
            connection.close()
            print("PostgreSQL connection closed.")

TABLES_TO_EXPORT = ["cldt", "errors", "document_versions"]

timestamp = datetime.now(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d_%H_%M_%S")

for table in TABLES_TO_EXPORT:
    OUTPUT_EXCEL_FILE = f"{table}_export_{timestamp}.xlsx"
    export_table_to_excel(table, OUTPUT_EXCEL_FILE)

#%%delete tables

tables_to_delete = []
connection = None
try:
    # Connect to the database
    connection = psycopg2.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME
    )
    cursor = connection.cursor()
    print("Database connection successful.")
    for table_name in tables_to_delete:
        SQL_DROP = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
        cursor.execute(SQL_DROP)    
        connection.commit()
        print("%s deleted successfuly;" % (table_name,))
except (Exception, Error) as error:
    # Catch connection and query errors
    print(f"Error while connecting to PostgreSQL or executing query: {error}")

finally:
    # This block always executes, ensuring the connection is closed
    if connection:
        cursor.close()
        connection.close()
        print("\nPostgreSQL connection closed.")    
    
#%%




lines_workbook = openpyxl.load_workbook(lines_list_file_path,data_only=True)
lines_sheet = lines_workbook['LINE LIST']

lines_names_cells = lines_sheet['H9' : 'H382']
P_design_cells = lines_sheet['S9' : 'S382']
lines_names = []
P_design = []
for cell in lines_names_cells:
    lines_names.append(cell[0].value)
for cell in P_design_cells:
    P_design.append(cell[0].value)

P_design_uniques = []
for P in P_design:
    if not P in P_design_uniques:
        P_design_uniques.append(P)


lines_names_sorted=[]
for i in range(0,len(P_design_uniques)):
    lines_names_sorted.append([])
    for j in range(0,len(lines_names)):
        if P_design[j] == P_design_uniques[i]:
            lines_names_sorted[i].append(lines_names[j])
            
            
colors = [[0.6,1,0.3],[1,1,0],[0.3,1,0.6],[0,1,1],[0.3,0.6,1],[0.6,0.3,1],[1,0.2,0.6],[1,0.6,0.4]]

PIDs = [cell[0].value for cell in sheet["BB4":"BB1500"]]
new_tags = [cell[0].value for cell in sheet["AC4":"AC1500"]]
# old_tags = [cell[0].value for cell in sheet["AI4":"AI1161"]]

PID_files = ["68-14-PR-PID-01010",
        "68-14-PR-PID-01020",
        "68-14-PR-PID-01030",
        "68-14-PR-PID-01040",
        "68-14-PR-PID-01050",
        "68-14-PR-PID-01060",
        "68-14-PR-PID-01070",
        "68-14-PR-PID-01080",
        "68-14-PR-PID-01090",
        "68-14-PR-PID-01100",
        "68-14-PR-PID-01110",
        "68-14-PR-PID-01120",
        "68-14-PR-PID-01130",
        "68-14-PR-PID-01140",
        "68-14-PR-PID-01150",
        "68-14-PR-PID-01160",
        "68-14-PR-PID-01170",
        "68-15-PR-PID-01010",
        "68-15-PR-PID-01020",
        "68-15-PR-PID-01030",
        "68-15-PR-PID-01040",
        "68-15-PR-PID-01050",
        "68-15-PR-PID-01060"]
for i in range(len(PID_files)):
    PID_file = PID_files[i]
    pdf_file = fitz.open("new_stabilo\\"+PID_file+"_0001.pdf")
    page = pdf_file[0]
    if i==0:
        matrix = page.rotation_matrix
    current_old_tags = []
    current_new_tags = []

    for l in range(len(PIDs)):
        if PIDs[l] == PID_file:
    #         current_old_tags.append(old_tags[i])
            current_new_tags.append(new_tags[l])

    instrum_tags = []
    other_tags = []
    prv_tags = []

    content_of_page = page.get_text("words",sort=False)  #get rect for all words

    full_tags = []
    for word in content_of_page:
# # # # # # #         uncomment for lines 
#         if "6814" in word[4]:
#             not_found = True
#             for k in range(len(P_design_uniques)):
#                 if word[4] in lines_names_sorted[k]:
#                     not_found = False
#                     rect_comp = fitz.Rect(word[0],word[1],word[2],word[3])
#                     highlight = page.add_highlight_annot(rect_comp)
#                     highlight.set_colors(stroke=colors[k])
#                     highlight.update()
#             if not_found:
#                 rect_comp = fitz.Rect(word[0],word[1],word[2],word[3])
#                 highlight = page.add_highlight_annot(rect_comp)
#                 highlight.set_colors(stroke=[1, 0.3, 0.3])
#                 highlight.update()
# # # # # # # # # # # # # 
        if ((len(word[4]) == 4)+(len(word[4]) == 5))*(word[4][:2]=="68"):
    #         print(word[4][:2])
            instrum_tags.append(word)
        if (word[4][:3]=="68-")*(word[-1]==0):
            if (not(word[4][3]=="1")):
                other_tags.append(word)
        if (word[4][:5]=="68PRV")*(word[-1]==0):
            prv_tags.append(word)    
    lines_result.insert_pdf(pdf_file,to_page=0)     
    for tag in instrum_tags:
 
        rect_comp1 = fitz.Rect(tag[0],tag[1],tag[2],tag[3])
        if page.rotation_matrix == matrix:
            ending_coord = [tag[0]-5,tag[3],tag[2]+5,2*tag[3]-tag[1]]
            rect_comp2 = fitz.Rect(ending_coord[0],ending_coord[1]+3,ending_coord[2],ending_coord[3]+3)
        else:
            ending_coord = [tag[2],tag[1]-5,2*tag[2]-tag[0],tag[3]+5]
            rect_comp2 = fitz.Rect(ending_coord[0]+3,ending_coord[1],ending_coord[2]+3,ending_coord[3])
        highlight1 = page.add_highlight_annot(rect_comp1)
        highlight2 = page.add_highlight_annot(rect_comp2)

        ending = page.get_textbox(ending_coord)
        if ending[-2:]=="\n+":
            ending = ending[:-2]
        item = ""
        item += tag[4]
        item += ending
        full_tags.append(item)
        if item in current_new_tags:
            highlight1.set_colors(stroke=[0.3, 1, 0.3])
            highlight1.update()
            highlight2.set_colors(stroke=[0.3, 1, 0.3])
            highlight2.update()
        else:
            highlight1.set_colors(stroke=[1, 0.3, 0.3])
            highlight1.update()
            highlight2.set_colors(stroke=[1, 0.3, 0.3])
            highlight2.update()
# print(full_tags)
    for tag in other_tags:
        rect_comp1 = fitz.Rect(tag[0],tag[1],tag[2],tag[3])
        highlight = page.add_highlight_annot(rect_comp1)
        item = ""
        item += tag[4][:2]
        item += "-"
        if tag[4][4]=="1":
            item += tag[4][3]
            item += tag[4][4:]
        else:
            item += tag[4][3:5]
            item += tag[4][5:]
        full_tags.append(item)
        if item in current_new_tags:
            highlight.set_colors(stroke=[0.3, 1, 0.3])
            highlight.update()
        else:
            highlight.set_colors(stroke=[1, 0.3, 0.3])
            highlight.update()
    
    for tag in prv_tags:        
        rect_comp1 = fitz.Rect(tag[0],tag[1],tag[2],tag[3])
        highlight = page.add_highlight_annot(rect_comp1)
        item = ""
        item += tag[4]
        full_tags.append(item)
        if item in current_new_tags:
            highlight.set_colors(stroke=[0.3, 1, 0.3])
            highlight.update()
        else:
            highlight.set_colors(stroke=[1, 0.3, 0.3])
            highlight.update()
    k = 0    
    for tag in current_new_tags:
        if not(tag in full_tags):
            text =  tag
            fontname_to_use = "Roboto"
            rect_x1 = 10
            rect_y1 = 10 + k*10
            rect_x2 = 150
            rect_y2 = 20 + k*10
            rect = (rect_x1, rect_y1, rect_x2, rect_y2)
            rc = page.add_freetext_annot(rect, text, fontsize=8, fontname=fontname_to_use, text_color=[1,0.3,0.3], rotate=0)
            k += 1
    result.insert_pdf(pdf_file,to_page=0)            
result.save("tags_output.pdf")
# lines_result.save("lines_output.pdf")












# for j in range(5,7):
#     old_file = fitz.open("old_MASTER\\C1377-A-12"+str(j)+" (2).pdf")
#     new_file = fitz.open("new_MASTER\\68-15-PR-PID-010"+str(j)+"0_0001_1.pdf")
# 
#     old_page = old_file[0]
#     new_page = new_file[0]
#     print(old_page.rotation_matrix)
#     print(new_page.rotation_matrix)
# 
#     content_of_page = old_page.get_text("words",sort=False)
#     for word in content_of_page:
#         if "OD" in word[4]:
#     #         print(word[4])
#     #         print(word[0:4])
#             height = word[3]-word[1]
#             width = word[2]-word[0]
#     #         p1 = fitz.Point(word[1]+10,new_page.rect.width-word[0])
#             p1 = fitz.Point(word[0],word[1])/new_page.rotation_matrix
#             rect_comp = fitz.Rect(word[0],word[1],word[0]+width,word[1]+height)/new_page.rotation_matrix
#     #         print(rect_comp)
#     #         rect_comp = fitz.Rect(word[0],word[1],word[2],word[3])
#     #         print(rect_comp)
#     #         p1 = fitz.Point(
#             fontname_to_use = "Roboto"
#     #         rc = new_page.insert_text(p1, word[4],color=(1,0,0), fontsize=10, rotate=90)
#             if width>height:
#                 rc = new_page.add_freetext_annot(rect_comp, word[4], fontsize=10, fontname=fontname_to_use, text_color=[1,0,0], rotate=90)
#             else:
#                 rc = new_page.add_freetext_annot(rect_comp, word[4], fontsize=10, fontname=fontname_to_use, text_color=[1,0,0], rotate=180)
#     # for i in range(10):
#     #     p1 = fitz.Point(40*i,new_page.rect.width-130*i)
#     #     rc = new_page.insert_text(p1, str(i),color=(1,0,0), fontsize=10, rotate=90)
#     new_file.save("new_MASTER\\68-15-PR-PID-010"+str(j)+"0_0001_2.pdf")

