
BOOL_EXPORT_EQDB = False

BOOL_DELETE_TABLES = False
tables_to_delete = ["cldt", "errors", "document_versions"]


excel_file_path = "CLDT.xlsx"
cldt_sheet_name = "Sheet1"

EQDB_file_path = "EQDB.xlsx"
eqdb_sheet_name = "NFE1-ME-20829-A-PO-F-001"

vdl_file_path = "VDL for CLDT.xlsm"
vdl_sheet_name = "Forecast List"

#%% connect_to_DB

DB_HOST = "localhost"
DB_NAME = "heru4"
DB_USER = "python_service"
DB_PASSWORD = "08082018"
DB_PORT = "5432"

directory = "documents/"
FILE_NUMBER_START = "3945_"
TAG_SYSTEM_PREFIX = "68"