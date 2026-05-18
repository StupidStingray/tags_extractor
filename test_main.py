import pytest
from unittest.mock import MagicMock, patch, mock_open
import main
import config
import os
import pandas as pd
from datetime import datetime

# --- Fixtures ---

@pytest.fixture
def mock_conn():
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    # Mocking context manager behavior for connection.cursor()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    return mock_connection, mock_cursor

@pytest.fixture
def sample_eqdb_data():
    return {"68-ME-1-123-A", "68-EE-1-456-B"}

# --- Tests for decompose_tag ---

def test_decompose_tag_with_prefix():
    tag = "68-ME-1-123-A"
    # Result should be ('ME', '1-', '12', '3A') based on main.py logic
    result = main.decompose_tag(tag)
    assert result == ("ME", "1-", "12", "3A")

def test_decompose_tag_without_dash_prefix():
    tag = "68ME1-123-A"
    result = main.decompose_tag(tag)
    assert result == ("ME", "1-", "12", "3A")

# --- Tests for DB operations ---

def test_create_or_overwrite_eqdb(mock_conn):
    conn, cursor = mock_conn
    table_name = "test_table"
    column_name = "test_col"
    data_set = {"val1", "val2"}
    
    main.create_or_overwrite_eqdb(conn, table_name, column_name, data_set)
    
    assert cursor.execute.call_count == 2 # Drop and Create
    assert cursor.executemany.call_count == 1
    conn.commit.assert_called()

def test_get_set_from_db(mock_conn):
    conn, cursor = mock_conn
    cursor.fetchall.return_value = [("tag1",), ("tag2",)]
    
    result = main.get_set_from_db(conn, "table", "col")
    
    assert result == {"tag1", "tag2"}
    cursor.execute.assert_called_once()

def test_insert_or_update_document_revision(mock_conn):
    conn, cursor = mock_conn
    cursor.rowcount = 1
    
    result = main.insert_or_update_document_revision(conn, "doc1", "01")
    
    assert result is True
    conn.commit.assert_called_once()

# --- Tests for Excel operations ---

@patch("openpyxl.load_workbook")
def test_eqdb_export_to_Postgres(mock_load_wb, mock_conn):
    conn, cursor = mock_conn
    mock_wb = MagicMock()
    mock_sheet = MagicMock()
    mock_load_wb.return_value = mock_wb
    mock_wb.__getitem__.return_value = mock_sheet
    
    # Mock sheet.iter_rows
    mock_cell_1 = MagicMock()
    mock_cell_1.value = "68-ME-1-123-A"
    mock_cell_2 = MagicMock()
    mock_cell_2.value = None
    mock_sheet.iter_rows.return_value = [[mock_cell_1], [mock_cell_2]]
    mock_sheet.max_row = 11
    
    with patch("main.create_or_overwrite_eqdb") as mock_create:
        main.eqdb_export_to_Postgres(conn)
        mock_create.assert_called_once()

@patch("main.get_set_from_db")
def test_eqdb_import(mock_get_set, mock_conn):
    conn, _ = mock_conn
    mock_get_set.return_value = {"68-ME-1-123-A"}
    
    main.eqdb_import(conn)
    
    assert "68-ME-1-123-A" in main.eqdb_tags
    assert (("ME", "1-", "12", "3A")) in main.eqdb_decomposed 

# --- Tests for PDF/File operations ---

def test_doc_num_and_rev():
    mock_pdf = MagicMock()
    mock_page = MagicMock()
    mock_pdf.__getitem__.return_value = mock_page
    # word format: (x0, y0, x1, y1, "word", block_no, line_no, word_no)
    mock_page.get_text.return_value = [
        (0, 0, 0, 0, "3945_12345678901234567", 0, 0, 0), # 22 chars
        (10, 10, 0, 0, "REV", 0, 0, 0),
        (10, -50, 0, 0, "02", 0, 0, 0)
    ]
    
    # Update config for test
    with patch("config.FILE_NUMBER_START", "3945_"):
        doc_num, rev = main.doc_num_and_rev(mock_pdf, "test.pdf")
        assert doc_num == "3945_12345678901234567"
        assert rev == "02"

@patch("os.listdir")
@patch("os.path.isfile")
def test_get_files(mock_isfile, mock_listdir):
    mock_listdir.return_value = ["file1.pdf", "dir1"]
    mock_isfile.side_effect = lambda x: x.endswith(".pdf")
    
    files = main.get_files()
    assert files == ["file1.pdf"]

@patch("openpyxl.load_workbook")
def test_import_cldt(mock_load_wb):
    mock_wb = MagicMock()
    mock_sheet = MagicMock()
    mock_load_wb.return_value = mock_wb
    mock_wb.__getitem__.return_value = mock_sheet
    
    # Mock data: (doc_id, ?, revision, ?, tag)
    mock_sheet.iter_rows.return_value = [
        ("DOC1", None, "01", None, "68-ME-1-123-A")
    ]
    
    eqdb_decomposed = {("ME", "1-", "12", "3A")}
    result = main.import_cldt(eqdb_decomposed)
    assert len(result) == 1

def test_analyze_file():
    mock_pdf = MagicMock()
    mock_page = MagicMock()
    mock_pdf.__iter__.return_value = [mock_page]
    mock_pdf.__getitem__.return_value = mock_page # for matrix access
    mock_page.rotation_matrix = MagicMock()
    mock_page.get_text.return_value = [
        (0, 0, 0, 0, "68-ME-1-123-A", 0, 0, 0)
    ]
    
    eqdb_tags = {"68-ME-1-123-A"}
    eqdb_dict = {("ME", "1-", "12", "3A"): "68-ME-1-123-A"}
    
    # Mock main.eqdb_decomposed for safety
    with patch("main.eqdb_decomposed", {("ME", "1-", "12", "3A")}):
        tags_found, suspects = main.analyze_file(mock_pdf, "DOC1", "01", eqdb_tags, eqdb_dict)
        assert "68-ME-1-123-A" in tags_found
        assert len(suspects) == 0

# --- Tests for combined logic ---

def test_merge_imported():
    imported = [["DOC1", None, "01", None, "TAG_EXCEL"]]
    found = {"TAG_PDF"}
    result = main.merge_imported(imported, found, "DOC1")
    assert result == {"TAG_PDF", "TAG_EXCEL"}

@patch("main.get_set_from_db")
@patch("main.insert_or_update_document_revision")
def test_cldt_directly(mock_insert, mock_get_set, mock_conn):
    conn, cursor = mock_conn
    mock_get_set.return_value = {"ALREADY_TREATED"}
    # Use two different docs to trigger insert_or_update_document_revision for the first one
    imported = [
        ["NEW_DOC1", None, "01", None, "TAG1"],
        ["NEW_DOC2", None, "01", None, "TAG2"]
    ]
    
    main.cldt_directly(conn, cursor, imported)
    
    assert cursor.executemany.call_count == 1
    # For NEW_DOC1 (i=0): doc_num=NEW_DOC1, imported[i-1][0]=NEW_DOC2. They are different.
    assert mock_insert.call_count == 2

@patch("main.get_files")
@patch("fitz.open")
@patch("main.doc_num_and_rev")
@patch("main.insert_or_update_document_revision")
@patch("main.analyze_file")
@patch("main.merge_imported")
def test_file_treatment(mock_merge, mock_analyze, mock_insert, mock_doc_rev, mock_fitz, mock_get_files, mock_conn):
    conn, cursor = mock_conn
    mock_get_files.return_value = ["test.pdf"]
    mock_doc_rev.return_value = ("DOC1", "01")
    mock_insert.return_value = True
    mock_analyze.return_value = ({"TAG1"}, [])
    mock_merge.return_value = {"TAG1"}
    
    # Set globals needed by file_treatment (it uses eqdb_tags, eqdb_dict)
    main.eqdb_tags = {"TAG1"}
    main.eqdb_dict = {}
    
    main.file_treatment(conn, cursor, [])
    
    assert cursor.execute.call_count >= 4 # Table creations + Delete
    assert cursor.executemany.call_count >= 1 # CLDT insert
    conn.commit.assert_called()

@patch("openpyxl.load_workbook")
@patch("pandas.read_sql")
@patch("pandas.DataFrame.to_excel")
def test_check_remaining(mock_to_excel, mock_read_sql, mock_load_wb, mock_conn):
    conn, _ = mock_conn
    mock_wb = MagicMock()
    mock_sheet = MagicMock()
    mock_load_wb.return_value = mock_wb
    mock_wb.__getitem__.return_value = mock_sheet
    mock_sheet.iter_rows.return_value = [["DOC1"]]
    
    mock_read_sql.return_value = pd.DataFrame([["DOC1"]], columns=["doc_id"])
    
    main.check_remaining(conn)
    mock_to_excel.assert_called_once()

@patch("pandas.read_sql")
@patch("pandas.DataFrame.to_excel")
def test_export_table_to_excel(mock_to_excel, mock_read_sql, mock_conn):
    conn, _ = mock_conn
    mock_read_sql.return_value = pd.DataFrame([{"col1": "val1"}])
    
    main.export_table_to_excel(conn, "table", "out.xlsx")
    
    mock_read_sql.assert_called_once()
    mock_to_excel.assert_called_once_with("out.xlsx", index=False)

# --- Test Main Pipeline ---

@patch("psycopg2.connect")
@patch("main.eqdb_import")
@patch("main.import_cldt")
@patch("main.file_treatment")
@patch("main.cldt_directly")
@patch("main.check_remaining")
@patch("main.export_tables")
@patch("main.export_untreated_files")
def test_main(mock_untreated, mock_tables, mock_check, mock_cldt_dir, mock_file_treat, mock_import_cldt, mock_eqdb_imp, mock_connect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    # Mock config to avoid long execution or errors
    with patch("config.BOOL_EXPORT_EQDB", False):
        with patch("config.BOOL_DELETE_TABLES", False):
            main.main()
    
    mock_eqdb_imp.assert_called_once()
    mock_file_treat.assert_called_once()
    mock_connect.assert_called_once()
    mock_conn.close.assert_called_once()

if __name__ == "__main__":
    pytest.main([__file__])
