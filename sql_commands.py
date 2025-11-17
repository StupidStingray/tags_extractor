SQL_DROP_TABLE = "DROP TABLE IF EXISTS {table_name} CASCADE;"
SQL_CREATE_TABLE = "CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, {column_name} VARCHAR(40) UNIQUE NOT NULL);"
SQL_INSERT_ROW = "INSERT INTO {table_name} ({column_name}) VALUES (%s);"

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


SQL_DELETE_PREVIOUS_TAGS = "DELETE FROM cldt WHERE doc_id = %s;"

SQL_INSERT_CLDT = """
    INSERT INTO cldt (doc_id, doc_part, revision_number, link_level, tag)
    VALUES (%s, %s, %s, %s, %s);"""


SQL_INSERT_ERRORS = """
        INSERT INTO errors (doc_id, revision_number, page, wrong_tag, right_tag)
        VALUES (%s, %s, %s, %s, %s);"""


SQL_INSERT_CLDT = """
    INSERT INTO cldt (doc_id, doc_part, revision_number, link_level, tag)
    VALUES (%s, %s, %s, %s, %s);"""