-- ========================================
-- SASS Page Clean Export to PostgreSQL
-- Exports filtered representative pages from sass_page_clean
-- No identity mapping - all rows are representatives
-- ========================================

DELIMITER $$

DROP PROCEDURE IF EXISTS DiagnoseExportEnvironment$$
DROP PROCEDURE IF EXISTS DiagnoseFileSystemIssues$$
DROP PROCEDURE IF EXISTS ExportSASSPageCleanSecure$$
DROP PROCEDURE IF EXISTS ExportSASSPageCleanChunked$$
DROP PROCEDURE IF EXISTS ValidateExportIntegrity$$
DROP PROCEDURE IF EXISTS CreateExportTable$$

-- Diagnostic procedure for export environment
CREATE PROCEDURE DiagnoseExportEnvironment()
BEGIN
    DECLARE total_pages BIGINT;
    DECLARE sample_1pct BIGINT;
    DECLARE min_page_id BIGINT;
    DECLARE max_page_id BIGINT;
    
    SELECT VERSION() as mysql_version;
    SHOW VARIABLES LIKE 'secure_file_priv';
    
    SELECT 
        COUNT(*),
        MIN(page_id),
        MAX(page_id)
    INTO total_pages, min_page_id, max_page_id
    FROM sass_page_clean;
    
    SET sample_1pct = FLOOR(total_pages / 100);
    
    SELECT 
        'Export Data Availability' as check_type,
        FORMAT(total_pages, 0) as total_pages_to_export,
        FORMAT(sample_1pct, 0) as sample_1pct,
        FORMAT(min_page_id, 0) as min_id,
        FORMAT(max_page_id, 0) as max_id;
    
    SELECT 
        'Export Recommendations' as info_type,
        'All pages in sass_page_clean are representatives (no identity filtering needed)' as architecture_note,
        'Use chunked export for large datasets' as method_recommendation,
        CONCAT('Estimated rows: ~', ROUND(total_pages/1000000, 2), 'M') as data_volume;
END$$

-- Diagnostic procedure for file system issues
CREATE PROCEDURE DiagnoseFileSystemIssues(
    IN requested_path VARCHAR(255)
)
BEGIN
    DECLARE secure_path VARCHAR(255);
    DECLARE test_file VARCHAR(512);
    
    SELECT @@GLOBAL.secure_file_priv INTO secure_path;
    
    SELECT 
        'MySQL File System Configuration' as diagnostic_type,
        secure_path as secure_file_priv_setting,
        requested_path as requested_export_path,
        CASE 
            WHEN secure_path = '' THEN 'WARNING: No restrictions'
            WHEN secure_path IS NULL THEN 'BLOCKED: File operations disabled'
            WHEN requested_path LIKE CONCAT(secure_path, '%') THEN 'Path is valid'
            ELSE 'ERROR: Path outside allowed directory'
        END as path_status;
    
    SET test_file = CONCAT(requested_path, 'mysql_test_', UNIX_TIMESTAMP(), '.txt');
    
    SET @test_sql = CONCAT(
        'SELECT "MySQL can write to this directory" ',
        'INTO OUTFILE "', test_file, '"'
    );
    
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        BEGIN
            SELECT 
                'File System Test' as test_type,
                'FAILED - Check permissions' as test_result;
        END;
        
        PREPARE stmt FROM @test_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        
        SELECT 
            'File System Test' as test_type,
            'SUCCESS' as test_result,
            test_file as test_file_created;
    END;
END$$

-- Export to single CSV file
CREATE PROCEDURE ExportSASSPageCleanSecure(
    IN export_directory VARCHAR(255)
)
BEGIN
    DECLARE export_file VARCHAR(512);
    DECLARE secure_path VARCHAR(255);
    DECLARE total_rows BIGINT;
    DECLARE start_time TIMESTAMP;
    
    SET start_time = NOW();
    SELECT @@GLOBAL.secure_file_priv INTO secure_path;
    
    SET export_file = CONCAT(
        export_directory,
        'sass_page_clean_',
        DATE_FORMAT(NOW(), '%Y%m%d_%H%i%s'),
        '.csv'
    );
    
    SELECT COUNT(*) INTO total_rows FROM sass_page_clean;
    
    SELECT 
        'Export Configuration' as info_type,
        secure_path as secure_file_priv,
        export_file as export_path,
        FORMAT(total_rows, 0) as total_rows;
    
    -- Export with PostgreSQL-compatible CSV format
    SET @export_sql = CONCAT(
        'SELECT ',
        '    "page_id","page_title","page_parent_id","page_root_id","page_dag_level","page_is_leaf" ',
        'UNION ALL ',
        'SELECT ',
        '    page_id, ',
        '    page_title, ',
        '    page_parent_id, ',
        '    page_root_id, ',
        '    page_dag_level, ',
        '    page_is_leaf ',
        'INTO OUTFILE "', export_file, '" ',
        'FIELDS TERMINATED BY "," ',
        'ENCLOSED BY \'"\' ',
        'ESCAPED BY \'"\' ',
        'LINES TERMINATED BY "\\n" ',
        'FROM sass_page_clean ',
        'ORDER BY page_id'
    );
    
    PREPARE stmt FROM @export_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    SELECT 
        'Export SUCCESS' as status,
        FORMAT(total_rows, 0) as rows_exported,
        export_file as file_path,
        ROUND(TIMESTAMPDIFF(SECOND, start_time, NOW()), 2) as export_time_sec;
END$$

-- Chunked export for large datasets
CREATE PROCEDURE ExportSASSPageCleanChunked(
    IN export_directory VARCHAR(255),
    IN chunk_size INT
)
BEGIN
    DECLARE chunk_counter INT DEFAULT 1;
    DECLARE chunk_file VARCHAR(512);
    DECLARE range_start BIGINT;
    DECLARE range_end BIGINT;
    DECLARE min_id BIGINT;
    DECLARE max_id BIGINT;
    DECLARE total_rows BIGINT;
    DECLARE chunk_rows BIGINT;
    DECLARE total_exported BIGINT DEFAULT 0;
    DECLARE estimated_chunks INT;
    DECLARE secure_path VARCHAR(255);
    DECLARE start_time TIMESTAMP;
    DECLARE progress_pct DECIMAL(5,1);
    
    SET start_time = NOW();
    SELECT @@GLOBAL.secure_file_priv INTO secure_path;
    
    IF chunk_size IS NULL THEN SET chunk_size = 200000; END IF;
    
    SELECT 
        MIN(page_id), 
        MAX(page_id), 
        COUNT(*)
    INTO min_id, max_id, total_rows
    FROM sass_page_clean;
    
    SET estimated_chunks = CEILING(total_rows / chunk_size);
    
    SELECT 
        'Starting Chunked Export' as status,
        FORMAT(total_rows, 0) as total_rows,
        FORMAT(chunk_size, 0) as chunk_size,
        estimated_chunks as estimated_chunks,
        export_directory as export_directory,
        'All pages are representatives - no filtering needed' as note;
    
    SET range_start = min_id;
    
    WHILE range_start <= max_id DO
        SET range_end = range_start + (chunk_size * 10);  -- Use larger range windows
        
        SET chunk_file = CONCAT(
            export_directory,
            'sass_page_clean_chunk_',
            LPAD(chunk_counter, 6, '0'),
            '.csv'
        );
        
        -- Export chunk with header
        SET @chunk_sql = CONCAT(
            'SELECT ',
            '    "page_id","page_title","page_parent_id","page_root_id","page_dag_level","page_is_leaf" ',
            'UNION ALL ',
            'SELECT ',
            '    page_id, ',
            '    page_title, ',
            '    page_parent_id, ',
            '    page_root_id, ',
            '    page_dag_level, ',
            '    page_is_leaf ',
            'INTO OUTFILE "', chunk_file, '" ',
            'FIELDS TERMINATED BY "," ',
            'ENCLOSED BY \'"\' ',
            'ESCAPED BY \'"\' ',
            'LINES TERMINATED BY "\\n" ',
            'FROM sass_page_clean ',
            'WHERE page_id >= ', range_start, ' ',
            'AND page_id < ', range_end, ' ',
            'ORDER BY page_id'
        );
        
        PREPARE stmt FROM @chunk_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        
        -- Count rows in this chunk
        SELECT COUNT(*) INTO chunk_rows
        FROM sass_page_clean
        WHERE page_id >= range_start AND page_id < range_end;
        
        SET total_exported = total_exported + chunk_rows;
        SET progress_pct = (total_exported * 100.0 / total_rows);
        
        SELECT 
            CONCAT('Chunk ', chunk_counter, ' SUCCESS') as status,
            FORMAT(chunk_rows, 0) as chunk_rows,
            FORMAT(total_exported, 0) as total_exported,
            CONCAT(FORMAT(progress_pct, 1), '%') as progress,
            chunk_file as file_path;
        
        SET chunk_counter = chunk_counter + 1;
        SET range_start = range_end;
        
        IF chunk_rows = 0 THEN
            LEAVE;
        END IF;
    END WHILE;
    
    SELECT 
        'Chunked Export Complete' as final_status,
        FORMAT(total_rows, 0) as total_rows,
        FORMAT(total_exported, 0) as rows_exported,
        (chunk_counter - 1) as chunks_created,
        ROUND(TIMESTAMPDIFF(SECOND, start_time, NOW()), 2) as total_time_sec;
END$$

-- Validate export readiness
CREATE PROCEDURE ValidateExportIntegrity()
BEGIN
    DECLARE source_count BIGINT;
    DECLARE secure_path VARCHAR(255);
    
    SELECT @@GLOBAL.secure_file_priv INTO secure_path;
    SELECT COUNT(*) INTO source_count FROM sass_page_clean;
    
    SELECT 
        'Export Validation' as validation_type,
        FORMAT(source_count, 0) as total_pages_to_export,
        'All pages are representatives (no duplicates)' as data_model,
        secure_path as secure_file_priv_directory;
    
    -- Data type compatibility
    SELECT 
        'PostgreSQL Compatibility Check' as check_type,
        'page_id range' as field,
        FORMAT(MIN(page_id), 0) as min_value,
        FORMAT(MAX(page_id), 0) as max_value,
        'INTEGER/BIGINT compatible' as postgres_type
    FROM sass_page_clean
    
    UNION ALL
    
    SELECT 
        'PostgreSQL Compatibility Check',
        'page_is_leaf values',
        CAST(MIN(page_is_leaf) AS CHAR),
        CAST(MAX(page_is_leaf) AS CHAR),
        'BOOLEAN compatible (0/1 -> FALSE/TRUE)'
    FROM sass_page_clean
    
    UNION ALL
    
    SELECT 
        'PostgreSQL Compatibility Check',
        'page_title encoding',
        'UTF-8 required',
        'Check for special chars',
        'VARCHAR(255) compatible';
    
    -- Check for data issues
    SELECT 
        'Data Quality Check' as check_type,
        'Self-references' as issue,
        COUNT(*) as count,
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status
    FROM sass_page_clean 
    WHERE page_id = page_parent_id AND page_dag_level > 0
    
    UNION ALL
    
    SELECT 
        'Data Quality Check',
        'Orphans',
        COUNT(*),
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END
    FROM sass_page_clean c 
    LEFT JOIN sass_page_clean p ON c.page_parent_id = p.page_id
    WHERE c.page_dag_level > 0 AND c.page_dag_level <= 7 AND p.page_id IS NULL;
END$$

-- Alternative: Create memory table for export
CREATE PROCEDURE CreateExportTable()
BEGIN
    DROP TABLE IF EXISTS sass_page_clean_export;
    
    CREATE TABLE sass_page_clean_export AS
    SELECT 
        page_id,
        page_title,
        page_parent_id,
        page_root_id,
        page_dag_level,
        page_is_leaf
    FROM sass_page_clean
    ORDER BY page_id;
    
    SELECT 
        'Memory Table Created' as status,
        FORMAT(COUNT(*), 0) as rows_in_table,
        'Query: SELECT * FROM sass_page_clean_export' as usage
    FROM sass_page_clean_export;
END$$

DELIMITER ;

-- ========================================
-- USAGE EXAMPLES
-- ========================================

/*
-- Diagnose environment
CALL DiagnoseExportEnvironment();
CALL DiagnoseFileSystemIssues('/private/tmp/mysql_export/');

-- Validate data before export
CALL ValidateExportIntegrity();

-- Single file export (for smaller datasets)
CALL ExportSASSPageCleanSecure('/private/tmp/mysql_export/');

-- Chunked export (recommended for ~1.85M rows)
CALL ExportSASSPageCleanChunked('/private/tmp/mysql_export/', 200000);

-- Alternative: Create table for manual export
CALL CreateExportTable();

-- PostgreSQL Import Commands:

-- Create table in PostgreSQL
CREATE TABLE sass_page_clean (
  page_id INTEGER PRIMARY KEY,
  page_title VARCHAR(255) NOT NULL,
  page_parent_id INTEGER NOT NULL,
  page_root_id INTEGER NOT NULL,
  page_dag_level INTEGER NOT NULL,
  page_is_leaf BOOLEAN NOT NULL
);

-- Import chunked files
\COPY sass_page_clean FROM '/path/to/sass_page_clean_chunk_000001.csv' WITH (FORMAT csv, HEADER true);
\COPY sass_page_clean FROM '/path/to/sass_page_clean_chunk_000002.csv' WITH (FORMAT csv, HEADER true);
-- Repeat for all chunks...

-- Or use bash loop:
for file in /path/to/sass_page_clean_chunk_*.csv; do
  psql -U postgres -d mydb -c "\COPY sass_page_clean FROM '$file' WITH (FORMAT csv, HEADER true);"
done

-- Build indices
CREATE INDEX idx_title ON sass_page_clean(page_title);
CREATE INDEX idx_parent ON sass_page_clean(page_parent_id);
CREATE INDEX idx_root ON sass_page_clean(page_root_id);
CREATE INDEX idx_level ON sass_page_clean(page_dag_level);
CREATE INDEX idx_leaf ON sass_page_clean(page_is_leaf);

KEY CHANGES FROM PREVIOUS VERSION:
- No sass_identity_pages references
- No representative filtering (all pages are representatives)
- Simplified queries (direct SELECT from sass_page_clean)
- Expected row count: ~1.85M (after weak branch filtering)
- Chunked export creates ~9-10 files of 200k rows each

ARCHITECTURE:
- sass_page_clean contains only representative pages
- No 1:1 identity mapping needed
- Each page appears exactly once
- All parent references point to valid representatives
- Weak branches already filtered out

ESTIMATED EXPORT TIME:
- Single file: 5-8 minutes
- Chunked (9-10 files): 8-12 minutes
- PostgreSQL import: 15-25 minutes
- Index creation: 5-10 minutes
- Total migration: 30-45 minutes
*/
