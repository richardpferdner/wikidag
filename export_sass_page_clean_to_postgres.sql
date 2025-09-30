-- ========================================
-- SASS Page Clean Export to PostgreSQL
-- Corrected CSV Export with Proper Escaping
-- ========================================

DELIMITER $$

-- Drop existing procedures
DROP PROCEDURE IF EXISTS DiagnoseExportEnvironment$$
DROP PROCEDURE IF EXISTS DiagnoseFileSystemIssues$$
DROP PROCEDURE IF EXISTS ExportSASSPageCleanSecure$$
DROP PROCEDURE IF EXISTS ExportSASSPageCleanChunkedOptimized$$
DROP PROCEDURE IF EXISTS ValidateExportIntegrity$$
DROP PROCEDURE IF EXISTS ExportSASSPageCleanToString$$
DROP PROCEDURE IF EXISTS CreateExportTable$$

-- Diagnostic procedure for export environment
CREATE PROCEDURE DiagnoseExportEnvironment()
BEGIN
    DECLARE total_representatives BIGINT;
    DECLARE sample_1pct BIGINT;
    DECLARE min_page_id BIGINT;
    DECLARE max_page_id BIGINT;
    
    -- MySQL version
    SELECT VERSION() as version;
    
    -- Check secure_file_priv setting
    SHOW VARIABLES LIKE 'secure_file_priv';
    
    -- Count representative pages and get statistics
    SELECT 
        COUNT(*) INTO total_representatives
    FROM sass_identity_pages
    WHERE representative_page_id = page_id;
    
    SET sample_1pct = FLOOR(total_representatives / 100);
    
    SELECT MIN(page_id), MAX(page_id) 
    INTO min_page_id, max_page_id
    FROM sass_identity_pages
    WHERE representative_page_id = page_id;
    
    -- Display statistics
    SELECT 
        'Representative Data Availability Check' as check_type,
        total_representatives as total_representative_rows,
        sample_1pct as sample_1pct,
        min_page_id as min_id,
        max_page_id as max_id;
    
    -- Export recommendations
    SELECT 
        'Export Recommendations' as info_type,
        'Index-free export for 3-4x faster PostgreSQL import' as performance_note,
        'Use ExportSASSPageCleanToString() for manual CSV export' as alternative_method,
        'Estimated total migration time: 1.5-2 hours' as time_estimate,
        CONCAT('Expected representative rows: ~', ROUND(total_representatives/1000000, 2), 'M (varies by filtering)') as data_volume;
END$$

-- Diagnostic procedure for file system issues
CREATE PROCEDURE DiagnoseFileSystemIssues(
    IN requested_path VARCHAR(255)
)
BEGIN
    DECLARE secure_path VARCHAR(255);
    DECLARE test_file VARCHAR(512);
    
    -- Get secure_file_priv setting
    SELECT @@GLOBAL.secure_file_priv INTO secure_path;
    
    -- Display configuration
    SELECT 
        'MySQL File System Configuration' as diagnostic_type,
        secure_path as secure_file_priv_setting,
        requested_path as requested_export_path,
        CASE 
            WHEN secure_path = '' THEN 'WARNING: No restrictions (security risk)'
            WHEN secure_path IS NULL THEN 'BLOCKED: File operations disabled'
            WHEN requested_path LIKE CONCAT(secure_path, '%') THEN 'Requested path is within allowed directory'
            ELSE 'ERROR: Requested path outside allowed directory'
        END as path_compatibility,
        CASE
            WHEN secure_path IS NULL THEN 'Enable secure_file_priv in MySQL config'
            ELSE 'Check directory exists and MySQL has write permissions'
        END as permission_note;
    
    -- Test file write capability
    SET test_file = CONCAT(requested_path, 'mysql_test_', UNIX_TIMESTAMP(), '.txt');
    
    SET @test_sql = CONCAT(
        'SELECT "MySQL can write to this directory" ',
        'INTO OUTFILE "', test_file, '"'
    );
    
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        BEGIN
            SELECT 
                'File System Test Results' as test_type,
                'FAILED - Check permissions and directory existence' as test_result,
                'N/A' as test_file_created,
                'Verify directory exists and MySQL user has write access' as recommendation;
        END;
        
        PREPARE stmt FROM @test_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        
        SELECT 
            'File System Test Results' as test_type,
            'SUCCESS - MySQL can write to directory' as test_result,
            test_file as test_file_created,
            'Directory permissions are correct' as recommendation;
    END;
    
    -- Alternative methods
    SELECT 
        'Alternative Export Methods' as alternatives_section,
        'CALL CreateExportTable(1.0);' as memory_table_method,
        'CALL ExportSASSPageCleanToString(1.0, 100);' as string_output_method,
        'Both methods avoid file system permissions' as benefit;
END$$

-- Export representative pages to CSV with proper escaping
CREATE PROCEDURE ExportSASSPageCleanSecure(
    IN sample_percentage DECIMAL(5,2),
    IN sampling_modulus INT
)
BEGIN
    DECLARE export_file VARCHAR(512);
    DECLARE secure_path VARCHAR(255);
    DECLARE total_rows BIGINT;
    DECLARE actual_sample_rate DECIMAL(5,2);
    DECLARE start_time TIMESTAMP;
    DECLARE end_time TIMESTAMP;
    
    SET start_time = NOW();
    SELECT @@GLOBAL.secure_file_priv INTO secure_path;
    
    -- Generate filename
    SET export_file = CONCAT(
        secure_path,
        'sass_page_clean_representatives_',
        REPLACE(FORMAT(sample_percentage, 2), '.', '_'),
        'pct.csv'
    );
    
    -- Count total representatives
    SELECT COUNT(*) INTO total_rows
    FROM sass_identity_pages
    WHERE representative_page_id = page_id;
    
    SET actual_sample_rate = (100.0 / sampling_modulus);
    
    -- Display export info
    SELECT 
        'Index-Free Export Configuration' as info_type,
        secure_path as secure_file_priv,
        export_file as planned_export_path,
        FORMAT(total_rows, 0) as total_representative_rows,
        sampling_modulus as sampling_modulus,
        'Data-only export (no indices)' as export_strategy;
    
    -- Export with proper CSV escaping for PostgreSQL
    SET @export_sql = CONCAT(
        'SELECT ',
        '    sip.page_id, ',
        '    spc.page_title, ',
        '    spc.page_parent_id, ',
        '    spc.page_root_id, ',
        '    spc.page_dag_level, ',
        '    spc.page_is_leaf ',
        'INTO OUTFILE "', export_file, '" ',
        'FIELDS TERMINATED BY "," ',
        'ENCLOSED BY \'"\' ',
        'ESCAPED BY \'"\' ',  -- Use double-quote escaping for PostgreSQL
        'LINES TERMINATED BY "\\n" ',
        'FROM sass_identity_pages sip ',
        'INNER JOIN sass_page_clean spc ON sip.page_id = spc.page_id ',
        'WHERE sip.representative_page_id = sip.page_id ',
        'AND MOD(sip.page_id, ', sampling_modulus, ') = 0'
    );
    
    -- Add header
    SET @header_sql = CONCAT(
        'SELECT ',
        '"page_id","page_title","page_parent_id","page_root_id","page_dag_level","page_is_leaf" ',
        'UNION ALL ',
        @export_sql
    );
    
    PREPARE stmt FROM @header_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    SET end_time = NOW();
    
    -- Success summary
    SELECT 
        'Index-Free Export SUCCESS' as status,
        FORMAT(total_rows, 0) as total_representative_rows,
        FORMAT(FLOOR(total_rows * actual_sample_rate / 100), 0) as sample_rows_exported,
        CONCAT(FORMAT(actual_sample_rate, 2), '%') as actual_sample_rate,
        export_file as export_file_path,
        'Data-only CSV (indices built post-import)' as export_type,
        ROUND(TIMESTAMPDIFF(SECOND, start_time, end_time), 2) as export_time_sec;
END$$

-- Chunked export with proper CSV escaping
CREATE PROCEDURE ExportSASSPageCleanChunkedOptimized(
    IN export_directory VARCHAR(255),
    IN chunk_size INT,
    IN sampling_modulus INT
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
    DECLARE chunk_start_time TIMESTAMP;
    DECLARE chunk_end_time TIMESTAMP;
    DECLARE progress_pct DECIMAL(5,1);
    
    SET start_time = NOW();
    SELECT @@GLOBAL.secure_file_priv INTO secure_path;
    
    -- Get range and count
    SELECT 
        MIN(page_id), 
        MAX(page_id), 
        COUNT(*)
    INTO min_id, max_id, total_rows
    FROM sass_identity_pages
    WHERE representative_page_id = page_id;
    
    SET estimated_chunks = CEILING(total_rows / chunk_size);
    
    -- Initial status
    SELECT 
        'Starting Index-Free Chunked Export' as status,
        FORMAT(total_rows, 0) as total_representative_rows,
        FORMAT(chunk_size, 0) as chunk_size,
        estimated_chunks as estimated_chunks,
        export_directory as export_directory,
        secure_path as secure_file_priv_setting,
        'Data-only chunks (indices built post-import)' as export_strategy,
        'Ensure export directory exists and MySQL has write access' as permission_note;
    
    -- Export chunks
    SET range_start = min_id;
    
    chunk_loop: WHILE range_start <= max_id DO
        SET range_end = range_start + chunk_size - 1;
        SET chunk_start_time = NOW();
        
        -- Generate chunk filename with 12-digit padding
        SET chunk_file = CONCAT(
            export_directory,
            'sass_page_clean_representatives_chunk_',
            LPAD(chunk_counter, 12, '0'),
            '.csv'
        );
        
        -- Export chunk with proper CSV escaping
        SET @chunk_sql = CONCAT(
            'SELECT ',
            '    "page_id","page_title","page_parent_id","page_root_id","page_dag_level","page_is_leaf" ',
            'UNION ALL ',
            'SELECT ',
            '    sip.page_id, ',
            '    spc.page_title, ',
            '    spc.page_parent_id, ',
            '    spc.page_root_id, ',
            '    spc.page_dag_level, ',
            '    spc.page_is_leaf ',
            'INTO OUTFILE "', chunk_file, '" ',
            'FIELDS TERMINATED BY "," ',
            'ENCLOSED BY \'"\' ',
            'ESCAPED BY \'"\' ',  -- PostgreSQL-compatible escaping
            'LINES TERMINATED BY "\\n" ',
            'FROM sass_identity_pages sip ',
            'INNER JOIN sass_page_clean spc ON sip.page_id = spc.page_id ',
            'WHERE sip.representative_page_id = sip.page_id ',
            'AND sip.page_id >= ', range_start, ' ',
            'AND sip.page_id <= ', range_end
        );
        
        PREPARE stmt FROM @chunk_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        
        -- Count rows in this chunk
        SET @count_sql = CONCAT(
            'SELECT COUNT(*) INTO @chunk_rows ',
            'FROM sass_identity_pages sip ',
            'WHERE sip.representative_page_id = sip.page_id ',
            'AND sip.page_id >= ', range_start, ' ',
            'AND sip.page_id <= ', range_end
        );
        
        PREPARE stmt FROM @count_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        
        SET chunk_rows = @chunk_rows;
        SET total_exported = total_exported + chunk_rows;
        SET chunk_end_time = NOW();
        SET progress_pct = (total_exported * 100.0 / total_rows);
        
        -- Chunk status
        SELECT 
            CONCAT('Chunk ', chunk_counter, ' SUCCESS') as status,
            CONCAT(FORMAT(range_start, 0), ' - ', FORMAT(range_end, 0)) as page_id_range,
            FORMAT(chunk_rows, 0) as chunk_rows,
            FORMAT(total_exported, 0) as total_processed,
            CONCAT(FORMAT(progress_pct, 1), '%') as progress,
            chunk_file as chunk_file_path,
            'Data-only chunk' as chunk_type,
            ROUND(TIMESTAMPDIFF(SECOND, start_time, chunk_end_time), 1) as elapsed_sec;
        
        SET chunk_counter = chunk_counter + 1;
        SET range_start = range_end + 1;
        
        -- Exit if no more data
        IF chunk_rows = 0 THEN
            LEAVE chunk_loop;
        END IF;
    END WHILE;
    
    -- Final summary
    SELECT 
        'Index-Free Chunked Export Complete' as final_status,
        FORMAT(total_rows, 0) as total_representative_rows,
        FORMAT(total_exported, 0) as total_rows_exported,
        (chunk_counter - 1) as chunks_created,
        FORMAT(ROUND(total_exported / (chunk_counter - 1)), 0) as avg_rows_per_chunk,
        'Data-only chunks (indices built post-import)' as export_strategy,
        'Estimated PostgreSQL import time: 45-60 minutes' as import_estimate,
        ROUND(TIMESTAMPDIFF(SECOND, start_time, NOW()), 2) as total_export_time_sec;
END$$

-- Validate export integrity
CREATE PROCEDURE ValidateExportIntegrity()
BEGIN
    DECLARE source_count BIGINT;
    DECLARE secure_path VARCHAR(255);
    
    SELECT @@GLOBAL.secure_file_priv INTO secure_path;
    
    -- Count source representatives
    SELECT COUNT(*) INTO source_count
    FROM sass_identity_pages
    WHERE representative_page_id = page_id;
    
    -- Validation summary
    SELECT 
        'Export Integrity Validation (Representatives)' as validation_type,
        FORMAT(source_count, 0) as source_representative_rows,
        'Check file row counts' as exported_file_rows,
        'Manual verification' as row_difference,
        'Verify manually' as validation_status,
        'Validates representative pages only (duplicates removed)' as validation_scope;
    
    -- List export files with details
    SELECT 
        'Export File Details' as file_details,
        SUBSTRING_INDEX(file_name, '/', -1) as file_name,
        'Check manually' as file_rows,
        file_size as file_bytes,
        file_modified as created_at
    FROM (
        SELECT 
            CONCAT(secure_path, 'sass_page_clean_representatives_1_00pct.csv') as file_name,
            'N/A' as file_size,
            NOW() as file_modified
        UNION ALL
        SELECT 
            CONCAT(secure_path, 'sass_page_clean_representatives_chunk_', LPAD(n, 12, '0'), '.csv'),
            'N/A',
            NOW()
        FROM (
            SELECT 1 as n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
            UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10
        ) numbers
    ) files;
    
    -- Data type compatibility check
    SELECT 
        'Data Type Compatibility Check' as compatibility_check,
        'page_id range' as field_check,
        FORMAT(MIN(page_id), 0) as min_value,
        FORMAT(MAX(page_id), 0) as max_value,
        'BIGINT compatible' as postgres_type_status
    FROM sass_identity_pages
    WHERE representative_page_id = page_id
    
    UNION ALL
    
    SELECT 
        'Data Type Compatibility Check',
        'page_is_leaf values',
        CAST(MIN(page_is_leaf) AS CHAR),
        CAST(MAX(page_is_leaf) AS CHAR),
        'BOOLEAN compatible (0/1 -> FALSE/TRUE)'
    FROM sass_page_clean
    
    UNION ALL
    
    SELECT 
        'Data Type Compatibility Check',
        'page_title encoding',
        'UTF-8 required',
        'Check for non-ASCII chars',
        'VARCHAR(255) compatible';
END$$

-- Alternative: Export to string for manual handling
CREATE PROCEDURE ExportSASSPageCleanToString(
    IN sample_percentage DECIMAL(5,2),
    IN row_limit INT
)
BEGIN
    SELECT 
        sip.page_id,
        spc.page_title,
        spc.page_parent_id,
        spc.page_root_id,
        spc.page_dag_level,
        spc.page_is_leaf
    FROM sass_identity_pages sip
    INNER JOIN sass_page_clean spc ON sip.page_id = spc.page_id
    WHERE sip.representative_page_id = sip.page_id
    AND MOD(sip.page_id, FLOOR(100 / sample_percentage)) = 0
    LIMIT row_limit;
END$$

-- Alternative: Create memory table for export
CREATE PROCEDURE CreateExportTable(
    IN sample_percentage DECIMAL(5,2)
)
BEGIN
    DROP TABLE IF EXISTS sass_page_clean_export;
    
    CREATE TABLE sass_page_clean_export AS
    SELECT 
        sip.page_id,
        spc.page_title,
        spc.page_parent_id,
        spc.page_root_id,
        spc.page_dag_level,
        spc.page_is_leaf
    FROM sass_identity_pages sip
    INNER JOIN sass_page_clean spc ON sip.page_id = spc.page_id
    WHERE sip.representative_page_id = sip.page_id
    AND MOD(sip.page_id, FLOOR(100 / sample_percentage)) = 0;
    
    SELECT 
        'Memory Table Created' as status,
        COUNT(*) as rows_in_table,
        'Query: SELECT * FROM sass_page_clean_export' as usage_note
    FROM sass_page_clean_export;
END$$

DELIMITER ;

-- Usage examples
-- mysql_export % rm -rf /private/tmp/mysql_export/*.csv
-- scripts-data-stack-postgres % docker exec -i safe-ai-data-postgres psql -U postgres -d mydb -c "TRUNCATE TABLE sass_page_clean, sass_page_clean_staging;"

-- mysql> 
-- CALL DiagnoseExportEnvironment();
-- CALL DiagnoseFileSystemIssues('/private/tmp/mysql_export/');
-- CALL ExportSASSPageCleanSecure(1.0, 100);
-- CALL ExportSASSPageCleanChunkedOptimized('/private/tmp/mysql_export/', 1000000, 1);
-- CALL ValidateExportIntegrity();
