-- SASS Page Clean: MySQL to PostgreSQL Migration
-- Implements subset testing, chunked export, and optimized COPY import
-- Follows recommended 3-phase approach for safe, fast migration

-- ========================================
-- EXPORT CONFIGURATION
-- ========================================

-- Export tracking table
CREATE TABLE IF NOT EXISTS postgres_export_state (
  export_key VARCHAR(255) PRIMARY KEY,
  export_value VARCHAR(1000) NOT NULL,
  export_status ENUM('pending', 'running', 'completed', 'failed') DEFAULT 'pending',
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- File validation table
CREATE TABLE IF NOT EXISTS export_file_validation (
  file_id INT AUTO_INCREMENT PRIMARY KEY,
  file_name VARCHAR(500) NOT NULL,
  file_path VARCHAR(1000) NOT NULL,
  row_count INT NOT NULL,
  file_size_bytes BIGINT NOT NULL,
  checksum_md5 VARCHAR(32) NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_file_name (file_name)
) ENGINE=InnoDB;

-- ========================================
-- PHASE 1: SUBSET TESTING (1% SAMPLE)
-- ========================================

DROP PROCEDURE IF EXISTS ExportSASSPageCleanSubset;

DELIMITER //

CREATE PROCEDURE ExportSASSPageCleanSubset(
  IN p_export_path VARCHAR(1000),
  IN p_sample_percentage DECIMAL(5,2)
)
BEGIN
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_sample_count INT DEFAULT 0;
  DECLARE v_total_count INT DEFAULT 0;
  DECLARE v_file_path VARCHAR(1000);
  
  -- Set defaults
  IF p_sample_percentage IS NULL THEN SET p_sample_percentage = 1.0; END IF;
  
  SET v_start_time = UNIX_TIMESTAMP();
  SET v_file_path = CONCAT(p_export_path, '/sass_page_clean_subset_', REPLACE(p_sample_percentage, '.', '_'), 'pct.csv');
  
  -- Get total count for sampling calculation
  SELECT COUNT(*) INTO v_total_count FROM sass_page_clean;
  
  -- Update export state
  INSERT INTO postgres_export_state (export_key, export_value, export_status) 
  VALUES ('subset_export_status', 'Starting subset export', 'running')
  ON DUPLICATE KEY UPDATE export_value = 'Starting subset export', export_status = 'running';
  
  -- Export subset with systematic sampling
  SET @sql = CONCAT(
    'SELECT page_id, page_title, page_parent_id, page_root_id, page_dag_level, page_is_leaf ',
    'FROM sass_page_clean ',
    'WHERE page_id % ', CEIL(100.0 / p_sample_percentage), ' = 1 ',
    'INTO OUTFILE ''', v_file_path, ''' ',
    'CHARACTER SET utf8mb4 ',
    'FIELDS TERMINATED BY '','' ',
    'OPTIONALLY ENCLOSED BY ''"'' ',
    'ESCAPED BY ''\\\\'' ',
    'LINES TERMINATED BY ''\\n'''
  );
  
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
  -- Get exported row count
  SELECT ROW_COUNT() INTO v_sample_count;
  
  -- Record file validation info
  INSERT INTO export_file_validation (file_name, file_path, row_count, file_size_bytes)
  VALUES (
    CONCAT('sass_page_clean_subset_', REPLACE(p_sample_percentage, '.', '_'), 'pct.csv'),
    v_file_path,
    v_sample_count,
    0  -- File size to be updated externally
  );
  
  -- Update completion status
  INSERT INTO postgres_export_state (export_key, export_value, export_status) 
  VALUES ('subset_export_status', CONCAT('Completed: ', v_sample_count, ' rows'), 'completed')
  ON DUPLICATE KEY UPDATE export_value = CONCAT('Completed: ', v_sample_count, ' rows'), export_status = 'completed';
  
  -- Summary report
  SELECT 
    'Subset Export Complete' AS status,
    FORMAT(v_total_count, 0) AS total_rows_available,
    FORMAT(v_sample_count, 0) AS sample_rows_exported,
    CONCAT(ROUND(100.0 * v_sample_count / v_total_count, 2), '%') AS actual_sample_rate,
    v_file_path AS export_file_path,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS export_time_sec;
  
  -- Sample data preview
  SELECT 
    'Sample Data Preview' AS preview_type,
    page_id, page_title, page_parent_id, page_root_id, page_dag_level, page_is_leaf
  FROM sass_page_clean 
  WHERE page_id % CEIL(100.0 / p_sample_percentage) = 1
  LIMIT 5;

END//

DELIMITER ;

-- ========================================
-- PHASE 2: CHUNKED FULL EXPORT
-- ========================================

DROP PROCEDURE IF EXISTS ExportSASSPageCleanChunked;

DELIMITER //

CREATE PROCEDURE ExportSASSPageCleanChunked(
  IN p_export_path VARCHAR(1000),
  IN p_chunk_size INT
)
BEGIN
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_total_rows INT DEFAULT 0;
  DECLARE v_current_chunk INT DEFAULT 1;
  DECLARE v_rows_processed INT DEFAULT 0;
  DECLARE v_chunk_rows INT DEFAULT 0;
  DECLARE v_min_page_id INT DEFAULT 0;
  DECLARE v_max_page_id INT DEFAULT 0;
  DECLARE v_chunk_start INT DEFAULT 0;
  DECLARE v_chunk_end INT DEFAULT 0;
  DECLARE v_continue TINYINT(1) DEFAULT 1;
  DECLARE v_file_path VARCHAR(1000);
  
  -- Set defaults
  IF p_chunk_size IS NULL THEN SET p_chunk_size = 1000000; END IF;
  
  SET v_start_time = UNIX_TIMESTAMP();
  
  -- Get total count and page ID range
  SELECT COUNT(*), MIN(page_id), MAX(page_id) 
  INTO v_total_rows, v_min_page_id, v_max_page_id 
  FROM sass_page_clean;
  
  -- Clear previous export records
  DELETE FROM export_file_validation WHERE file_name LIKE 'sass_page_clean_chunk_%';
  
  -- Update export state
  INSERT INTO postgres_export_state (export_key, export_value, export_status) 
  VALUES ('chunked_export_status', 'Starting chunked export', 'running')
  ON DUPLICATE KEY UPDATE export_value = 'Starting chunked export', export_status = 'running';
  
  -- Progress report
  SELECT 
    'Starting Chunked Export' AS status,
    FORMAT(v_total_rows, 0) AS total_rows,
    p_chunk_size AS chunk_size,
    CEIL(v_total_rows / p_chunk_size) AS estimated_chunks;
  
  -- ========================================
  -- CHUNKED EXPORT LOOP
  -- ========================================
  
  SET v_chunk_start = v_min_page_id;
  
  WHILE v_continue = 1 DO
    SET v_chunk_end = v_chunk_start + p_chunk_size - 1;
    SET v_file_path = CONCAT(p_export_path, '/sass_page_clean_chunk_', LPAD(v_current_chunk, 4, '0'), '.csv');
    
    -- Export current chunk
    SET @sql = CONCAT(
      'SELECT page_id, page_title, page_parent_id, page_root_id, page_dag_level, page_is_leaf ',
      'FROM sass_page_clean ',
      'WHERE page_id BETWEEN ', v_chunk_start, ' AND ', v_chunk_end, ' ',
      'ORDER BY page_id ',
      'INTO OUTFILE ''', v_file_path, ''' ',
      'CHARACTER SET utf8mb4 ',
      'FIELDS TERMINATED BY '','' ',
      'OPTIONALLY ENCLOSED BY ''"'' ',
      'ESCAPED BY ''\\\\'' ',
      'LINES TERMINATED BY ''\\n'''
    );
    
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    SET v_chunk_rows = ROW_COUNT();
    DEALLOCATE PREPARE stmt;
    
    -- Record chunk validation info
    INSERT INTO export_file_validation (file_name, file_path, row_count, file_size_bytes)
    VALUES (
      CONCAT('sass_page_clean_chunk_', LPAD(v_current_chunk, 4, '0'), '.csv'),
      v_file_path,
      v_chunk_rows,
      0  -- File size to be updated externally
    );
    
    SET v_rows_processed = v_rows_processed + v_chunk_rows;
    
    -- Progress report
    SELECT 
      CONCAT('Chunk ', v_current_chunk, ' Complete') AS status,
      CONCAT(v_chunk_start, ' - ', v_chunk_end) AS page_id_range,
      FORMAT(v_chunk_rows, 0) AS chunk_rows,
      FORMAT(v_rows_processed, 0) AS total_processed,
      CONCAT(ROUND(100.0 * v_rows_processed / v_total_rows, 1), '%') AS progress,
      v_file_path AS chunk_file_path,
      ROUND(UNIX_TIMESTAMP() - v_start_time, 1) AS elapsed_sec;
    
    -- Check if done
    IF v_chunk_rows = 0 OR v_rows_processed >= v_total_rows THEN
      SET v_continue = 0;
    ELSE
      SET v_current_chunk = v_current_chunk + 1;
      -- Find next chunk start (handle gaps in page_id sequence)
      SELECT MIN(page_id) INTO v_chunk_start
      FROM sass_page_clean 
      WHERE page_id > v_chunk_end;
      
      IF v_chunk_start IS NULL THEN
        SET v_continue = 0;
      END IF;
    END IF;
    
  END WHILE;
  
  -- Update completion status
  INSERT INTO postgres_export_state (export_key, export_value, export_status) 
  VALUES ('chunked_export_status', CONCAT('Completed: ', v_current_chunk - 1, ' chunks, ', v_rows_processed, ' rows'), 'completed')
  ON DUPLICATE KEY UPDATE export_value = CONCAT('Completed: ', v_current_chunk - 1, ' chunks, ', v_rows_processed, ' rows'), export_status = 'completed';
  
  -- Final summary
  SELECT 
    'Chunked Export Complete' AS final_status,
    FORMAT(v_total_rows, 0) AS total_rows_in_table,
    FORMAT(v_rows_processed, 0) AS total_rows_exported,
    v_current_chunk - 1 AS chunks_created,
    ROUND(v_rows_processed / (v_current_chunk - 1), 0) AS avg_rows_per_chunk,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_export_time_sec;
  
  -- Chunk summary
  SELECT 
    'Export File Summary' AS summary_type,
    file_name,
    FORMAT(row_count, 0) AS rows_in_file,
    file_path
  FROM export_file_validation 
  WHERE file_name LIKE 'sass_page_clean_chunk_%'
  ORDER BY file_name;

END//

DELIMITER ;

-- ========================================
-- PHASE 3: POSTGRESQL IMPORT SCRIPTS
-- ========================================

-- Generate PostgreSQL table creation script
DROP PROCEDURE IF EXISTS GeneratePostgreSQLTableScript;

DELIMITER //

CREATE PROCEDURE GeneratePostgreSQLTableScript()
BEGIN
  SELECT '-- PostgreSQL Table Creation (No Indices)' AS script_section
  UNION ALL
  SELECT 'DROP TABLE IF EXISTS sass_page_clean;'
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT 'CREATE TABLE sass_page_clean ('
  UNION ALL
  SELECT '  page_id BIGINT NOT NULL,'
  UNION ALL
  SELECT '  page_title VARCHAR(255) NOT NULL,'
  UNION ALL
  SELECT '  page_parent_id INTEGER NOT NULL,'
  UNION ALL
  SELECT '  page_root_id INTEGER NOT NULL,'
  UNION ALL
  SELECT '  page_dag_level INTEGER NOT NULL,'
  UNION ALL
  SELECT '  page_is_leaf BOOLEAN NOT NULL DEFAULT FALSE'
  UNION ALL
  SELECT ');'
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT '-- Data Import Commands'
  UNION ALL
  SELECT "\\COPY sass_page_clean FROM '/path/to/sass_page_clean_subset_1_0pct.csv' WITH (FORMAT CSV, ENCODING 'UTF8');"
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT '-- For chunked import, repeat for each chunk:'
  UNION ALL
  SELECT "-- \\COPY sass_page_clean FROM '/path/to/sass_page_clean_chunk_0001.csv' WITH (FORMAT CSV, ENCODING 'UTF8');"
  UNION ALL
  SELECT "-- \\COPY sass_page_clean FROM '/path/to/sass_page_clean_chunk_0002.csv' WITH (FORMAT CSV, ENCODING 'UTF8');"
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT '-- Post-Load Index Creation'
  UNION ALL
  SELECT 'CREATE UNIQUE INDEX CONCURRENTLY idx_sass_page_clean_pkey ON sass_page_clean (page_id);'
  UNION ALL
  SELECT 'ALTER TABLE sass_page_clean ADD CONSTRAINT sass_page_clean_pkey PRIMARY KEY USING INDEX idx_sass_page_clean_pkey;'
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT 'CREATE INDEX CONCURRENTLY idx_sass_page_clean_title ON sass_page_clean (page_title);'
  UNION ALL
  SELECT 'CREATE INDEX CONCURRENTLY idx_sass_page_clean_parent ON sass_page_clean (page_parent_id);'
  UNION ALL
  SELECT 'CREATE INDEX CONCURRENTLY idx_sass_page_clean_root ON sass_page_clean (page_root_id);'
  UNION ALL
  SELECT 'CREATE INDEX CONCURRENTLY idx_sass_page_clean_level ON sass_page_clean (page_dag_level);'
  UNION ALL
  SELECT 'CREATE INDEX CONCURRENTLY idx_sass_page_clean_leaf ON sass_page_clean (page_is_leaf);'
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT '-- Validation Queries'
  UNION ALL
  SELECT 'SELECT COUNT(*) as total_rows FROM sass_page_clean;'
  UNION ALL
  SELECT 'SELECT page_dag_level, COUNT(*) as level_count FROM sass_page_clean GROUP BY page_dag_level ORDER BY page_dag_level;'
  UNION ALL
  SELECT 'SELECT page_is_leaf, COUNT(*) as leaf_count FROM sass_page_clean GROUP BY page_is_leaf;';

END//

DELIMITER ;

-- Generate chunk-specific PostgreSQL import script
DROP PROCEDURE IF EXISTS GeneratePostgreSQLImportScript;

DELIMITER //

CREATE PROCEDURE GeneratePostgreSQLImportScript(
  IN p_import_path VARCHAR(1000)
)
BEGIN
  DECLARE v_chunk_count INT DEFAULT 0;
  DECLARE v_current_chunk INT DEFAULT 1;
  
  -- Get chunk count
  SELECT COUNT(*) INTO v_chunk_count
  FROM export_file_validation 
  WHERE file_name LIKE 'sass_page_clean_chunk_%';
  
  -- Header
  SELECT '-- PostgreSQL Import Script for Chunked Files' AS import_script
  UNION ALL
  SELECT CONCAT('-- Total chunks to import: ', v_chunk_count)
  UNION ALL
  SELECT CONCAT('-- Import path: ', COALESCE(p_import_path, '/path/to/export/files/'))
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT '-- Start transaction'
  UNION ALL
  SELECT 'BEGIN;'
  UNION ALL
  SELECT '';
  
  -- Generate COPY commands for each chunk
  WHILE v_current_chunk <= v_chunk_count DO
    SELECT CONCAT('\\COPY sass_page_clean FROM ''', 
                  COALESCE(p_import_path, '/path/to/export/files/'), 
                  'sass_page_clean_chunk_', LPAD(v_current_chunk, 4, '0'), 
                  '.csv'' WITH (FORMAT CSV, ENCODING ''UTF8'');') AS import_command;
    
    SET v_current_chunk = v_current_chunk + 1;
  END WHILE;
  
  -- Footer
  SELECT ''
  UNION ALL
  SELECT '-- Commit transaction'
  UNION ALL
  SELECT 'COMMIT;'
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT '-- Verify import'
  UNION ALL
  SELECT 'SELECT COUNT(*) as imported_rows FROM sass_page_clean;';

END//

DELIMITER ;

-- ========================================
-- VALIDATION AND UTILITY PROCEDURES
-- ========================================

DROP PROCEDURE IF EXISTS ValidateExportIntegrity;

DELIMITER //

CREATE PROCEDURE ValidateExportIntegrity()
BEGIN
  DECLARE v_total_source_rows INT DEFAULT 0;
  DECLARE v_total_export_rows INT DEFAULT 0;
  
  -- Get source table count
  SELECT COUNT(*) INTO v_total_source_rows FROM sass_page_clean;
  
  -- Get total exported rows
  SELECT COALESCE(SUM(row_count), 0) INTO v_total_export_rows
  FROM export_file_validation 
  WHERE file_name LIKE 'sass_page_clean_chunk_%';
  
  -- Validation report
  SELECT 
    'Export Integrity Validation' AS validation_type,
    FORMAT(v_total_source_rows, 0) AS source_table_rows,
    FORMAT(v_total_export_rows, 0) AS exported_file_rows,
    FORMAT(v_total_source_rows - v_total_export_rows, 0) AS row_difference,
    CASE 
      WHEN v_total_source_rows = v_total_export_rows THEN 'PASS'
      ELSE 'FAIL'
    END AS validation_status;
  
  -- Export file summary
  SELECT 
    'Export File Details' AS file_details,
    file_name,
    FORMAT(row_count, 0) AS rows,
    FORMAT(file_size_bytes, 0) AS bytes,
    created_at
  FROM export_file_validation 
  WHERE file_name LIKE 'sass_page_clean_%'
  ORDER BY file_name;
  
  -- Data type analysis for PostgreSQL compatibility
  SELECT 
    'Data Type Compatibility Check' AS compatibility_check,
    'page_id range' AS field_check,
    FORMAT(MIN(page_id), 0) AS min_value,
    FORMAT(MAX(page_id), 0) AS max_value,
    'BIGINT compatible' AS postgres_type_status
  FROM sass_page_clean
  
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
    'VARCHAR(255) compatible'
  FROM DUAL;

END//

DELIMITER ;

-- Data encoding validation
DROP PROCEDURE IF EXISTS ValidateDataEncoding;

DELIMITER //

CREATE PROCEDURE ValidateDataEncoding()
BEGIN
  -- Check for problematic characters that might cause CSV issues
  SELECT 
    'Encoding Validation' AS validation_type,
    'Characters requiring CSV escaping' AS issue_type,
    COUNT(*) AS affected_rows,
    'Quotes, commas, newlines in page_title' AS description
  FROM sass_page_clean 
  WHERE page_title LIKE '%"%' 
     OR page_title LIKE '%,%' 
     OR page_title LIKE '%\n%' 
     OR page_title LIKE '%\r%'
  
  UNION ALL
  
  SELECT 
    'Encoding Validation',
    'Non-ASCII characters',
    COUNT(*),
    'Unicode characters in page_title'
  FROM sass_page_clean 
  WHERE page_title REGEXP '[^\x00-\x7F]'
  
  UNION ALL
  
  SELECT 
    'Encoding Validation',
    'Very long titles',
    COUNT(*),
    'Titles approaching VARCHAR(255) limit'
  FROM sass_page_clean 
  WHERE LENGTH(page_title) > 240;
  
  -- Sample problematic titles
  SELECT 
    'Sample Problematic Titles' AS sample_type,
    page_id,
    page_title,
    LENGTH(page_title) AS title_length,
    'Contains CSV special chars' AS issue
  FROM sass_page_clean 
  WHERE (page_title LIKE '%"%' OR page_title LIKE '%,%' OR page_title LIKE '%\n%')
  LIMIT 10;

END//

DELIMITER ;

-- ========================================
-- USAGE EXAMPLES AND DOCUMENTATION
-- ========================================

/*
-- MIGRATION WORKFLOW EXAMPLES

-- Phase 1: Test with 1% subset
CALL ExportSASSPageCleanSubset('/tmp/postgres_export', 1.0);

-- Phase 2: Full chunked export (1M rows per chunk)
CALL ExportSASSPageCleanChunked('/tmp/postgres_export', 1000000);

-- Validate export integrity
CALL ValidateExportIntegrity();
CALL ValidateDataEncoding();

-- Generate PostgreSQL scripts
CALL GeneratePostgreSQLTableScript();
CALL GeneratePostgreSQLImportScript('/tmp/postgres_export/');

-- Check export status
SELECT * FROM postgres_export_state ORDER BY updated_at DESC;
SELECT * FROM export_file_validation ORDER BY file_name;

-- PostgreSQL Import Process:
-- 1. Create table without indices using generated script
-- 2. Import subset for testing: \COPY sass_page_clean FROM 'subset.csv' WITH (FORMAT CSV, ENCODING 'UTF8');
-- 3. Validate subset data and performance
-- 4. Drop table, recreate for full load
-- 5. Import all chunks sequentially or in parallel
-- 6. Create indices using CONCURRENTLY option
-- 7. Add primary key constraint
-- 8. Run validation queries

-- Performance Notes:
-- - CSV format with UTF-8 encoding ensures PostgreSQL compatibility
-- - Chunked exports enable parallel processing and memory management
-- - COPY FROM is 10-100x faster than INSERT statements
-- - CREATE INDEX CONCURRENTLY allows non-blocking index creation
-- - File compression (gzip) recommended for transfer efficiency

-- Data Type Mappings:
-- MySQL INT UNSIGNED -> PostgreSQL BIGINT (safe for all positive integers)
-- MySQL TINYINT(1) -> PostgreSQL BOOLEAN (0/1 auto-converts to FALSE/TRUE)
-- MySQL VARCHAR(255) -> PostgreSQL VARCHAR(255) (identical)
*/
