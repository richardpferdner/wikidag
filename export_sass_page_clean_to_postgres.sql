-- SASS Page Clean: MySQL to PostgreSQL Migration - FIXED VERSION
-- Implements secure file handling, alternative export methods, and better error detection
-- Addresses MySQL secure_file_priv restrictions and file permission issues

-- ========================================
-- EXPORT CONFIGURATION
-- ========================================

-- Export tracking table
CREATE TABLE IF NOT EXISTS postgres_export_state (
  export_key VARCHAR(255) PRIMARY KEY,
  export_value VARCHAR(1000) NOT NULL,
  export_status ENUM('pending', 'running', 'completed', 'failed') DEFAULT 'pending',
  error_message TEXT NULL,
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
-- SYSTEM DIAGNOSTICS
-- ========================================

DROP PROCEDURE IF EXISTS DiagnoseExportEnvironment;

DELIMITER //

CREATE PROCEDURE DiagnoseExportEnvironment()
BEGIN
  -- Show MySQL version and secure_file_priv setting
  SHOW VARIABLES LIKE 'version';
  SHOW VARIABLES LIKE 'secure_file_priv';
  
  -- Test data availability
  SELECT 'Data Availability Check' AS check_type,
         COUNT(*) AS total_rows,
         COUNT(CASE WHEN page_id % 100 = 1 THEN 1 END) AS sample_1pct,
         MIN(page_id) AS min_id,
         MAX(page_id) AS max_id
  FROM sass_page_clean;
  
  -- Show export recommendations
  SELECT 'Export Recommendations' AS info_type,
         'If secure_file_priv is NULL, file exports are disabled' AS file_export_note,
         'Use ExportSASSPageCleanToString() for manual CSV export' AS alternative_method;
END//

DELIMITER ;

-- ========================================
-- ALTERNATIVE EXPORT METHOD - SELECT INTO STRING
-- ========================================

DROP PROCEDURE IF EXISTS ExportSASSPageCleanToString;

DELIMITER //

CREATE PROCEDURE ExportSASSPageCleanToString(
  IN p_sample_percentage DECIMAL(5,2),
  IN p_max_rows INT
)
BEGIN
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_sample_count INT DEFAULT 0;
  DECLARE v_total_count INT DEFAULT 0;
  DECLARE v_sampling_mod INT;
  
  -- Set defaults
  IF p_sample_percentage IS NULL THEN SET p_sample_percentage = 1.0; END IF;
  IF p_max_rows IS NULL THEN SET p_max_rows = 100; END IF;
  
  SET v_start_time = UNIX_TIMESTAMP();
  SET v_sampling_mod = CEIL(100.0 / p_sample_percentage);
  
  -- Get total count for sampling calculation
  SELECT COUNT(*) INTO v_total_count FROM sass_page_clean;
  
  -- Get sample count
  SELECT COUNT(*) INTO v_sample_count 
  FROM sass_page_clean 
  WHERE page_id % v_sampling_mod = 1;
  
  -- Update export state
  INSERT INTO postgres_export_state (export_key, export_value, export_status) 
  VALUES ('string_export_status', 'Generating CSV string output', 'running')
  ON DUPLICATE KEY UPDATE export_value = 'Generating CSV string output', export_status = 'running';
  
  -- Display sampling info
  SELECT 
    'String Export Configuration' AS info_type,
    FORMAT(v_total_count, 0) AS total_rows_available,
    FORMAT(v_sample_count, 0) AS sample_rows_to_export,
    CONCAT(ROUND(100.0 * v_sample_count / v_total_count, 2), '%') AS actual_sample_rate,
    v_sampling_mod AS sampling_modulus,
    p_max_rows AS max_output_rows;
  
  -- Generate CSV header
  SELECT 'CSV Export Data - Copy to PostgreSQL' AS export_section,
         'page_id,page_title,page_parent_id,page_root_id,page_dag_level,page_is_leaf' AS csv_header;
  
  -- Generate CSV data rows
  SELECT CONCAT(
    page_id, ',',
    '"', REPLACE(REPLACE(page_title, '"', '""'), ',', '\,'), '",',
    page_parent_id, ',',
    page_root_id, ',',
    page_dag_level, ',',
    CASE WHEN page_is_leaf = 1 THEN 'true' ELSE 'false' END
  ) AS csv_row
  FROM sass_page_clean 
  WHERE page_id % v_sampling_mod = 1
  ORDER BY page_id
  LIMIT p_max_rows;
  
  -- Update completion status
  INSERT INTO postgres_export_state (export_key, export_value, export_status) 
  VALUES ('string_export_status', 'String export completed', 'completed')
  ON DUPLICATE KEY UPDATE export_value = 'String export completed', export_status = 'completed';
  
  -- Summary
  SELECT 
    'String Export Complete' AS status,
    FORMAT(LEAST(v_sample_count, p_max_rows), 0) AS rows_displayed,
    FORMAT(v_sample_count, 0) AS total_sample_rows_available,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS export_time_sec,
    'Copy output to .csv file for PostgreSQL import' AS next_step;

END//

DELIMITER ;

-- ========================================
-- FIXED SECURE FILE EXPORT 
-- ========================================

DROP PROCEDURE IF EXISTS ExportSASSPageCleanSecure;

DELIMITER //

CREATE PROCEDURE ExportSASSPageCleanSecure(
  IN p_sample_percentage DECIMAL(5,2),
  IN p_use_secure_path TINYINT(1)
)
BEGIN
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_sample_count INT DEFAULT 0;
  DECLARE v_total_count INT DEFAULT 0;
  DECLARE v_file_path VARCHAR(1000);
  DECLARE v_secure_file_priv VARCHAR(1000);
  DECLARE v_sampling_mod INT;
  DECLARE v_error_occurred TINYINT(1) DEFAULT 0;
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET v_error_occurred = 1;
  
  -- Set defaults
  IF p_sample_percentage IS NULL THEN SET p_sample_percentage = 1.0; END IF;
  IF p_use_secure_path IS NULL THEN SET p_use_secure_path = 1; END IF;
  
  SET v_start_time = UNIX_TIMESTAMP();
  SET v_sampling_mod = CEIL(100.0 / p_sample_percentage);
  
  -- Get secure_file_priv setting
  SELECT @@secure_file_priv INTO v_secure_file_priv;
  
  -- Determine export file path
  IF p_use_secure_path = 1 AND v_secure_file_priv IS NOT NULL AND v_secure_file_priv != '' THEN
    SET v_file_path = CONCAT(v_secure_file_priv, 'sass_page_clean_subset_', REPLACE(p_sample_percentage, '.', '_'), 'pct.csv');
  ELSE
    SET v_file_path = CONCAT('/tmp/sass_page_clean_subset_', REPLACE(p_sample_percentage, '.', '_'), 'pct.csv');
  END IF;
  
  -- Get total count for sampling calculation
  SELECT COUNT(*) INTO v_total_count FROM sass_page_clean;
  
  -- Update export state
  INSERT INTO postgres_export_state (export_key, export_value, export_status) 
  VALUES ('secure_export_status', 'Starting secure file export', 'running')
  ON DUPLICATE KEY UPDATE export_value = 'Starting secure file export', export_status = 'running';
  
  -- Display pre-export information
  SELECT 
    'Secure Export Configuration' AS info_type,
    COALESCE(v_secure_file_priv, 'NULL (file exports disabled)') AS secure_file_priv,
    v_file_path AS planned_export_path,
    FORMAT(v_total_count, 0) AS total_rows_available,
    v_sampling_mod AS sampling_modulus;
  
  -- Attempt export with error handling
  SET @sql = CONCAT(
    'SELECT page_id, page_title, page_parent_id, page_root_id, page_dag_level, page_is_leaf ',
    'FROM sass_page_clean ',
    'WHERE page_id % ', v_sampling_mod, ' = 1 ',
    'INTO OUTFILE ''', v_file_path, ''' ',
    'CHARACTER SET utf8mb4 ',
    'FIELDS TERMINATED BY '','' ',
    'OPTIONALLY ENCLOSED BY ''"'' ',
    'ESCAPED BY ''\\\\'' ',
    'LINES TERMINATED BY ''\\n'''
  );
  
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  
  IF v_error_occurred = 0 THEN
    SET v_sample_count = ROW_COUNT();
    
    -- Record successful export
    INSERT INTO export_file_validation (file_name, file_path, row_count, file_size_bytes)
    VALUES (
      CONCAT('sass_page_clean_subset_', REPLACE(p_sample_percentage, '.', '_'), 'pct.csv'),
      v_file_path,
      v_sample_count,
      0
    );
    
    INSERT INTO postgres_export_state (export_key, export_value, export_status) 
    VALUES ('secure_export_status', CONCAT('Completed: ', v_sample_count, ' rows exported'), 'completed')
    ON DUPLICATE KEY UPDATE export_value = CONCAT('Completed: ', v_sample_count, ' rows exported'), export_status = 'completed';
    
    SELECT 
      'Secure Export SUCCESS' AS status,
      FORMAT(v_total_count, 0) AS total_rows_available,
      FORMAT(v_sample_count, 0) AS sample_rows_exported,
      CONCAT(ROUND(100.0 * v_sample_count / v_total_count, 2), '%') AS actual_sample_rate,
      v_file_path AS export_file_path,
      ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS export_time_sec;
  ELSE
    INSERT INTO postgres_export_state (export_key, export_value, export_status, error_message) 
    VALUES ('secure_export_status', 'Export failed', 'failed', 'File export error - check permissions and secure_file_priv')
    ON DUPLICATE KEY UPDATE export_value = 'Export failed', export_status = 'failed', error_message = 'File export error - check permissions and secure_file_priv';
    
    SELECT 
      'Secure Export FAILED' AS status,
      'File export error occurred' AS error_type,
      v_file_path AS attempted_path,
      'Check secure_file_priv setting and directory permissions' AS recommendation;
  END IF;
  
  DEALLOCATE PREPARE stmt;

END//

DELIMITER ;

-- ========================================
-- MEMORY-BASED EXPORT (NO FILE SYSTEM)
-- ========================================

DROP PROCEDURE IF EXISTS CreateExportTable;

DELIMITER //

CREATE PROCEDURE CreateExportTable(
  IN p_sample_percentage DECIMAL(5,2)
)
BEGIN
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_sample_count INT DEFAULT 0;
  DECLARE v_total_count INT DEFAULT 0;
  DECLARE v_sampling_mod INT;
  
  -- Set defaults
  IF p_sample_percentage IS NULL THEN SET p_sample_percentage = 1.0; END IF;
  
  SET v_start_time = UNIX_TIMESTAMP();
  SET v_sampling_mod = CEIL(100.0 / p_sample_percentage);
  
  -- Drop and recreate export table
  DROP TABLE IF EXISTS sass_page_clean_export;
  
  CREATE TABLE sass_page_clean_export (
    page_id INT UNSIGNED NOT NULL,
    page_title VARCHAR(255) NOT NULL,
    page_parent_id INT NOT NULL,
    page_root_id INT NOT NULL,
    page_dag_level INT NOT NULL,
    page_is_leaf TINYINT(1) NOT NULL DEFAULT 0,
    export_order INT AUTO_INCREMENT PRIMARY KEY,
    INDEX idx_page_id (page_id)
  ) ENGINE=InnoDB;
  
  -- Populate export table with sample
  INSERT INTO sass_page_clean_export (page_id, page_title, page_parent_id, page_root_id, page_dag_level, page_is_leaf)
  SELECT page_id, page_title, page_parent_id, page_root_id, page_dag_level, page_is_leaf
  FROM sass_page_clean 
  WHERE page_id % v_sampling_mod = 1
  ORDER BY page_id;
  
  SET v_sample_count = ROW_COUNT();
  SELECT COUNT(*) INTO v_total_count FROM sass_page_clean;
  
  -- Summary
  SELECT 
    'Export Table Created' AS status,
    'sass_page_clean_export' AS table_name,
    FORMAT(v_total_count, 0) AS total_source_rows,
    FORMAT(v_sample_count, 0) AS sample_rows_exported,
    CONCAT(ROUND(100.0 * v_sample_count / v_total_count, 2), '%') AS actual_sample_rate,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS creation_time_sec;
  
  -- Show sample data
  SELECT 
    'Sample Export Data' AS sample_type,
    page_id, page_title, page_parent_id, page_root_id, page_dag_level, page_is_leaf
  FROM sass_page_clean_export
  ORDER BY export_order
  LIMIT 10;
  
  -- Instructions
  SELECT 
    'Next Steps' AS instruction_type,
    'Use: SELECT * FROM sass_page_clean_export ORDER BY export_order;' AS query_command,
    'Export results manually to CSV for PostgreSQL import' AS manual_export,
    'Or use mysqldump: mysqldump --tab=/tmp/ database sass_page_clean_export' AS mysqldump_option;

END//

DELIMITER ;

-- ========================================
-- CHUNKED EXPORT WITH ENHANCED ERROR HANDLING
-- ========================================

DROP PROCEDURE IF EXISTS ExportSASSPageCleanChunkedFixed;

DELIMITER //

CREATE PROCEDURE ExportSASSPageCleanChunkedFixed(
  IN p_export_path VARCHAR(1000),
  IN p_chunk_size INT,
  IN p_use_secure_path TINYINT(1)
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
  DECLARE v_secure_file_priv VARCHAR(1000);
  DECLARE v_actual_export_path VARCHAR(1000);
  DECLARE v_error_occurred TINYINT(1) DEFAULT 0;
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET v_error_occurred = 1;
  
  -- Set defaults
  IF p_chunk_size IS NULL THEN SET p_chunk_size = 1000000; END IF;
  IF p_use_secure_path IS NULL THEN SET p_use_secure_path = 1; END IF;
  
  SET v_start_time = UNIX_TIMESTAMP();
  
  -- Get secure_file_priv setting and determine export path
  SELECT @@secure_file_priv INTO v_secure_file_priv;
  
  IF p_use_secure_path = 1 AND v_secure_file_priv IS NOT NULL AND v_secure_file_priv != '' THEN
    SET v_actual_export_path = v_secure_file_priv;
  ELSE
    SET v_actual_export_path = COALESCE(p_export_path, '/tmp/');
  END IF;
  
  -- Get total count and page ID range
  SELECT COUNT(*), MIN(page_id), MAX(page_id) 
  INTO v_total_rows, v_min_page_id, v_max_page_id 
  FROM sass_page_clean;
  
  -- Clear previous export records
  DELETE FROM export_file_validation WHERE file_name LIKE 'sass_page_clean_chunk_%';
  
  -- Update export state
  INSERT INTO postgres_export_state (export_key, export_value, export_status) 
  VALUES ('chunked_export_fixed_status', 'Starting enhanced chunked export', 'running')
  ON DUPLICATE KEY UPDATE export_value = 'Starting enhanced chunked export', export_status = 'running';
  
  -- Progress report
  SELECT 
    'Starting Enhanced Chunked Export' AS status,
    FORMAT(v_total_rows, 0) AS total_rows,
    p_chunk_size AS chunk_size,
    CEIL(v_total_rows / p_chunk_size) AS estimated_chunks,
    v_actual_export_path AS export_directory,
    COALESCE(v_secure_file_priv, 'NULL (may cause export failure)') AS secure_file_priv_setting;
  
  -- ========================================
  -- CHUNKED EXPORT LOOP WITH ERROR HANDLING
  -- ========================================
  
  SET v_chunk_start = v_min_page_id;
  
  WHILE v_continue = 1 AND v_current_chunk <= 10 DO  -- Limit to 10 chunks for safety
    SET v_chunk_end = v_chunk_start + p_chunk_size - 1;
    SET v_file_path = CONCAT(v_actual_export_path, 'sass_page_clean_chunk_', LPAD(v_current_chunk, 4, '0'), '.csv');
    SET v_error_occurred = 0;
    
    -- Attempt to export current chunk
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
    
    IF v_error_occurred = 0 THEN
      SET v_chunk_rows = ROW_COUNT();
      
      -- Record successful chunk
      INSERT INTO export_file_validation (file_name, file_path, row_count, file_size_bytes)
      VALUES (
        CONCAT('sass_page_clean_chunk_', LPAD(v_current_chunk, 4, '0'), '.csv'),
        v_file_path,
        v_chunk_rows,
        0
      );
      
      SET v_rows_processed = v_rows_processed + v_chunk_rows;
      
      -- Progress report
      SELECT 
        CONCAT('Chunk ', v_current_chunk, ' SUCCESS') AS status,
        CONCAT(v_chunk_start, ' - ', v_chunk_end) AS page_id_range,
        FORMAT(v_chunk_rows, 0) AS chunk_rows,
        FORMAT(v_rows_processed, 0) AS total_processed,
        CONCAT(ROUND(100.0 * v_rows_processed / v_total_rows, 1), '%') AS progress,
        v_file_path AS chunk_file_path,
        ROUND(UNIX_TIMESTAMP() - v_start_time, 1) AS elapsed_sec;
    ELSE
      -- Record failed chunk
      INSERT INTO postgres_export_state (export_key, export_value, export_status, error_message) 
      VALUES (CONCAT('chunk_', v_current_chunk, '_status'), 'Failed', 'failed', CONCAT('Export failed for chunk ', v_current_chunk))
      ON DUPLICATE KEY UPDATE export_value = 'Failed', export_status = 'failed';
      
      SELECT 
        CONCAT('Chunk ', v_current_chunk, ' FAILED') AS status,
        v_file_path AS attempted_path,
        'File system export error' AS error_type,
        'Consider using CreateExportTable() method instead' AS recommendation;
    END IF;
    
    DEALLOCATE PREPARE stmt;
    
    -- Check if done or prepare for next chunk
    IF v_chunk_rows = 0 OR v_rows_processed >= v_total_rows OR v_error_occurred = 1 THEN
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
  VALUES ('chunked_export_fixed_status', CONCAT('Completed: ', v_current_chunk - 1, ' chunks, ', v_rows_processed, ' rows'), 'completed')
  ON DUPLICATE KEY UPDATE export_value = CONCAT('Completed: ', v_current_chunk - 1, ' chunks, ', v_rows_processed, ' rows'), export_status = 'completed';
  
  -- Final summary
  SELECT 
    'Enhanced Chunked Export Complete' AS final_status,
    FORMAT(v_total_rows, 0) AS total_rows_in_table,
    FORMAT(v_rows_processed, 0) AS total_rows_exported,
    v_current_chunk - 1 AS chunks_created,
    CASE WHEN v_current_chunk > 1 THEN ROUND(v_rows_processed / (v_current_chunk - 1), 0) ELSE 0 END AS avg_rows_per_chunk,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_export_time_sec;

END//

DELIMITER ;

-- ========================================
-- LEGACY COMPATIBILITY PROCEDURES
-- ========================================

-- Original subset export with fixed parameters
DROP PROCEDURE IF EXISTS ExportSASSPageCleanSubset;

DELIMITER //

CREATE PROCEDURE ExportSASSPageCleanSubset(
  IN p_export_path VARCHAR(1000),
  IN p_sample_percentage DECIMAL(5,2)
)
BEGIN
  -- Try secure file export first, fall back to table creation
  CALL ExportSASSPageCleanSecure(p_sample_percentage, 1);
  
  -- Show alternative method recommendation
  SELECT 
    'Alternative Export Method' AS method_type,
    'CALL CreateExportTable(1.0);' AS table_method,
    'CALL ExportSASSPageCleanToString(1.0, 100);' AS string_method,
    'Use if file export fails due to secure_file_priv restrictions' AS usage_note;
END//

DELIMITER ;

-- Original chunked export procedure
DROP PROCEDURE IF EXISTS ExportSASSPageCleanChunked;

DELIMITER //

CREATE PROCEDURE ExportSASSPageCleanChunked(
  IN p_export_path VARCHAR(1000),
  IN p_chunk_size INT
)
BEGIN
  -- Call the enhanced chunked export
  CALL ExportSASSPageCleanChunkedFixed(p_export_path, p_chunk_size, 1);
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
-- POSTGRESQL SCRIPT GENERATION
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
-- USAGE EXAMPLES AND DOCUMENTATION
-- ========================================

/*
-- PRODUCTION EXPORT WORKFLOW

-- Step 1: Diagnose environment
CALL DiagnoseExportEnvironment();

-- Step 2: Test with 1% sample export
CALL ExportSASSPageCleanSecure(1.0, 1);

-- Step 3: Full chunked export (recommended for production)
CALL ExportSASSPageCleanChunkedFixed('/private/tmp/mysql_export/', 1000000, 1);

-- Step 4: Validate export integrity
CALL ValidateExportIntegrity();

-- Step 5: Generate PostgreSQL import scripts
CALL GeneratePostgreSQLTableScript();
CALL GeneratePostgreSQLImportScript('/path/to/export/files/');

-- ALTERNATIVE METHODS (if file exports fail)

-- String-based export (manual copy-paste)
CALL ExportSASSPageCleanToString(1.0, 100);

-- Memory-based export table
CALL CreateExportTable(1.0);
SELECT * FROM sass_page_clean_export ORDER BY export_order;

-- Check export status
SELECT * FROM postgres_export_state ORDER BY updated_at DESC;

-- POSTGRESQL IMPORT PROCESS

-- 1. Create table (without indices for faster import)
DROP TABLE IF EXISTS sass_page_clean;
CREATE TABLE sass_page_clean (
  page_id BIGINT NOT NULL,
  page_title VARCHAR(255) NOT NULL,
  page_parent_id INTEGER NOT NULL,
  page_root_id INTEGER NOT NULL,
  page_dag_level INTEGER NOT NULL,
  page_is_leaf BOOLEAN NOT NULL DEFAULT FALSE
);

-- 2. Import data (repeat for each chunk file)
\COPY sass_page_clean FROM '/path/to/sass_page_clean_chunk_0001.csv' WITH (FORMAT CSV, ENCODING 'UTF8');
\COPY sass_page_clean FROM '/path/to/sass_page_clean_chunk_0002.csv' WITH (FORMAT CSV, ENCODING 'UTF8');

-- 3. Create indices after import
CREATE UNIQUE INDEX CONCURRENTLY idx_sass_page_clean_pkey ON sass_page_clean (page_id);
ALTER TABLE sass_page_clean ADD CONSTRAINT sass_page_clean_pkey PRIMARY KEY USING INDEX idx_sass_page_clean_pkey;
CREATE INDEX CONCURRENTLY idx_sass_page_clean_title ON sass_page_clean (page_title);
CREATE INDEX CONCURRENTLY idx_sass_page_clean_parent ON sass_page_clean (page_parent_id);
CREATE INDEX CONCURRENTLY idx_sass_page_clean_root ON sass_page_clean (page_root_id);
CREATE INDEX CONCURRENTLY idx_sass_page_clean_level ON sass_page_clean (page_dag_level);
CREATE INDEX CONCURRENTLY idx_sass_page_clean_leaf ON sass_page_clean (page_is_leaf);

-- 4. Validate import
SELECT COUNT(*) as total_rows FROM sass_page_clean;
SELECT page_dag_level, COUNT(*) as level_count FROM sass_page_clean GROUP BY page_dag_level ORDER BY page_dag_level;
SELECT page_is_leaf, COUNT(*) as leaf_count FROM sass_page_clean GROUP BY page_is_leaf;

PERFORMANCE NOTES:
- Full export: ~9.4M rows in 10-15 chunks of 1M rows each
- Expected export time: 5-10 minutes depending on hardware
- PostgreSQL import: Use \COPY for best performance
- Create indices AFTER data import for speed
*/
