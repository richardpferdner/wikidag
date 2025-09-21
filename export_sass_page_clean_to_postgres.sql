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
  DECLARE v_secure_file_priv VARCHAR(1000);
  DECLARE v_version VARCHAR(100);
  
  -- Get MySQL version and secure_file_priv setting
  SELECT @@version INTO v_version;
  SELECT @@secure_file_priv INTO v_secure_file_priv;
  
  -- Display system configuration
  SELECT 'MySQL Export Environment Diagnostics' AS diagnostic_type,
         v_version AS mysql_version,
         COALESCE(v_secure_file_priv, 'NULL (exports disabled)') AS secure_file_priv_setting,
         CURRENT_USER() AS current_user,
         SCHEMA() AS current_database;
  
  -- Test basic export capability
  SELECT 'Export Capability Test' AS test_type,
         CASE 
           WHEN v_secure_file_priv IS NULL THEN 'BLOCKED: secure_file_priv is NULL'
           WHEN v_secure_file_priv = '' THEN 'ALLOWED: Any directory'
           ELSE CONCAT('RESTRICTED: Only ', v_secure_file_priv)
         END AS export_status,
         'Use SHOW VARIABLES LIKE "secure_file_priv" for details' AS recommendation;
  
  -- Sample data availability check
  SELECT 'Data Availability Check' AS check_type,
         FORMAT(COUNT(*), 0) AS total_rows_sass_page_clean,
         FORMAT(COUNT(CASE WHEN page_id % 100 = 1 THEN 1 END), 0) AS sample_rows_1pct,
         FORMAT(MIN(page_id), 0) AS min_page_id,
         FORMAT(MAX(page_id), 0) AS max_page_id
  FROM sass_page_clean;

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
-- USAGE EXAMPLES AND TROUBLESHOOTING
-- ========================================

/*
-- TROUBLESHOOTING WORKFLOW

-- Step 1: Diagnose environment
CALL DiagnoseExportEnvironment();

-- Step 2: Try string-based export (always works)
CALL ExportSASSPageCleanToString(1.0, 50);

-- Step 3: Try secure file export
CALL ExportSASSPageCleanSecure(1.0, 1);

-- Step 4: Create memory-based export table
CALL CreateExportTable(1.0);

-- Step 5: Manual export from export table
SELECT * FROM sass_page_clean_export ORDER BY export_order;

-- Step 6: Check export status
SELECT * FROM postgres_export_state ORDER BY updated_at DESC;

-- Alternative: Use mysqldump (if file exports fail)
-- mysqldump --tab=/tmp/ --fields-terminated-by=, --fields-optionally-enclosed-by=\" database sass_page_clean_export

-- PostgreSQL import from string output:
-- 1. Copy CSV data from ExportSASSPageCleanToString() output
-- 2. Save to file: sass_page_clean_sample.csv
-- 3. Import: \COPY sass_page_clean FROM 'sass_page_clean_sample.csv' WITH (FORMAT CSV, HEADER);

COMMON SOLUTIONS:
1. secure_file_priv = NULL: File exports disabled in MySQL config
2. Permission denied: Directory not writable by MySQL user
3. Path not found: Directory doesn't exist
4. Use CreateExportTable() + manual CSV export as fallback
*/
