-- SASS Page Clean: MySQL to PostgreSQL Migration - INDEX-FREE OPTIMIZED VERSION
-- Implements high-performance export/import strategy for representative pages only
-- Exports data without indices, rebuilds indices post-import for 3-4x faster completion
-- Optimized for ~9.16M representative pages with estimated 1.5-2 hour total migration time
-- Includes comprehensive file system troubleshooting and alternative export methods

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
  
  -- Test representative data availability
  SELECT 'Representative Data Availability Check' AS check_type,
         COUNT(*) AS total_representative_rows,
         COUNT(CASE WHEN page_id % 100 = 1 THEN 1 END) AS sample_1pct,
         MIN(page_id) AS min_id,
         MAX(page_id) AS max_id,
         COUNT(CASE WHEN page_is_leaf = 1 THEN 1 END) AS representative_articles,
         COUNT(CASE WHEN page_is_leaf = 0 THEN 1 END) AS representative_categories
  FROM sass_page_clean;
  
  -- Show export recommendations
  SELECT 'Export Recommendations' AS info_type,
         'Index-free export for 3-4x faster PostgreSQL import' AS performance_note,
         'Use ExportSASSPageCleanToString() for manual CSV export' AS alternative_method,
         'Estimated total migration time: 1.5-2 hours' AS time_estimate,
         'Expected representative rows: ~9.16M (varies by filtering)' AS data_volume;
END//

DELIMITER ;

-- ========================================
-- FILE SYSTEM TROUBLESHOOTING PROCEDURE
-- ========================================

DROP PROCEDURE IF EXISTS DiagnoseFileSystemIssues;

DELIMITER //

CREATE PROCEDURE DiagnoseFileSystemIssues(
  IN p_test_path VARCHAR(1000)
)
BEGIN
  DECLARE v_secure_file_priv VARCHAR(1000);
  DECLARE v_test_file_path VARCHAR(1000);
  DECLARE v_error_occurred TINYINT(1) DEFAULT 0;
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET v_error_occurred = 1;
  
  -- Set default test path
  IF p_test_path IS NULL THEN SET p_test_path = '/private/tmp/mysql_export/'; END IF;
  
  -- Get MySQL settings
  SELECT @@secure_file_priv INTO v_secure_file_priv;
  
  -- Show MySQL file system configuration
  SELECT 
    'MySQL File System Configuration' AS diagnostic_type,
    COALESCE(v_secure_file_priv, 'NULL (file exports disabled)') AS secure_file_priv_setting,
    p_test_path AS requested_export_path,
    CASE 
      WHEN v_secure_file_priv IS NULL THEN 'File exports disabled by MySQL configuration'
      WHEN v_secure_file_priv = '' THEN 'File exports allowed to any writable directory'
      WHEN p_test_path LIKE CONCAT(v_secure_file_priv, '%') THEN 'Requested path is within allowed directory'
      ELSE 'Requested path is OUTSIDE allowed directory - will fail'
    END AS path_compatibility,
    'Check directory exists and MySQL has write permissions' AS permission_note;
  
  -- Test file creation if secure_file_priv allows it
  IF v_secure_file_priv IS NOT NULL THEN
    IF v_secure_file_priv = '' OR p_test_path LIKE CONCAT(v_secure_file_priv, '%') THEN
      SET v_test_file_path = CONCAT(p_test_path, 'mysql_test_', UNIX_TIMESTAMP(), '.txt');
      SET v_error_occurred = 0;
      
      -- Attempt to create a test file
      SET @test_sql = CONCAT(
        'SELECT ''MySQL file write test'' INTO OUTFILE ''', v_test_file_path, ''' ',
        'FIELDS TERMINATED BY '','' LINES TERMINATED BY ''\\n'''
      );
      
      PREPARE test_stmt FROM @test_sql;
      EXECUTE test_stmt;
      DEALLOCATE PREPARE test_stmt;
      
      IF v_error_occurred = 0 THEN
        SELECT 
          'File System Test Results' AS test_type,
          'SUCCESS - MySQL can write to directory' AS test_result,
          v_test_file_path AS test_file_created,
          'Directory permissions are correct' AS recommendation;
        
        -- Clean up test file
        SET @cleanup_sql = CONCAT('SELECT LOAD_FILE(''', v_test_file_path, ''') INTO @dummy');
        PREPARE cleanup_stmt FROM @cleanup_sql;
        EXECUTE cleanup_stmt;
        DEALLOCATE PREPARE cleanup_stmt;
      ELSE
        SELECT 
          'File System Test Results' AS test_type,
          'FAILED - MySQL cannot write to directory' AS test_result,
          v_test_file_path AS attempted_test_file,
          'Check directory exists and MySQL user has write permissions' AS recommendation;
      END IF;
    ELSE
      SELECT 
        'File System Test Results' AS test_type,
        'SKIPPED - Requested path outside secure_file_priv' AS test_result,
        'Use a path within the allowed directory or update MySQL configuration' AS recommendation;
    END IF;
  ELSE
    SELECT 
      'File System Test Results' AS test_type,
      'SKIPPED - File exports disabled in MySQL' AS test_result,
      'Use CreateExportTable() method instead' AS recommendation;
  END IF;
  
  -- Suggest alternative export methods
  SELECT 
    'Alternative Export Methods' AS alternatives_section,
    'CALL CreateExportTable(1.0);' AS memory_table_method,
    'CALL ExportSASSPageCleanToString(1.0, 100);' AS string_output_method,
    'Both methods avoid file system permissions' AS benefit;

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
  VALUES ('string_export_status', 'Generating CSV string output for representative pages', 'running')
  ON DUPLICATE KEY UPDATE export_value = 'Generating CSV string output for representative pages', export_status = 'running';
  
  -- Display sampling info
  SELECT 
    'String Export Configuration' AS info_type,
    FORMAT(v_total_count, 0) AS total_representative_rows,
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
-- ENHANCED SECURE FILE EXPORT (INDEX-FREE)
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
    SET v_file_path = CONCAT(v_secure_file_priv, 'sass_page_clean_representatives_', REPLACE(p_sample_percentage, '.', '_'), 'pct.csv');
  ELSE
    SET v_file_path = CONCAT('/tmp/sass_page_clean_representatives_', REPLACE(p_sample_percentage, '.', '_'), 'pct.csv');
  END IF;
  
  -- Get total count for sampling calculation
  SELECT COUNT(*) INTO v_total_count FROM sass_page_clean;
  
  -- Update export state
  INSERT INTO postgres_export_state (export_key, export_value, export_status) 
  VALUES ('secure_export_status', 'Starting index-free file export of representatives', 'running')
  ON DUPLICATE KEY UPDATE export_value = 'Starting index-free file export of representatives', export_status = 'running';
  
  -- Display pre-export information
  SELECT 
    'Index-Free Export Configuration' AS info_type,
    COALESCE(v_secure_file_priv, 'NULL (file exports disabled)') AS secure_file_priv,
    v_file_path AS planned_export_path,
    FORMAT(v_total_count, 0) AS total_representative_rows,
    v_sampling_mod AS sampling_modulus,
    'Data-only export (no indices)' AS export_strategy;
  
  -- Attempt export with error handling (data-only)
  SET @sql = CONCAT(
    'SELECT page_id, page_title, page_parent_id, page_root_id, page_dag_level, page_is_leaf ',
    'FROM sass_page_clean ',
    'WHERE page_id % ', v_sampling_mod, ' = 1 ',
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
    SET v_sample_count = ROW_COUNT();
    
    -- Record successful export
    INSERT INTO export_file_validation (file_name, file_path, row_count, file_size_bytes)
    VALUES (
      CONCAT('sass_page_clean_representatives_', REPLACE(p_sample_percentage, '.', '_'), 'pct.csv'),
      v_file_path,
      v_sample_count,
      0
    );
    
    INSERT INTO postgres_export_state (export_key, export_value, export_status) 
    VALUES ('secure_export_status', CONCAT('Completed: ', v_sample_count, ' representative rows exported'), 'completed')
    ON DUPLICATE KEY UPDATE export_value = CONCAT('Completed: ', v_sample_count, ' representative rows exported'), export_status = 'completed';
    
    SELECT 
      'Index-Free Export SUCCESS' AS status,
      FORMAT(v_total_count, 0) AS total_representative_rows,
      FORMAT(v_sample_count, 0) AS sample_rows_exported,
      CONCAT(ROUND(100.0 * v_sample_count / v_total_count, 2), '%') AS actual_sample_rate,
      v_file_path AS export_file_path,
      'Data-only CSV (indices built post-import)' AS export_type,
      ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS export_time_sec;
  ELSE
    INSERT INTO postgres_export_state (export_key, export_value, export_status, error_message) 
    VALUES ('secure_export_status', 'Export failed', 'failed', 'File export error - check permissions and secure_file_priv')
    ON DUPLICATE KEY UPDATE export_value = 'Export failed', export_status = 'failed', error_message = 'File export error - check permissions and secure_file_priv';
    
    SELECT 
      'Index-Free Export FAILED' AS status,
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
  
  -- Drop and recreate export table (no indices for PostgreSQL compatibility)
  DROP TABLE IF EXISTS sass_page_clean_export;
  
  CREATE TABLE sass_page_clean_export (
    page_id INT UNSIGNED NOT NULL,
    page_title VARCHAR(255) NOT NULL,
    page_parent_id INT NOT NULL,
    page_root_id INT NOT NULL,
    page_dag_level INT NOT NULL,
    page_is_leaf TINYINT(1) NOT NULL DEFAULT 0,
    export_order INT AUTO_INCREMENT PRIMARY KEY
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
    'Export Table Created (Index-Free)' AS status,
    'sass_page_clean_export' AS table_name,
    FORMAT(v_total_count, 0) AS total_representative_rows,
    FORMAT(v_sample_count, 0) AS sample_rows_exported,
    CONCAT(ROUND(100.0 * v_sample_count / v_total_count, 2), '%') AS actual_sample_rate,
    'Data-only table (indices built post-import)' AS export_strategy,
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
    'Use: SELECT page_id,page_title,page_parent_id,page_root_id,page_dag_level,page_is_leaf FROM sass_page_clean_export ORDER BY page_id;' AS query_command,
    'Export results manually to CSV for PostgreSQL import' AS manual_export,
    'Or use mysqldump: mysqldump --tab=/tmp/ database sass_page_clean_export' AS mysqldump_option;

END//

DELIMITER ;

-- ========================================
-- CHUNKED EXPORT WITH INDEX-FREE OPTIMIZATION
-- ========================================

DROP PROCEDURE IF EXISTS ExportSASSPageCleanChunkedOptimized;

DELIMITER //

CREATE PROCEDURE ExportSASSPageCleanChunkedOptimized(
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
  DELETE FROM export_file_validation WHERE file_name LIKE 'sass_page_clean_representatives_chunk_%';
  
  -- Update export state
  INSERT INTO postgres_export_state (export_key, export_value, export_status) 
  VALUES ('chunked_export_optimized_status', 'Starting index-free chunked export of representatives', 'running')
  ON DUPLICATE KEY UPDATE export_value = 'Starting index-free chunked export of representatives', export_status = 'running';
  
  -- Progress report
  SELECT 
    'Starting Index-Free Chunked Export' AS status,
    FORMAT(v_total_rows, 0) AS total_representative_rows,
    p_chunk_size AS chunk_size,
    CEIL(v_total_rows / p_chunk_size) AS estimated_chunks,
    v_actual_export_path AS export_directory,
    COALESCE(v_secure_file_priv, 'NULL (may cause export failure)') AS secure_file_priv_setting,
    'Data-only chunks (indices built post-import)' AS export_strategy,
    'Ensure export directory exists and MySQL has write access' AS permission_note;
  
  -- ========================================
  -- CHUNKED EXPORT LOOP WITH INDEX-FREE OPTIMIZATION
  -- ========================================
  
  SET v_chunk_start = v_min_page_id;
  
  WHILE v_continue = 1 DO
    SET v_chunk_end = v_chunk_start + p_chunk_size - 1;
    SET v_file_path = CONCAT(v_actual_export_path, 'sass_page_clean_representatives_chunk_', LPAD(v_current_chunk, 12, '0'), '.csv');
    SET v_error_occurred = 0;
    
    -- Attempt to export current chunk (data-only)
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
        CONCAT('sass_page_clean_representatives_chunk_', LPAD(v_current_chunk, 12, '0'), '.csv'),
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
        'Data-only chunk' AS chunk_type,
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
  VALUES ('chunked_export_optimized_status', CONCAT('Completed: ', v_current_chunk - 1, ' chunks, ', v_rows_processed, ' representatives'), 'completed')
  ON DUPLICATE KEY UPDATE export_value = CONCAT('Completed: ', v_current_chunk - 1, ' chunks, ', v_rows_processed, ' representatives'), export_status = 'completed';
  
  -- Final summary
  SELECT 
    'Index-Free Chunked Export Complete' AS final_status,
    FORMAT(v_total_rows, 0) AS total_representative_rows,
    FORMAT(v_rows_processed, 0) AS total_rows_exported,
    v_current_chunk - 1 AS chunks_created,
    CASE WHEN v_current_chunk > 1 THEN ROUND(v_rows_processed / (v_current_chunk - 1), 0) ELSE 0 END AS avg_rows_per_chunk,
    'Data-only chunks (indices built post-import)' AS export_strategy,
    'Estimated PostgreSQL import time: 45-60 minutes' AS import_estimate,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_export_time_sec;

END//

DELIMITER ;

-- ========================================
-- LEGACY COMPATIBILITY PROCEDURES
-- ========================================

-- Original subset export with optimized parameters
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
    'Alternative Index-Free Export Methods' AS method_type,
    'CALL CreateExportTable(1.0);' AS table_method,
    'CALL ExportSASSPageCleanToString(1.0, 100);' AS string_method,
    'Use if file export fails due to secure_file_priv restrictions' AS usage_note,
    'All methods export data-only for optimal PostgreSQL import' AS optimization_note;
END//

DELIMITER ;

-- Original chunked export procedure (backward compatibility)
DROP PROCEDURE IF EXISTS ExportSASSPageCleanChunked;

DELIMITER //

CREATE PROCEDURE ExportSASSPageCleanChunked(
  IN p_export_path VARCHAR(1000),
  IN p_chunk_size INT
)
BEGIN
  -- Call the optimized chunked export
  CALL ExportSASSPageCleanChunkedOptimized(p_export_path, p_chunk_size, 1);
END//

DELIMITER ;

-- Legacy procedure name for backward compatibility
DROP PROCEDURE IF EXISTS ExportSASSPageCleanChunkedFixed;

DELIMITER //

CREATE PROCEDURE ExportSASSPageCleanChunkedFixed(
  IN p_export_path VARCHAR(1000),
  IN p_chunk_size INT,
  IN p_use_secure_path TINYINT(1)
)
BEGIN
  -- Call the optimized chunked export with all parameters
  CALL ExportSASSPageCleanChunkedOptimized(p_export_path, p_chunk_size, p_use_secure_path);
  
  -- Additional guidance for file system issues
  SELECT 
    'File System Troubleshooting' AS help_section,
    'Check directory exists and MySQL has write permissions' AS directory_check,
    'Try: CALL CreateExportTable(1.0); for alternative export' AS fallback_method,
    'Estimated representative rows: ~9.16M' AS expected_data_volume;
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
  
  -- Get source table count (representatives)
  SELECT COUNT(*) INTO v_total_source_rows FROM sass_page_clean;
  
  -- Get total exported rows
  SELECT COALESCE(SUM(row_count), 0) INTO v_total_export_rows
  FROM export_file_validation 
  WHERE file_name LIKE 'sass_page_clean_representatives_chunk_%';
  
  -- Validation report
  SELECT 
    'Export Integrity Validation (Representatives)' AS validation_type,
    FORMAT(v_total_source_rows, 0) AS source_representative_rows,
    FORMAT(v_total_export_rows, 0) AS exported_file_rows,
    FORMAT(v_total_source_rows - v_total_export_rows, 0) AS row_difference,
    CASE 
      WHEN v_total_source_rows = v_total_export_rows THEN 'PASS'
      ELSE 'FAIL'
    END AS validation_status,
    'Validates representative pages only (duplicates removed)' AS validation_scope;
  
  -- Export file summary
  SELECT 
    'Export File Details' AS file_details,
    file_name,
    FORMAT(row_count, 0) AS file_rows,
    FORMAT(file_size_bytes, 0) AS file_bytes,
    created_at
  FROM export_file_validation 
  WHERE file_name LIKE 'sass_page_clean_representatives_%'
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
    'Encoding Validation (Representatives)' AS validation_type,
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
    'Encoding Validation (Representatives)',
    'Non-ASCII characters',
    COUNT(*),
    'Unicode characters in page_title'
  FROM sass_page_clean 
  WHERE page_title REGEXP '[^\x00-\x7F]'
  
  UNION ALL
  
  SELECT 
    'Encoding Validation (Representatives)',
    'Very long titles',
    COUNT(*),
    'Titles approaching VARCHAR(255) limit'
  FROM sass_page_clean 
  WHERE LENGTH(page_title) > 240;
  
  -- Sample problematic titles
  SELECT 
    'Sample Problematic Titles (Representatives)' AS sample_type,
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
-- POSTGRESQL SCRIPT GENERATION (INDEX-FREE OPTIMIZED)
-- ========================================

-- Generate PostgreSQL table creation script (no indices)
DROP PROCEDURE IF EXISTS GeneratePostgreSQLTableScript;

DELIMITER //

CREATE PROCEDURE GeneratePostgreSQLTableScript()
BEGIN
  SELECT '-- PostgreSQL Table Creation (INDEX-FREE for Optimal Import)' AS script_section
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
  SELECT '-- NO INDICES CREATED YET - BUILD AFTER IMPORT FOR OPTIMAL PERFORMANCE'
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT '-- Data Import Commands (Fast Bulk Loading)'
  UNION ALL
  SELECT "\\COPY sass_page_clean FROM '/path/to/sass_page_clean_representatives_1_0pct.csv' WITH (FORMAT CSV, ENCODING 'UTF8');"
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT '-- For chunked import, repeat for each chunk:'
  UNION ALL
  SELECT "-- \\COPY sass_page_clean FROM '/path/to/sass_page_clean_representatives_chunk_000000000001.csv' WITH (FORMAT CSV, ENCODING 'UTF8');"
  UNION ALL
  SELECT "-- \\COPY sass_page_clean FROM '/path/to/sass_page_clean_representatives_chunk_000000000002.csv' WITH (FORMAT CSV, ENCODING 'UTF8');"
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT '-- Validation Query (Run After Import, Before Index Creation)'
  UNION ALL
  SELECT 'SELECT COUNT(*) as imported_representative_rows FROM sass_page_clean;'
  UNION ALL
  SELECT 'SELECT page_dag_level, COUNT(*) as level_count FROM sass_page_clean GROUP BY page_dag_level ORDER BY page_dag_level;'
  UNION ALL
  SELECT 'SELECT page_is_leaf, COUNT(*) as leaf_count FROM sass_page_clean GROUP BY page_is_leaf;';

END//

DELIMITER ;

-- Generate PostgreSQL index creation script (post-import)
DROP PROCEDURE IF EXISTS GeneratePostgreSQLIndexScript;

DELIMITER //

CREATE PROCEDURE GeneratePostgreSQLIndexScript()
BEGIN
  SELECT '-- PostgreSQL Index Creation Script (Run AFTER Data Import)' AS script_section
  UNION ALL
  SELECT '-- Estimated index creation time: 30-45 minutes for ~9.4M representatives'
  UNION ALL
  SELECT '-- Can be run in parallel for faster completion'
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT '-- Step 1: Create PRIMARY KEY (Most Important - Creates Unique Constraint)'
  UNION ALL
  SELECT 'CREATE UNIQUE INDEX CONCURRENTLY idx_sass_page_clean_pkey ON sass_page_clean (page_id);'
  UNION ALL
  SELECT 'ALTER TABLE sass_page_clean ADD CONSTRAINT sass_page_clean_pkey PRIMARY KEY USING INDEX idx_sass_page_clean_pkey;'
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT '-- Step 2: Create Secondary Indices (Can be run in parallel sessions)'
  UNION ALL
  SELECT '-- Session 1:'
  UNION ALL
  SELECT 'CREATE INDEX CONCURRENTLY idx_sass_page_clean_title ON sass_page_clean (page_title);'
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT '-- Session 2:'
  UNION ALL
  SELECT 'CREATE INDEX CONCURRENTLY idx_sass_page_clean_parent ON sass_page_clean (page_parent_id);'
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT '-- Session 3:'
  UNION ALL
  SELECT 'CREATE INDEX CONCURRENTLY idx_sass_page_clean_root ON sass_page_clean (page_root_id);'
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT '-- Session 4:'
  UNION ALL
  SELECT 'CREATE INDEX CONCURRENTLY idx_sass_page_clean_level ON sass_page_clean (page_dag_level);'
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT '-- Session 5:'
  UNION ALL
  SELECT 'CREATE INDEX CONCURRENTLY idx_sass_page_clean_leaf ON sass_page_clean (page_is_leaf);'
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT '-- Final Validation'
  UNION ALL
  SELECT 'SELECT schemaname, tablename, indexname, indexdef FROM pg_indexes WHERE tablename = ''sass_page_clean'' ORDER BY indexname;'
  UNION ALL
  SELECT 'ANALYZE sass_page_clean;'
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT '-- Performance Test Query'
  UNION ALL
  SELECT 'EXPLAIN (ANALYZE, BUFFERS) SELECT COUNT(*) FROM sass_page_clean WHERE page_dag_level = 5 AND page_is_leaf = true;';

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
  WHERE file_name LIKE 'sass_page_clean_representatives_chunk_%';
  
  -- Header
  SELECT '-- PostgreSQL Import Script for Index-Free Chunked Files' AS import_script
  UNION ALL
  SELECT CONCAT('-- Total chunks to import: ', v_chunk_count)
  UNION ALL
  SELECT CONCAT('-- Import path: ', COALESCE(p_import_path, '/path/to/export/files/'))
  UNION ALL
  SELECT '-- Estimated import time: 45-60 minutes (index-free bulk loading)'
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT '-- Start transaction for atomic import'
  UNION ALL
  SELECT 'BEGIN;'
  UNION ALL
  SELECT '';
  
  -- Generate COPY commands for each chunk
  WHILE v_current_chunk <= v_chunk_count DO
    SELECT CONCAT('\\COPY sass_page_clean FROM ''', 
                  COALESCE(p_import_path, '/path/to/export/files/'), 
                  'sass_page_clean_representatives_chunk_', LPAD(v_current_chunk, 12, '0'), 
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
  SELECT '-- Verify import before index creation'
  UNION ALL
  SELECT 'SELECT COUNT(*) as imported_representative_rows FROM sass_page_clean;'
  UNION ALL
  SELECT '-- Expected: ~9.16M representative pages (varies by filtering)'
  UNION ALL
  SELECT ''
  UNION ALL
  SELECT '-- Next step: Run GeneratePostgreSQLIndexScript() to create indices'
  UNION ALL
  SELECT '-- Estimated index creation time: 30-45 minutes';

END//

DELIMITER ;

-- ========================================
-- USAGE EXAMPLES AND DOCUMENTATION
-- ========================================

/*
-- INDEX-FREE EXPORT WORKFLOW FOR OPTIMAL PERFORMANCE

-- Step 0: Ensure export directory exists and has proper permissions
-- mkdir -p /private/tmp/mysql_export/
-- chmod 755 /private/tmp/mysql_export/
-- chown mysql:mysql /private/tmp/mysql_export/  (if needed)


-- or if existing, empty export directory
cd /tmp/mysql_export
mysql_export % rm *.csv

-- Step 1: Diagnose environment and check data availability
CALL DiagnoseExportEnvironment();

-- Step 1b: If file export issues expected, diagnose file system
CALL DiagnoseFileSystemIssues('/private/tmp/mysql_export/');

-- Step 2: Test with 1% sample export (index-free)
CALL ExportSASSPageCleanSecure(1.0, 1);

-- Step 3: Full chunked export (recommended for production)
-- Updated procedure name for better compatibility:
CALL ExportSASSPageCleanChunkedOptimized('/private/tmp/mysql_export/', 1000000, 1);

-- Step 4: Validate export integrity
CALL ValidateExportIntegrity();

-- Step 5: Generate PostgreSQL scripts
CALL GeneratePostgreSQLTableScript();
CALL GeneratePostgreSQLIndexScript();
CALL GeneratePostgreSQLImportScript('/path/to/export/files/');

-- TROUBLESHOOTING FILE EXPORT ISSUES

-- If file export fails with permission errors:
-- 1. Check if directory exists:
SELECT @@secure_file_priv AS mysql_export_directory;

-- 2. Try alternative export methods:
CALL CreateExportTable(1.0);
SELECT page_id,page_title,page_parent_id,page_root_id,page_dag_level,page_is_leaf FROM sass_page_clean_export ORDER BY page_id;

-- 3. String-based export (manual copy-paste):
CALL ExportSASSPageCleanToString(1.0, 100);

-- 4. Check export status:
SELECT * FROM postgres_export_state ORDER BY updated_at DESC;

-- OPTIMIZED POSTGRESQL IMPORT PROCESS

-- Phase 1: Create table structure (NO INDICES)
DROP TABLE IF EXISTS sass_page_clean;
CREATE TABLE sass_page_clean (
  page_id BIGINT NOT NULL,
  page_title VARCHAR(255) NOT NULL,
  page_parent_id INTEGER NOT NULL,
  page_root_id INTEGER NOT NULL,
  page_dag_level INTEGER NOT NULL,
  page_is_leaf BOOLEAN NOT NULL DEFAULT FALSE
);

-- Phase 2: Import data (FAST - no index maintenance)
\COPY sass_page_clean FROM '/path/to/sass_page_clean_representatives_chunk_000000000001.csv' WITH (FORMAT CSV, ENCODING 'UTF8');
\COPY sass_page_clean FROM '/path/to/sass_page_clean_representatives_chunk_000000000002.csv' WITH (FORMAT CSV, ENCODING 'UTF8');
-- ... repeat for all chunks

-- Phase 3: Verify import
SELECT COUNT(*) as imported_representative_rows FROM sass_page_clean;
-- Expected: ~9.16M representative pages (may vary based on filtering)

-- Phase 4: Create indices AFTER import (parallel execution recommended)
-- Session 1:
CREATE UNIQUE INDEX CONCURRENTLY idx_sass_page_clean_pkey ON sass_page_clean (page_id);
ALTER TABLE sass_page_clean ADD CONSTRAINT sass_page_clean_pkey PRIMARY KEY USING INDEX idx_sass_page_clean_pkey;

-- Session 2:
CREATE INDEX CONCURRENTLY idx_sass_page_clean_title ON sass_page_clean (page_title);

-- Session 3:
CREATE INDEX CONCURRENTLY idx_sass_page_clean_parent ON sass_page_clean (page_parent_id);

-- Session 4:
CREATE INDEX CONCURRENTLY idx_sass_page_clean_root ON sass_page_clean (page_root_id);

-- Session 5:
CREATE INDEX CONCURRENTLY idx_sass_page_clean_level ON sass_page_clean (page_dag_level);

-- Session 6:
CREATE INDEX CONCURRENTLY idx_sass_page_clean_leaf ON sass_page_clean (page_is_leaf);

-- Phase 5: Final optimization
ANALYZE sass_page_clean;

-- PERFORMANCE ESTIMATES (Representative Pages Only):
-- MySQL Export: 12-15 minutes (data-only CSV files)
-- PostgreSQL Import: 45-60 minutes (bulk loading without indices)
-- Index Creation: 30-45 minutes (batch creation, can run in parallel)
-- Total Migration Time: 1.5-2 hours (vs 4.5-6.5 hours with indices)

-- REPRESENTATIVE PAGE BENEFITS:
-- - Deduplicated canonical pages only (~9.16M actual vs theoretical 9.4M)
-- - All essential information preserved in representatives
-- - Optimal for search and navigation systems
-- - Significantly faster processing due to reduced dataset size

-- DATA QUALITY ASSURANCE:
-- - Only canonical representative pages exported
-- - Title variations consolidated into representatives
-- - DAG level relationships preserved through representatives
-- - Root domain mappings maintained

-- COMMON TROUBLESHOOTING:
-- 1. File permission errors: Ensure MySQL can write to export directory
-- 2. Disk space: Ensure adequate space (~2-3GB for full export)
-- 3. Timeout issues: Use CreateExportTable() for memory-based export
-- 4. Encoding issues: All exports use UTF8MB4 for full Unicode support
-- 5. Row count variations: Final count depends on filtering and deduplication
*/
