-- SASS Wikipedia Page Title Cleaner
-- Converts sass_page to sass_page_clean with standardized titles
-- Applies text normalization for improved matching and search

-- ========================================
-- TABLE DEFINITIONS
-- ========================================

-- Clean SASS page table with identical structure to sass_page
CREATE TABLE IF NOT EXISTS sass_page_clean (
  page_id INT UNSIGNED NOT NULL,
  page_title VARCHAR(255) NOT NULL,
  page_parent_id INT NOT NULL,
  page_root_id INT NOT NULL,
  page_dag_level INT NOT NULL,
  page_is_leaf TINYINT(1) NOT NULL DEFAULT 0,
  
  PRIMARY KEY (page_id),
  INDEX idx_title (page_title),
  INDEX idx_parent (page_parent_id),
  INDEX idx_root (page_root_id),
  INDEX idx_level (page_dag_level),
  INDEX idx_leaf (page_is_leaf)
) ENGINE=InnoDB;

-- Build progress tracking
CREATE TABLE IF NOT EXISTS clean_build_state (
  state_key VARCHAR(255) PRIMARY KEY,
  state_value INT NOT NULL,
  state_text VARCHAR(500) NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- ========================================
-- TITLE CLEANING FUNCTION
-- ========================================

DELIMITER //

-- Function to clean page titles according to specifications
CREATE FUNCTION IF NOT EXISTS clean_page_title(
  original_title VARCHAR(255)
) RETURNS VARCHAR(255)
READS SQL DATA
DETERMINISTIC
BEGIN
  DECLARE cleaned_title VARCHAR(255);
  
  SET cleaned_title = original_title;
  
  -- Step 1: Convert whitespace variations to single underscore
  SET cleaned_title = REGEXP_REPLACE(cleaned_title, '\\s+', '_');
  
  -- Step 2: Remove all quotes
  SET cleaned_title = REGEXP_REPLACE(cleaned_title, '[''''""„‚"'']', '');
  
  -- Step 3: Convert currency to text
  SET cleaned_title = REPLACE(cleaned_title, '€', 'Euro');
  SET cleaned_title = REPLACE(cleaned_title, '£', 'Pound');
  SET cleaned_title = REPLACE(cleaned_title, '¥', 'Yen');
  SET cleaned_title = REPLACE(cleaned_title, '$', 'Dollar');
  
  -- Step 4: Remove specific symbols
  SET cleaned_title = REGEXP_REPLACE(cleaned_title, '[§©®™]', '');
  
  -- Step 5: Replace punctuation clusters with single underscore
  SET cleaned_title = REGEXP_REPLACE(cleaned_title, '[!@#%^&*+={}|;:<>?,\\/]+', '_');
  
  -- Step 6: Convert multiple underscores to single underscore
  SET cleaned_title = REGEXP_REPLACE(cleaned_title, '_{2,}', '_');
  
  -- Step 7: Trim leading and trailing underscores
  SET cleaned_title = TRIM(BOTH '_' FROM cleaned_title);
  
  -- Ensure not empty
  IF LENGTH(cleaned_title) = 0 THEN
    SET cleaned_title = 'Empty_Title';
  END IF;
  
  RETURN cleaned_title;
END//

DELIMITER ;

-- ========================================
-- MAIN CONVERSION PROCEDURE
-- ========================================

DROP PROCEDURE IF EXISTS ConvertSASSPageClean;

DELIMITER //

CREATE PROCEDURE ConvertSASSPageClean(
  IN p_batch_size INT
)
BEGIN
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_total_pages INT DEFAULT 0;
  DECLARE v_processed_pages INT DEFAULT 0;
  DECLARE v_batch_count INT DEFAULT 0;
  DECLARE v_last_page_id INT DEFAULT 0;
  DECLARE v_continue TINYINT(1) DEFAULT 1;
  
  -- Set defaults
  IF p_batch_size IS NULL THEN SET p_batch_size = 100000; END IF;
  
  SET v_start_time = UNIX_TIMESTAMP();
  
  -- Get total count
  SELECT COUNT(*) INTO v_total_pages FROM sass_page;
  
  -- Clear target table
  TRUNCATE TABLE sass_page_clean;
  
  -- Initialize build state
  INSERT INTO clean_build_state (state_key, state_value, state_text) 
  VALUES ('build_status', 0, 'Starting page title cleaning')
  ON DUPLICATE KEY UPDATE state_value = 0, state_text = 'Starting page title cleaning';
  
  -- Progress report
  SELECT 
    'Starting Conversion' AS status,
    FORMAT(v_total_pages, 0) AS total_pages_to_process,
    p_batch_size AS batch_size;
  
  -- ========================================
  -- BATCH PROCESSING LOOP
  -- ========================================
  
  WHILE v_continue = 1 DO
    SET v_batch_count = v_batch_count + 1;
    
    -- Process batch with title cleaning
    INSERT INTO sass_page_clean (
      page_id,
      page_title,
      page_parent_id,
      page_root_id,
      page_dag_level,
      page_is_leaf
    )
    SELECT 
      sp.page_id,
      clean_page_title(sp.page_title) as page_title,
      sp.page_parent_id,
      sp.page_root_id,
      sp.page_dag_level,
      sp.page_is_leaf
    FROM sass_page sp
    WHERE sp.page_id > v_last_page_id
    ORDER BY sp.page_id
    LIMIT p_batch_size;
    
    SET v_processed_pages = v_processed_pages + ROW_COUNT();
    
    -- Update last processed ID for next batch
    SELECT MAX(page_id) INTO v_last_page_id FROM sass_page_clean;
    
    -- Update progress
    INSERT INTO clean_build_state (state_key, state_value) 
    VALUES ('pages_processed', v_processed_pages)
    ON DUPLICATE KEY UPDATE state_value = v_processed_pages;
    
    -- Progress report
    SELECT 
      CONCAT('Batch ', v_batch_count, ' Complete') AS status,
      FORMAT(ROW_COUNT(), 0) AS pages_in_batch,
      FORMAT(v_processed_pages, 0) AS total_processed,
      CONCAT(ROUND(100.0 * v_processed_pages / v_total_pages, 1), '%') AS progress,
      ROUND(UNIX_TIMESTAMP() - v_start_time, 1) AS elapsed_sec;
    
    -- Check if done
    IF ROW_COUNT() < p_batch_size OR v_processed_pages >= v_total_pages THEN
      SET v_continue = 0;
    END IF;
    
  END WHILE;
  
  -- Update final build state
  INSERT INTO clean_build_state (state_key, state_value, state_text) 
  VALUES ('build_status', 100, 'Conversion completed successfully')
  ON DUPLICATE KEY UPDATE state_value = 100, state_text = 'Conversion completed successfully';
  
  INSERT INTO clean_build_state (state_key, state_value) 
  VALUES ('total_pages_converted', v_processed_pages)
  ON DUPLICATE KEY UPDATE state_value = v_processed_pages;
  
  -- ========================================
  -- FINAL SUMMARY REPORT
  -- ========================================
  
  SELECT 
    'COMPLETE - SASS Page Title Cleaning' AS final_status,
    FORMAT(v_total_pages, 0) AS original_pages,
    FORMAT(v_processed_pages, 0) AS converted_pages,
    v_batch_count AS total_batches,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_time_sec;
  
  -- Sample cleaned titles
  SELECT 
    'Sample Title Conversions' AS sample_type,
    CONVERT(sp.page_title, CHAR) AS original_title,
    spc.page_title AS cleaned_title,
    spc.page_dag_level AS level,
    CASE WHEN spc.page_is_leaf = 1 THEN 'Article' ELSE 'Category' END AS type
  FROM sass_page sp
  JOIN sass_page_clean spc ON sp.page_id = spc.page_id
  WHERE sp.page_title != spc.page_title
  ORDER BY RAND()
  LIMIT 10;
  
  -- Cleaning statistics
  SELECT 
    'Title Cleaning Statistics' AS metric_type,
    FORMAT(COUNT(*), 0) AS total_titles,
    FORMAT(SUM(CASE WHEN sp.page_title = spc.page_title THEN 1 ELSE 0 END), 0) AS unchanged_titles,
    FORMAT(SUM(CASE WHEN sp.page_title != spc.page_title THEN 1 ELSE 0 END), 0) AS modified_titles,
    CONCAT(ROUND(100.0 * SUM(CASE WHEN sp.page_title != spc.page_title THEN 1 ELSE 0 END) / COUNT(*), 1), '%') AS modification_rate
  FROM sass_page sp
  JOIN sass_page_clean spc ON sp.page_id = spc.page_id;
  
END//

DELIMITER ;

-- ========================================
-- SIMPLE CONVERSION PROCEDURE
-- ========================================

DROP PROCEDURE IF EXISTS ConvertSASSPageCleanSimple;

DELIMITER //

CREATE PROCEDURE ConvertSASSPageCleanSimple()
BEGIN
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_total_processed INT DEFAULT 0;
  
  SET v_start_time = UNIX_TIMESTAMP();
  
  -- Clear target table
  TRUNCATE TABLE sass_page_clean;
  
  -- Single query conversion with title cleaning
  INSERT INTO sass_page_clean (
    page_id,
    page_title,
    page_parent_id,
    page_root_id,
    page_dag_level,
    page_is_leaf
  )
  SELECT 
    sp.page_id,
    clean_page_title(sp.page_title) as page_title,
    sp.page_parent_id,
    sp.page_root_id,
    sp.page_dag_level,
    sp.page_is_leaf
  FROM sass_page sp;
  
  SET v_total_processed = ROW_COUNT();
  
  -- Summary report
  SELECT 
    'Simple Conversion Complete' AS status,
    FORMAT(v_total_processed, 0) AS pages_converted,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_time_sec;
  
  -- Sample results
  SELECT 
    CONVERT(sp.page_title, CHAR) AS original_title,
    spc.page_title AS cleaned_title,
    spc.page_dag_level AS level
  FROM sass_page sp
  JOIN sass_page_clean spc ON sp.page_id = spc.page_id
  WHERE sp.page_title != spc.page_title
  LIMIT 5;

END//

DELIMITER ;

-- ========================================
-- UTILITY PROCEDURES
-- ========================================

-- Procedure to test title cleaning function
DROP PROCEDURE IF EXISTS TestTitleCleaning;

DELIMITER //

CREATE PROCEDURE TestTitleCleaning(
  IN p_test_title VARCHAR(255)
)
BEGIN
  SELECT 
    'Title Cleaning Test' AS test_type,
    p_test_title AS original_title,
    clean_page_title(p_test_title) AS cleaned_title,
    LENGTH(p_test_title) AS original_length,
    LENGTH(clean_page_title(p_test_title)) AS cleaned_length;
END//

DELIMITER ;

-- Procedure to analyze cleaning patterns
DROP PROCEDURE IF EXISTS AnalyzeCleaningPatterns;

DELIMITER //

CREATE PROCEDURE AnalyzeCleaningPatterns()
BEGIN
  -- Most common title changes
  SELECT 
    'Common Title Modifications' AS analysis_type,
    CONVERT(sp.page_title, CHAR) AS original_pattern,
    spc.page_title AS cleaned_pattern,
    COUNT(*) AS occurrence_count
  FROM sass_page sp
  JOIN sass_page_clean spc ON sp.page_id = spc.page_id
  WHERE sp.page_title != spc.page_title
  GROUP BY sp.page_title, spc.page_title
  HAVING COUNT(*) > 1
  ORDER BY COUNT(*) DESC
  LIMIT 10;
  
  -- Length distribution changes
  SELECT 
    'Title Length Changes' AS analysis_type,
    'Original Avg Length' AS metric,
    ROUND(AVG(LENGTH(CONVERT(sp.page_title, CHAR))), 1) AS value
  FROM sass_page sp
  
  UNION ALL
  
  SELECT 
    'Title Length Changes',
    'Cleaned Avg Length',
    ROUND(AVG(LENGTH(spc.page_title)), 1)
  FROM sass_page_clean spc;

END//

DELIMITER ;

-- ========================================
-- USAGE EXAMPLES AND DOCUMENTATION
-- ========================================

/*
-- TITLE CLEANING EXAMPLES

-- Standard conversion with batching:
CALL ConvertSASSPageClean(100000);

-- Simple conversion (single query):
CALL ConvertSASSPageCleanSimple();

-- Test title cleaning function:
CALL TestTitleCleaning('Machine_learning!!@@');
CALL TestTitleCleaning('Artificial$$intelligence##');
CALL TestTitleCleaning('Computer   science???');

-- Analyze cleaning patterns:
CALL AnalyzeCleaningPatterns();

-- Check conversion status:
SELECT * FROM clean_build_state ORDER BY updated_at DESC;

-- Sample queries on cleaned data:

-- Compare original vs cleaned titles:
SELECT 
  CONVERT(sp.page_title, CHAR) AS original,
  spc.page_title AS cleaned,
  spc.page_dag_level
FROM sass_page sp
JOIN sass_page_clean spc ON sp.page_id = spc.page_id
WHERE sp.page_title != spc.page_title
LIMIT 20;

-- Find pages with significant title changes:
SELECT 
  CONVERT(sp.page_title, CHAR) AS original,
  spc.page_title AS cleaned,
  LENGTH(CONVERT(sp.page_title, CHAR)) - LENGTH(spc.page_title) AS length_diff
FROM sass_page sp
JOIN sass_page_clean spc ON sp.page_id = spc.page_id
WHERE ABS(LENGTH(CONVERT(sp.page_title, CHAR)) - LENGTH(spc.page_title)) > 5
ORDER BY length_diff DESC
LIMIT 15;

CLEANING TRANSFORMATIONS:
1. Whitespace variations → single underscore
2. All quotes → removed
3. Currency symbols → text equivalents (€→Euro, £→Pound, ¥→Yen, $→Dollar)
4. Symbols §©®™ → removed
5. Punctuation clusters → single underscore
6. Multiple underscores → single underscore
7. Leading/trailing underscores → trimmed

PERFORMANCE NOTES:
- Batched procedure recommended for large datasets
- Simple procedure faster for sufficient memory
- Function-based cleaning enables easy customization
- All original data preserved in sass_page table
*/
