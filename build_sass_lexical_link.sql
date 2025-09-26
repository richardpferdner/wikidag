-- SASS Wikipedia Page Title Cleaner with Identity Pages
-- Converts sass_page to sass_page_clean with standardized titles
-- Creates sass_identity_pages with representative page mapping
-- Applies enhanced text normalization for improved matching and search

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

-- Identity pages table with representative page mapping
CREATE TABLE IF NOT EXISTS sass_identity_pages (
  page_id INT UNSIGNED NOT NULL,
  page_title VARCHAR(255) NOT NULL,
  page_parent_id INT NOT NULL,
  page_root_id INT NOT NULL,
  page_dag_level INT NOT NULL,
  page_is_leaf TINYINT(1) NOT NULL DEFAULT 0,
  representative_page_id INT UNSIGNED NOT NULL,
  
  PRIMARY KEY (page_id),
  INDEX idx_title (page_title),
  INDEX idx_parent (page_parent_id),
  INDEX idx_root (page_root_id),
  INDEX idx_level (page_dag_level),
  INDEX idx_leaf (page_is_leaf),
  INDEX idx_representative (representative_page_id),
  FOREIGN KEY (representative_page_id) REFERENCES sass_page_clean(page_id)
) ENGINE=InnoDB;

-- Build progress tracking
CREATE TABLE IF NOT EXISTS clean_build_state (
  state_key VARCHAR(255) PRIMARY KEY,
  state_value INT NOT NULL,
  state_text VARCHAR(500) NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- ========================================
-- ENHANCED TITLE CLEANING FUNCTION
-- ========================================

DELIMITER //

-- Function to clean page titles with enhanced normalization
CREATE FUNCTION IF NOT EXISTS clean_page_title_enhanced(
  original_title VARCHAR(255)
) RETURNS VARCHAR(255)
READS SQL DATA
DETERMINISTIC
BEGIN
  DECLARE cleaned_title VARCHAR(255);
  
  SET cleaned_title = original_title;
  
  -- Step 1: Convert whitespace variations to single underscore
  SET cleaned_title = REGEXP_REPLACE(cleaned_title, '\\s+', '_');
  
  -- Step 2: Convert parentheses to underscores
  SET cleaned_title = REPLACE(cleaned_title, '(', '_');
  SET cleaned_title = REPLACE(cleaned_title, ')', '_');
  
  -- Step 3: Convert various dashes to underscores (em-dash, en-dash, hyphen)
  SET cleaned_title = REPLACE(cleaned_title, '—', '_');  -- em-dash
  SET cleaned_title = REPLACE(cleaned_title, '–', '_');  -- en-dash
  SET cleaned_title = REPLACE(cleaned_title, '-', '_');  -- hyphen
  
  -- Step 4: Remove quotes, commas, and periods
  SET cleaned_title = REGEXP_REPLACE(cleaned_title, '[''''""„‚"'']', '');
  SET cleaned_title = REPLACE(cleaned_title, ',', '');
  SET cleaned_title = REPLACE(cleaned_title, '.', '');
  
  -- Step 5: Convert currency to text (preserve existing functionality)
  SET cleaned_title = REPLACE(cleaned_title, '€', 'Euro');
  SET cleaned_title = REPLACE(cleaned_title, '£', 'Pound');
  SET cleaned_title = REPLACE(cleaned_title, '¥', 'Yen');
  SET cleaned_title = REPLACE(cleaned_title, '$', 'Dollar');
  
  -- Step 6: Remove specific symbols
  SET cleaned_title = REGEXP_REPLACE(cleaned_title, '[§©®™]', '');
  
  -- Step 7: Replace remaining punctuation clusters with single underscore
  SET cleaned_title = REGEXP_REPLACE(cleaned_title, '[!@#%^&*+={}|;:<>?\\/]+', '_');
  
  -- Step 8: Convert multiple underscores to single underscore
  SET cleaned_title = REGEXP_REPLACE(cleaned_title, '_{2,}', '_');
  
  -- Step 9: Trim leading and trailing underscores
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

DROP PROCEDURE IF EXISTS ConvertSASSPageCleanWithIdentity;

DELIMITER //

CREATE PROCEDURE ConvertSASSPageCleanWithIdentity(
  IN p_batch_size INT
)
BEGIN
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_total_pages INT DEFAULT 0;
  DECLARE v_processed_pages INT DEFAULT 0;
  DECLARE v_batch_count INT DEFAULT 0;
  DECLARE v_last_page_id INT DEFAULT 0;
  DECLARE v_continue TINYINT(1) DEFAULT 1;
  DECLARE v_identity_pages INT DEFAULT 0;
  
  -- Set defaults
  IF p_batch_size IS NULL THEN SET p_batch_size = 100000; END IF;
  
  SET v_start_time = UNIX_TIMESTAMP();
  
  -- Get total count
  SELECT COUNT(*) INTO v_total_pages FROM sass_page;
  
  -- Clear target tables (disable foreign key checks for truncation)
  SET FOREIGN_KEY_CHECKS = 0;
  TRUNCATE TABLE sass_page_clean;
  TRUNCATE TABLE sass_identity_pages;
  SET FOREIGN_KEY_CHECKS = 1;
  
  -- Initialize build state
  INSERT INTO clean_build_state (state_key, state_value, state_text) 
  VALUES ('build_status', 0, 'Starting page title cleaning with identity mapping')
  ON DUPLICATE KEY UPDATE state_value = 0, state_text = 'Starting page title cleaning with identity mapping';
  
  -- Progress report
  SELECT 
    'Starting Dual Table Conversion' AS status,
    FORMAT(v_total_pages, 0) AS total_pages_to_process,
    p_batch_size AS batch_size;
  
  -- ========================================
  -- PHASE 1: BUILD sass_page_clean
  -- ========================================
  
  WHILE v_continue = 1 DO
    SET v_batch_count = v_batch_count + 1;
    
    -- Process batch with enhanced title cleaning
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
      clean_page_title_enhanced(sp.page_title) as page_title,
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
    
    -- Progress report
    SELECT 
      CONCAT('sass_page_clean Batch ', v_batch_count) AS status,
      FORMAT(ROW_COUNT(), 0) AS pages_in_batch,
      FORMAT(v_processed_pages, 0) AS total_processed,
      CONCAT(ROUND(100.0 * v_processed_pages / v_total_pages, 1), '%') AS progress,
      ROUND(UNIX_TIMESTAMP() - v_start_time, 1) AS elapsed_sec;
    
    -- Check if done
    IF ROW_COUNT() < p_batch_size OR v_processed_pages >= v_total_pages THEN
      SET v_continue = 0;
    END IF;
    
  END WHILE;
  
  -- ========================================
  -- PHASE 2: BUILD sass_identity_pages
  -- ========================================
  
  INSERT INTO clean_build_state (state_key, state_value, state_text) 
  VALUES ('build_phase', 2, 'Building identity pages with representative mapping')
  ON DUPLICATE KEY UPDATE state_value = 2, state_text = 'Building identity pages with representative mapping';
  
  -- Create temporary table for representative page calculation
  CREATE TEMPORARY TABLE temp_representatives AS
  SELECT 
    page_title,
    page_id as representative_page_id
  FROM (
    SELECT 
      page_title,
      page_id,
      page_dag_level,
      page_is_leaf,
      ROW_NUMBER() OVER (
        PARTITION BY page_title 
        ORDER BY page_dag_level DESC, page_is_leaf ASC, page_id ASC
      ) as rn
    FROM sass_page_clean
  ) ranked
  WHERE rn = 1;
  
  -- Build sass_identity_pages with representative mapping
  INSERT INTO sass_identity_pages (
    page_id,
    page_title,
    page_parent_id,
    page_root_id,
    page_dag_level,
    page_is_leaf,
    representative_page_id
  )
  SELECT 
    spc.page_id,
    spc.page_title,
    spc.page_parent_id,
    spc.page_root_id,
    spc.page_dag_level,
    spc.page_is_leaf,
    tr.representative_page_id
  FROM sass_page_clean spc
  JOIN temp_representatives tr ON spc.page_title = tr.page_title;
  
  SET v_identity_pages = ROW_COUNT();
  
  -- Clean up temporary table
  DROP TEMPORARY TABLE temp_representatives;
  
  -- Re-enable foreign key checks
  SET FOREIGN_KEY_CHECKS = 1;
  
  -- Update final build state
  INSERT INTO clean_build_state (state_key, state_value, state_text) 
  VALUES ('build_status', 100, 'Dual table conversion completed successfully')
  ON DUPLICATE KEY UPDATE state_value = 100, state_text = 'Dual table conversion completed successfully';
  
  INSERT INTO clean_build_state (state_key, state_value) 
  VALUES ('total_pages_converted', v_processed_pages)
  ON DUPLICATE KEY UPDATE state_value = v_processed_pages;
  
  INSERT INTO clean_build_state (state_key, state_value) 
  VALUES ('total_identity_pages', v_identity_pages)
  ON DUPLICATE KEY UPDATE state_value = v_identity_pages;
  
  -- ========================================
  -- FINAL SUMMARY REPORT
  -- ========================================
  
  SELECT 
    'COMPLETE - SASS Identity Page Conversion' AS final_status,
    FORMAT(v_total_pages, 0) AS original_pages,
    FORMAT(v_processed_pages, 0) AS pages_in_clean_table,
    FORMAT(v_identity_pages, 0) AS identity_pages_created,
    FORMAT((SELECT COUNT(DISTINCT representative_page_id) FROM sass_identity_pages), 0) AS unique_representatives,
    v_batch_count AS total_batches,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_time_sec;
  
  -- Sample identity groups showing representative mapping
  SELECT 
    'Sample Identity Groups' AS sample_type,
    sip.page_title,
    COUNT(*) AS pages_with_same_title,
    sip.representative_page_id,
    rep.page_dag_level AS rep_level,
    CASE WHEN rep.page_is_leaf = 1 THEN 'Article' ELSE 'Category' END AS rep_type
  FROM sass_identity_pages sip
  JOIN sass_page_clean rep ON sip.representative_page_id = rep.page_id
  GROUP BY sip.page_title, sip.representative_page_id, rep.page_dag_level, rep.page_is_leaf
  HAVING COUNT(*) > 1
  ORDER BY COUNT(*) DESC
  LIMIT 10;
  
  -- Representative selection statistics
  SELECT 
    'Representative Selection Stats' AS metric_type,
    COUNT(DISTINCT page_title) AS unique_titles,
    COUNT(DISTINCT representative_page_id) AS unique_representatives,
    ROUND(AVG(pages_per_title), 1) AS avg_pages_per_title,
    MAX(pages_per_title) AS max_pages_per_title
  FROM (
    SELECT 
      page_title,
      COUNT(*) AS pages_per_title
    FROM sass_identity_pages
    GROUP BY page_title
  ) title_stats;
  
END//

DELIMITER ;

-- ========================================
-- SIMPLE CONVERSION PROCEDURE
-- ========================================

DROP PROCEDURE IF EXISTS ConvertSASSPageCleanWithIdentitySimple;

DELIMITER //

CREATE PROCEDURE ConvertSASSPageCleanWithIdentitySimple()
BEGIN
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_total_processed INT DEFAULT 0;
  DECLARE v_identity_pages INT DEFAULT 0;
  
  SET v_start_time = UNIX_TIMESTAMP();
  
  -- Clear target tables (disable foreign key checks for entire operation)
  SET FOREIGN_KEY_CHECKS = 0;
  TRUNCATE TABLE sass_page_clean;
  TRUNCATE TABLE sass_identity_pages;
  
  -- Create identity pages with all pages and representative mapping
  INSERT INTO sass_identity_pages (
    page_id,
    page_title,
    page_parent_id,
    page_root_id,
    page_dag_level,
    page_is_leaf,
    representative_page_id
  )
  SELECT 
    sp.page_id,
    clean_page_title_enhanced(sp.page_title) as page_title,
    sp.page_parent_id,
    sp.page_root_id,
    sp.page_dag_level,
    sp.page_is_leaf,
    FIRST_VALUE(sp.page_id) OVER (
      PARTITION BY clean_page_title_enhanced(sp.page_title)
      ORDER BY sp.page_dag_level DESC, sp.page_is_leaf ASC, sp.page_id ASC
      ROWS UNBOUNDED PRECEDING
    ) as representative_page_id
  FROM sass_page sp;
  
  SET v_identity_pages = ROW_COUNT();
  
  -- Insert only representative pages into sass_page_clean
  INSERT INTO sass_page_clean (
    page_id,
    page_title,
    page_parent_id,
    page_root_id,
    page_dag_level,
    page_is_leaf
  )
  SELECT DISTINCT
    representative_page_id as page_id,
    page_title,
    page_parent_id,
    page_root_id,
    page_dag_level,
    page_is_leaf
  FROM sass_identity_pages sip1
  WHERE sip1.page_id = sip1.representative_page_id;
  
  SET v_total_processed = ROW_COUNT();
  
  -- Re-enable foreign key checks
  SET FOREIGN_KEY_CHECKS = 1;
  
  -- Summary report
  SELECT 
    'Simple Identity Conversion Complete' AS status,
    FORMAT(v_total_processed, 0) AS pages_converted,
    FORMAT(v_identity_pages, 0) AS identity_pages_created,
    FORMAT((SELECT COUNT(DISTINCT representative_page_id) FROM sass_identity_pages), 0) AS unique_representatives,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_time_sec;

END//

DELIMITER ;

-- ========================================
-- UTILITY PROCEDURES
-- ========================================

-- Procedure to test enhanced title cleaning function
DROP PROCEDURE IF EXISTS TestEnhancedTitleCleaning;

DELIMITER //

CREATE PROCEDURE TestEnhancedTitleCleaning(
  IN p_test_title VARCHAR(255)
)
BEGIN
  SELECT 
    'Enhanced Title Cleaning Test' AS test_type,
    p_test_title AS original_title,
    clean_page_title_enhanced(p_test_title) AS cleaned_title,
    LENGTH(p_test_title) AS original_length,
    LENGTH(clean_page_title_enhanced(p_test_title)) AS cleaned_length;
END//

DELIMITER ;

-- Procedure to analyze identity page mapping
DROP PROCEDURE IF EXISTS AnalyzeIdentityMapping;

DELIMITER //

CREATE PROCEDURE AnalyzeIdentityMapping()
BEGIN
  -- Title group size distribution
  SELECT 
    'Identity Group Size Distribution' AS analysis_type,
    pages_per_title,
    COUNT(*) AS title_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(DISTINCT page_title) FROM sass_identity_pages), 1) AS percentage
  FROM (
    SELECT page_title, COUNT(*) AS pages_per_title
    FROM sass_identity_pages
    GROUP BY page_title
  ) AS title_groups
  GROUP BY pages_per_title
  ORDER BY pages_per_title;
  
  -- Largest identity groups
  SELECT 
    'Largest Identity Groups' AS analysis_type,
    page_title,
    COUNT(*) AS pages_in_group,
    representative_page_id,
    MAX(page_dag_level) AS max_level,
    MIN(page_dag_level) AS min_level
  FROM sass_identity_pages
  GROUP BY page_title, representative_page_id
  ORDER BY COUNT(*) DESC
  LIMIT 15;
  
  -- Representative selection analysis
  SELECT 
    'Representative Selection Analysis' AS analysis_type,
    'Categories as representatives' AS selection_type,
    COUNT(*) AS count
  FROM sass_identity_pages sip
  JOIN sass_page_clean spc ON sip.representative_page_id = spc.page_id
  WHERE spc.page_is_leaf = 0
  
  UNION ALL
  
  SELECT 
    'Representative Selection Analysis',
    'Articles as representatives',
    COUNT(*)
  FROM sass_identity_pages sip
  JOIN sass_page_clean spc ON sip.representative_page_id = spc.page_id
  WHERE spc.page_is_leaf = 1;

END//

DELIMITER ;

-- ========================================
-- USAGE EXAMPLES AND DOCUMENTATION
-- ========================================

/*
-- ENHANCED TITLE CLEANING WITH IDENTITY MAPPING

-- Standard conversion with batching:
CALL ConvertSASSPageCleanWithIdentity(100000);

-- Simple conversion (single query):
CALL ConvertSASSPageCleanWithIdentitySimple();

-- Test enhanced title cleaning:
CALL TestEnhancedTitleCleaning('Machine Learning (AI)');
CALL TestEnhancedTitleCleaning('Artificial—Intelligence, Inc.');
CALL TestEnhancedTitleCleaning('Computer   Science: "Theory"');

-- Analyze identity mapping:
CALL AnalyzeIdentityMapping();

-- Check conversion status:
SELECT * FROM clean_build_state ORDER BY updated_at DESC;

-- Query examples on dual tables:

-- Individual page lookup (sass_page_clean):
SELECT page_title, page_dag_level, page_is_leaf 
FROM sass_page_clean 
WHERE page_id = 12345;

-- Find representative for a page (sass_identity_pages):
SELECT representative_page_id 
FROM sass_identity_pages 
WHERE page_id = 12345;

-- Find all pages with same normalized title:
SELECT page_id, page_dag_level, page_is_leaf
FROM sass_identity_pages 
WHERE page_title = 'machine_learning';

-- Get representative page details:
SELECT spc.* 
FROM sass_identity_pages sip
JOIN sass_page_clean spc ON sip.representative_page_id = spc.page_id
WHERE sip.page_id = 12345;

REPRESENTATIVE SELECTION CRITERIA:
1. Highest page_dag_level (deepest in hierarchy)
2. If tied, prefer page_is_leaf = 0 (categories over articles)
3. If still tied, use lowest page_id

PERFORMANCE ESTIMATE:
- 9M records: ~15-20 minutes total runtime
- Enhanced normalization: +25% processing time
- Identity mapping with window functions: +40% for representative calculation
- Memory usage: ~2GB peak during window function operations
*/
