-- SASS Wikipedia Page Title Cleaner with Identity Pages - UPDATED
-- Converts sass_page to sass_page_clean with standardized titles
-- Creates sass_identity_pages with representative page mapping
-- PRESERVES original Wiki_top3_levels page IDs for levels 0-2
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
-- MAIN CONVERSION PROCEDURE WITH HIERARCHY PRESERVATION
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
  DECLARE v_preserved_hierarchy_pages INT DEFAULT 0;
  
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
  VALUES ('build_status', 0, 'Starting page title cleaning with hierarchy preservation')
  ON DUPLICATE KEY UPDATE state_value = 0, state_text = 'Starting page title cleaning with hierarchy preservation';
  
  -- Progress report
  SELECT 
    'Starting Dual Table Conversion with Hierarchy Preservation' AS status,
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
  -- PHASE 2: BUILD sass_identity_pages WITH HIERARCHY PRESERVATION
  -- ========================================
  
  INSERT INTO clean_build_state (state_key, state_value, state_text) 
  VALUES ('build_phase', 2, 'Building identity pages with hierarchy-preserving representative mapping')
  ON DUPLICATE KEY UPDATE state_value = 2, state_text = 'Building identity pages with hierarchy-preserving representative mapping';
  
  -- Create temporary table for representative page calculation with hierarchy preservation
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
        ORDER BY 
          CASE WHEN page_dag_level <= 2 THEN 0 ELSE 1 END ASC,  -- Prioritize original hierarchy pages (levels 0-2)
          page_dag_level DESC, 
          page_is_leaf ASC, 
          page_id ASC
      ) as rn
    FROM sass_page_clean
  ) ranked
  WHERE rn = 1;
  
  -- Count preserved hierarchy pages
  SELECT COUNT(*) INTO v_preserved_hierarchy_pages
  FROM temp_representatives tr
  JOIN sass_page_clean spc ON tr.representative_page_id = spc.page_id
  WHERE spc.page_dag_level <= 2;
  
  -- Build sass_identity_pages with hierarchy-preserving representative mapping
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
  
  -- Update final build state
  INSERT INTO clean_build_state (state_key, state_value, state_text) 
  VALUES ('build_status', 100, 'Dual table conversion with hierarchy preservation completed successfully')
  ON DUPLICATE KEY UPDATE state_value = 100, state_text = 'Dual table conversion with hierarchy preservation completed successfully';
  
  INSERT INTO clean_build_state (state_key, state_value) 
  VALUES ('total_pages_converted', v_processed_pages)
  ON DUPLICATE KEY UPDATE state_value = v_processed_pages;
  
  INSERT INTO clean_build_state (state_key, state_value) 
  VALUES ('total_identity_pages', v_identity_pages)
  ON DUPLICATE KEY UPDATE state_value = v_identity_pages;
  
  INSERT INTO clean_build_state (state_key, state_value) 
  VALUES ('preserved_hierarchy_representatives', v_preserved_hierarchy_pages)
  ON DUPLICATE KEY UPDATE state_value = v_preserved_hierarchy_pages;
  
  -- ========================================
  -- FINAL SUMMARY REPORT WITH HIERARCHY PRESERVATION METRICS
  -- ========================================
  
  SELECT 
    'COMPLETE - SASS Identity Page Conversion with Hierarchy Preservation' AS final_status,
    FORMAT(v_total_pages, 0) AS original_pages,
    FORMAT(v_processed_pages, 0) AS pages_in_clean_table,
    FORMAT(v_identity_pages, 0) AS identity_pages_created,
    FORMAT((SELECT COUNT(DISTINCT representative_page_id) FROM sass_identity_pages), 0) AS unique_representatives,
    FORMAT(v_preserved_hierarchy_pages, 0) AS hierarchy_representatives_preserved,
    v_batch_count AS total_batches,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_time_sec;
  
  -- Hierarchy preservation validation
  SELECT 
    'Hierarchy Preservation Validation' AS validation_type,
    spc.page_dag_level,
    COUNT(*) AS pages_at_level,
    COUNT(CASE WHEN sip.page_id = sip.representative_page_id THEN 1 END) AS representatives_at_level,
    CONCAT(ROUND(100.0 * COUNT(CASE WHEN sip.page_id = sip.representative_page_id THEN 1 END) / COUNT(*), 1), '%') AS preservation_rate
  FROM sass_identity_pages sip
  JOIN sass_page_clean spc ON sip.page_id = spc.page_id
  WHERE spc.page_dag_level <= 2
  GROUP BY spc.page_dag_level
  ORDER BY spc.page_dag_level;
  
  -- Sample identity groups showing hierarchy-preserved representative mapping
  SELECT 
    'Sample Hierarchy-Preserved Identity Groups' AS sample_type,
    sip.page_title,
    COUNT(*) AS pages_with_same_title,
    sip.representative_page_id,
    rep.page_dag_level AS rep_level,
    CASE WHEN rep.page_is_leaf = 1 THEN 'Article' ELSE 'Category' END AS rep_type,
    CASE WHEN rep.page_dag_level <= 2 THEN 'PRESERVED' ELSE 'STANDARD' END AS selection_type
  FROM sass_identity_pages sip
  JOIN sass_page_clean rep ON sip.representative_page_id = rep.page_id
  GROUP BY sip.page_title, sip.representative_page_id, rep.page_dag_level, rep.page_is_leaf
  HAVING COUNT(*) > 1
  ORDER BY rep.page_dag_level ASC, COUNT(*) DESC
  LIMIT 15;
  
  -- Representative selection statistics with hierarchy metrics
  SELECT 
    'Representative Selection Stats with Hierarchy Preservation' AS metric_type,
    COUNT(DISTINCT page_title) AS unique_titles,
    COUNT(DISTINCT representative_page_id) AS unique_representatives,
    COUNT(DISTINCT CASE WHEN spc.page_dag_level <= 2 THEN representative_page_id END) AS hierarchy_representatives,
    ROUND(AVG(pages_per_title), 1) AS avg_pages_per_title,
    MAX(pages_per_title) AS max_pages_per_title
  FROM (
    SELECT 
      sip.page_title,
      sip.representative_page_id,
      COUNT(*) AS pages_per_title
    FROM sass_identity_pages sip
    JOIN sass_page_clean spc ON sip.representative_page_id = spc.page_id
    GROUP BY sip.page_title, sip.representative_page_id
  ) title_stats
  JOIN sass_page_clean spc ON title_stats.representative_page_id = spc.page_id;
  
END//

DELIMITER ;

-- ========================================
-- SIMPLE CONVERSION PROCEDURE WITH HIERARCHY PRESERVATION
-- ========================================

DROP PROCEDURE IF EXISTS ConvertSASSPageCleanWithIdentitySimple;

DELIMITER //

CREATE PROCEDURE ConvertSASSPageCleanWithIdentitySimple()
BEGIN
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_total_processed INT DEFAULT 0;
  DECLARE v_identity_pages INT DEFAULT 0;
  DECLARE v_preserved_hierarchy_pages INT DEFAULT 0;
  
  SET v_start_time = UNIX_TIMESTAMP();
  
  -- Clear target tables (disable foreign key checks for truncation)
  SET FOREIGN_KEY_CHECKS = 0;
  TRUNCATE TABLE sass_page_clean;
  TRUNCATE TABLE sass_identity_pages;
  SET FOREIGN_KEY_CHECKS = 1;
  
  -- Create identity pages with all pages and hierarchy-preserving representative mapping
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
      ORDER BY 
        CASE WHEN sp.page_dag_level <= 2 THEN 0 ELSE 1 END ASC,  -- Prioritize original hierarchy pages (levels 0-2)
        sp.page_dag_level DESC, 
        sp.page_is_leaf ASC, 
        sp.page_id ASC
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
  
  -- Count preserved hierarchy representatives
  SELECT COUNT(DISTINCT representative_page_id) INTO v_preserved_hierarchy_pages
  FROM sass_identity_pages sip
  JOIN sass_page_clean spc ON sip.representative_page_id = spc.page_id
  WHERE spc.page_dag_level <= 2;
  
  -- Summary report with hierarchy preservation metrics
  SELECT 
    'Simple Identity Conversion with Hierarchy Preservation Complete' AS status,
    FORMAT(v_total_processed, 0) AS pages_converted,
    FORMAT(v_identity_pages, 0) AS identity_pages_created,
    FORMAT((SELECT COUNT(DISTINCT representative_page_id) FROM sass_identity_pages), 0) AS unique_representatives,
    FORMAT(v_preserved_hierarchy_pages, 0) AS hierarchy_representatives_preserved,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_time_sec;
  
  -- Hierarchy preservation validation
  SELECT 
    'Hierarchy Preservation Validation' AS validation_type,
    'Levels 0-2 Representative Preservation' AS metric,
    COUNT(CASE WHEN spc.page_dag_level <= 2 AND sip.page_id = sip.representative_page_id THEN 1 END) AS preserved_count,
    COUNT(CASE WHEN spc.page_dag_level <= 2 THEN 1 END) AS total_hierarchy_pages,
    CONCAT(ROUND(100.0 * COUNT(CASE WHEN spc.page_dag_level <= 2 AND sip.page_id = sip.representative_page_id THEN 1 END) / 
                 COUNT(CASE WHEN spc.page_dag_level <= 2 THEN 1 END), 1), '%') AS preservation_rate
  FROM sass_identity_pages sip
  JOIN sass_page_clean spc ON sip.page_id = spc.page_id;

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

-- Procedure to analyze identity page mapping with hierarchy preservation
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
  
  -- Hierarchy preservation analysis
  SELECT 
    'Hierarchy Preservation Analysis' AS analysis_type,
    spc.page_dag_level,
    COUNT(*) AS total_pages,
    COUNT(CASE WHEN sip.page_id = sip.representative_page_id THEN 1 END) AS representatives,
    COUNT(CASE WHEN sip.page_id != sip.representative_page_id THEN 1 END) AS non_representatives,
    CONCAT(ROUND(100.0 * COUNT(CASE WHEN sip.page_id = sip.representative_page_id THEN 1 END) / COUNT(*), 1), '%') AS representation_rate
  FROM sass_identity_pages sip
  JOIN sass_page_clean spc ON sip.page_id = spc.page_id
  WHERE spc.page_dag_level <= 5
  GROUP BY spc.page_dag_level
  ORDER BY spc.page_dag_level;
  
  -- Largest identity groups with hierarchy status
  SELECT 
    'Largest Identity Groups with Hierarchy Status' AS analysis_type,
    page_title,
    COUNT(*) AS pages_in_group,
    representative_page_id,
    MAX(spc.page_dag_level) AS max_level,
    MIN(spc.page_dag_level) AS min_level,
    CASE WHEN MIN(spc.page_dag_level) <= 2 THEN 'HIERARCHY PRESERVED' ELSE 'STANDARD SELECTION' END AS selection_method
  FROM sass_identity_pages sip
  JOIN sass_page_clean spc ON sip.representative_page_id = spc.page_id
  GROUP BY page_title, representative_page_id
  ORDER BY COUNT(*) DESC
  LIMIT 20;
  
  -- Representative selection analysis with hierarchy metrics
  SELECT 
    'Representative Selection Analysis with Hierarchy' AS analysis_type,
    'Total representatives' AS selection_type,
    COUNT(DISTINCT representative_page_id) AS count
  FROM sass_identity_pages
  
  UNION ALL
  
  SELECT 
    'Representative Selection Analysis with Hierarchy',
    'Hierarchy representatives (levels 0-2)',
    COUNT(DISTINCT sip.representative_page_id)
  FROM sass_identity_pages sip
  JOIN sass_page_clean spc ON sip.representative_page_id = spc.page_id
  WHERE spc.page_dag_level <= 2
  
  UNION ALL
  
  SELECT 
    'Representative Selection Analysis with Hierarchy',
    'Standard representatives (levels 3+)',
    COUNT(DISTINCT sip.representative_page_id)
  FROM sass_identity_pages sip
  JOIN sass_page_clean spc ON sip.representative_page_id = spc.page_id
  WHERE spc.page_dag_level > 2;

END//

DELIMITER ;

-- Procedure to validate hierarchy preservation
DROP PROCEDURE IF EXISTS ValidateHierarchyPreservation;

DELIMITER //

CREATE PROCEDURE ValidateHierarchyPreservation()
BEGIN
  -- Check if any original hierarchy pages lost representative status
  SELECT 
    'Hierarchy Preservation Validation' AS validation_type,
    COUNT(*) AS total_hierarchy_pages,
    COUNT(CASE WHEN sip.page_id = sip.representative_page_id THEN 1 END) AS preserved_as_representatives,
    COUNT(CASE WHEN sip.page_id != sip.representative_page_id THEN 1 END) AS demoted_from_representative,
    CASE 
      WHEN COUNT(CASE WHEN sip.page_id != sip.representative_page_id THEN 1 END) = 0 THEN 'PASS - All hierarchy pages preserved'
      ELSE 'FAIL - Some hierarchy pages demoted'
    END AS validation_status
  FROM sass_identity_pages sip
  JOIN sass_page_clean spc ON sip.page_id = spc.page_id
  WHERE spc.page_dag_level <= 2;
  
  -- Show any demoted hierarchy pages (should be empty)
  SELECT 
    'Demoted Hierarchy Pages (Should be Empty)' AS issue_type,
    sip.page_id,
    sip.page_title,
    spc.page_dag_level,
    sip.representative_page_id AS demoted_to_representative
  FROM sass_identity_pages sip
  JOIN sass_page_clean spc ON sip.page_id = spc.page_id
  WHERE spc.page_dag_level <= 2
    AND sip.page_id != sip.representative_page_id
  LIMIT 10;
  
  -- Verify Technology page preservation (example)
  SELECT 
    'Technology Page Preservation Check' AS check_type,
    sip.page_id,
    sip.page_title,
    spc.page_dag_level,
    sip.representative_page_id,
    CASE 
      WHEN sip.page_id = sip.representative_page_id THEN 'PRESERVED as representative'
      ELSE 'DEMOTED - pointing to different representative'
    END AS preservation_status
  FROM sass_identity_pages sip
  JOIN sass_page_clean spc ON sip.page_id = spc.page_id
  WHERE LOWER(sip.page_title) LIKE '%technology%'
    AND spc.page_dag_level <= 2
  LIMIT 5;

END//

DELIMITER ;

-- ========================================
-- USAGE EXAMPLES AND DOCUMENTATION
-- ========================================

/*
-- ENHANCED TITLE CLEANING WITH HIERARCHY PRESERVATION

-- Standard conversion with hierarchy preservation:
CALL ConvertSASSPageCleanWithIdentity(100000);

-- Simple conversion with hierarchy preservation:
CALL ConvertSASSPageCleanWithIdentitySimple();

-- Test enhanced title cleaning:
CALL TestEnhancedTitleCleaning('Technology');
CALL TestEnhancedTitleCleaning('Machine Learning (AI)');

-- Analyze identity mapping with hierarchy metrics:
CALL AnalyzeIdentityMapping();

-- Validate hierarchy preservation:
CALL ValidateHierarchyPreservation();

-- Check conversion status:
SELECT * FROM clean_build_state ORDER BY updated_at DESC;

-- Verify specific hierarchy pages preserved:
SELECT 
  sip.page_id,
  sip.page_title,
  spc.page_dag_level,
  sip.representative_page_id,
  CASE WHEN sip.page_id = sip.representative_page_id THEN 'PRESERVED' ELSE 'DEMOTED' END AS status
FROM sass_identity_pages sip
JOIN sass_page_clean spc ON sip.page_id = spc.page_id
WHERE spc.page_dag_level <= 2
ORDER BY spc.page_dag_level, sip.page_title;

KEY HIERARCHY PRESERVATION FEATURES:
1. Modified representative selection to prioritize pages with page_dag_level <= 2
2. Original Wiki_top3_levels page IDs (levels 0-2) maintain representative status
3. Standard representative selection applies only to levels 3+
4. Comprehensive validation procedures to verify preservation
5. Technology page ID 696648 will be preserved instead of replaced by 29816

REPRESENTATIVE SELECTION CRITERIA (UPDATED):
1. FIRST: Original hierarchy pages (page_dag_level <= 2)
2. THEN: Highest page_dag_level (deepest in hierarchy)
3. THEN: Prefer page_is_leaf = 0 (categories over articles)
4. FINALLY: Use lowest page_id for tie-breaking

PERFORMANCE ESTIMATE:
- 9M records: ~15-20 minutes total runtime
- Hierarchy preservation adds minimal overhead (~2-3% increase)
- Enhanced normalization: +25% processing time
- Identity mapping with hierarchy-aware window functions: +40% for representative calculation
- Memory usage: ~2GB peak during window function operations
*/
