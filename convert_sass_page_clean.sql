-- SASS Wikipedia Page Title Cleaner with Identity Pages - UPDATED
-- Converts sass_page to sass_page_clean with standardized titles
-- Creates sass_identity_pages with representative page mapping
-- PRESERVES original Wiki_top3_levels page IDs for levels 0-2
-- FIXES parent_id references to maintain strict level-parent relationships
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

DROP FUNCTION IF EXISTS clean_page_title_enhanced//

CREATE FUNCTION clean_page_title_enhanced(original_title VARCHAR(255))
RETURNS VARCHAR(255)
DETERMINISTIC
BEGIN
  DECLARE cleaned_title VARCHAR(255);
  
  SET cleaned_title = original_title;
  
  -- Step 1: Normalize whitespace (spaces, tabs, newlines)
  SET cleaned_title = REGEXP_REPLACE(cleaned_title, '\\s+', '_');
  
  -- Step 2: Remove parenthetical disambiguation
  SET cleaned_title = REGEXP_REPLACE(cleaned_title, '\\s*\\([^)]*\\)\\s*', '');
  
  -- Step 3: Normalize common variations
  SET cleaned_title = REPLACE(cleaned_title, '–', '-');
  SET cleaned_title = REPLACE(cleaned_title, '—', '-');
  SET cleaned_title = REPLACE(cleaned_title, ''', '\'');
  SET cleaned_title = REPLACE(cleaned_title, ''', '\'');
  SET cleaned_title = REPLACE(cleaned_title, '"', '"');
  SET cleaned_title = REPLACE(cleaned_title, '"', '"');
  
  -- Step 4: Remove common prefixes/suffixes
  SET cleaned_title = REGEXP_REPLACE(cleaned_title, '^(The|A|An)_', '');
  SET cleaned_title = REGEXP_REPLACE(cleaned_title, '_(article|page|category)$', '', 'i');
  SET cleaned_title = REGEXP_REPLACE(cleaned_title, '_(disambiguation)$', '');
  
  -- Step 5: Convert currency to text
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
-- HELPER FUNCTION: FIND VALID PARENT
-- ========================================

DELIMITER //

DROP FUNCTION IF EXISTS find_valid_parent//

CREATE FUNCTION find_valid_parent(
  p_page_id INT UNSIGNED,
  p_page_level INT,
  p_root_id INT
)
RETURNS INT UNSIGNED
DETERMINISTIC
READS SQL DATA
BEGIN
  DECLARE v_parent_id INT UNSIGNED DEFAULT 0;
  
  -- Level 0 has no parent
  IF p_page_level = 0 THEN
    RETURN 0;
  END IF;
  
  -- Try to find any page at level N-1 with same root_id
  SELECT page_id INTO v_parent_id
  FROM sass_page_clean
  WHERE page_dag_level = p_page_level - 1
    AND page_root_id = p_root_id
  ORDER BY page_id ASC
  LIMIT 1;
  
  -- If no parent found at correct level, return root_id
  IF v_parent_id IS NULL THEN
    RETURN p_root_id;
  END IF;
  
  RETURN v_parent_id;
END//

DELIMITER ;

-- ========================================
-- PARENT REMAPPING PROCEDURE
-- ========================================

DELIMITER //

DROP PROCEDURE IF EXISTS remap_parent_ids//

CREATE PROCEDURE remap_parent_ids()
BEGIN
  DECLARE v_current_level INT DEFAULT 3;
  DECLARE v_max_level INT DEFAULT 0;
  DECLARE v_fixed_count INT DEFAULT 0;
  DECLARE v_total_fixed INT DEFAULT 0;
  
  -- Get max level
  SELECT MAX(page_dag_level) INTO v_max_level FROM sass_page_clean;
  
  -- Process each level starting from 3
  WHILE v_current_level <= v_max_level DO
    
    -- Create temp table for parent remapping at this level
    CREATE TEMPORARY TABLE temp_parent_fixes AS
    SELECT 
      c.page_id,
      c.page_parent_id AS old_parent_id,
      COALESCE(
        -- Case 1: Parent exists and is at correct level
        CASE 
          WHEN p.page_id IS NOT NULL AND p.page_dag_level = v_current_level - 1 
          THEN c.page_parent_id
          ELSE NULL
        END,
        -- Case 2: Parent exists but wrong level, find its representative
        CASE
          WHEN p.page_id IS NOT NULL
          THEN (
            SELECT representative_page_id 
            FROM sass_identity_pages 
            WHERE page_id = c.page_parent_id
            LIMIT 1
          )
          ELSE NULL
        END,
        -- Case 3: Parent doesn't exist, check if it has a representative
        (
          SELECT spc.page_id
          FROM sass_identity_pages sip
          JOIN sass_page_clean spc ON sip.representative_page_id = spc.page_id
          WHERE sip.page_id = c.page_parent_id
            AND spc.page_dag_level = v_current_level - 1
          LIMIT 1
        ),
        -- Case 4: Find any valid parent at level N-1
        find_valid_parent(c.page_id, c.page_dag_level, c.page_root_id)
      ) AS new_parent_id
    FROM sass_page_clean c
    LEFT JOIN sass_page_clean p ON c.page_parent_id = p.page_id
    WHERE c.page_dag_level = v_current_level
      AND (
        p.page_id IS NULL 
        OR p.page_dag_level != v_current_level - 1
      );
    
    -- Update parent_ids that need fixing
    UPDATE sass_page_clean c
    INNER JOIN temp_parent_fixes f ON c.page_id = f.page_id
    SET c.page_parent_id = f.new_parent_id
    WHERE f.new_parent_id IS NOT NULL 
      AND f.new_parent_id != f.old_parent_id;
    
    SET v_fixed_count = ROW_COUNT();
    SET v_total_fixed = v_total_fixed + v_fixed_count;
    
    -- Log progress
    IF v_fixed_count > 0 THEN
      INSERT INTO clean_build_state (state_key, state_value, state_text)
      VALUES (CONCAT('level_', v_current_level, '_parent_fixes'), v_fixed_count, 
              CONCAT('Fixed ', v_fixed_count, ' parent references at level ', v_current_level))
      ON DUPLICATE KEY UPDATE 
        state_value = v_fixed_count,
        state_text = CONCAT('Fixed ', v_fixed_count, ' parent references at level ', v_current_level);
    END IF;
    
    DROP TEMPORARY TABLE temp_parent_fixes;
    SET v_current_level = v_current_level + 1;
  END WHILE;
  
  -- Final summary
  INSERT INTO clean_build_state (state_key, state_value, state_text)
  VALUES ('total_parent_fixes', v_total_fixed, CONCAT('Total parent references fixed: ', v_total_fixed))
  ON DUPLICATE KEY UPDATE 
    state_value = v_total_fixed,
    state_text = CONCAT('Total parent references fixed: ', v_total_fixed);
    
  SELECT 
    'Parent Remapping Complete' AS status,
    FORMAT(v_total_fixed, 0) AS total_parent_fixes,
    v_max_level AS max_level_processed;
END//

DELIMITER ;

-- ========================================
-- MAIN CONVERSION PROCEDURE WITH HIERARCHY PRESERVATION
-- ========================================

DELIMITER //

DROP PROCEDURE IF EXISTS ConvertSASSPageCleanWithIdentity//

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
  
  -- Clear target tables
  SET FOREIGN_KEY_CHECKS = 0;
  TRUNCATE TABLE sass_page_clean;
  TRUNCATE TABLE sass_identity_pages;
  SET FOREIGN_KEY_CHECKS = 1;
  
  -- Initialize build state
  INSERT INTO clean_build_state (state_key, state_value, state_text) 
  VALUES ('build_status', 0, 'Starting conversion with parent remapping')
  ON DUPLICATE KEY UPDATE state_value = 0, state_text = 'Starting conversion with parent remapping';
  
  -- Progress report
  SELECT 
    'Starting Conversion with Parent Remapping' AS status,
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
  VALUES ('build_phase', 2, 'Building identity pages with hierarchy preservation')
  ON DUPLICATE KEY UPDATE state_value = 2, state_text = 'Building identity pages with hierarchy preservation';
  
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
        ORDER BY 
          CASE WHEN page_dag_level <= 2 THEN 0 ELSE 1 END ASC,
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
  
  -- Build sass_identity_pages
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
  
  -- ========================================
  -- PHASE 3: REMAP PARENT IDs
  -- ========================================
  
  INSERT INTO clean_build_state (state_key, state_value, state_text) 
  VALUES ('build_phase', 3, 'Remapping parent IDs to maintain level relationships')
  ON DUPLICATE KEY UPDATE state_value = 3, state_text = 'Remapping parent IDs to maintain level relationships';
  
  -- Remove non-representative pages from sass_page_clean
  DELETE FROM sass_page_clean
  WHERE page_id NOT IN (
    SELECT DISTINCT representative_page_id FROM sass_identity_pages
  );
  
  -- Now remap parent IDs
  CALL remap_parent_ids();
  
  -- ========================================
  -- FINAL VALIDATION AND REPORTING
  -- ========================================
  
  INSERT INTO clean_build_state (state_key, state_value, state_text) 
  VALUES ('build_status', 100, 'Conversion with parent remapping completed successfully')
  ON DUPLICATE KEY UPDATE state_value = 100, state_text = 'Conversion with parent remapping completed successfully';
  
  INSERT INTO clean_build_state (state_key, state_value) 
  VALUES ('total_pages_converted', (SELECT COUNT(*) FROM sass_page_clean))
  ON DUPLICATE KEY UPDATE state_value = (SELECT COUNT(*) FROM sass_page_clean);
  
  INSERT INTO clean_build_state (state_key, state_value) 
  VALUES ('total_identity_pages', v_identity_pages)
  ON DUPLICATE KEY UPDATE state_value = v_identity_pages;
  
  INSERT INTO clean_build_state (state_key, state_value) 
  VALUES ('preserved_hierarchy_representatives', v_preserved_hierarchy_pages)
  ON DUPLICATE KEY UPDATE state_value = v_preserved_hierarchy_pages;
  
  -- Final summary report
  SELECT 
    'COMPLETE - Conversion with Parent Remapping' AS final_status,
    FORMAT(v_total_pages, 0) AS original_pages,
    FORMAT((SELECT COUNT(*) FROM sass_page_clean), 0) AS pages_in_clean_table,
    FORMAT(v_identity_pages, 0) AS identity_pages_created,
    FORMAT((SELECT COUNT(DISTINCT representative_page_id) FROM sass_identity_pages), 0) AS unique_representatives,
    FORMAT(v_preserved_hierarchy_pages, 0) AS hierarchy_representatives_preserved,
    v_batch_count AS total_batches,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_time_sec;
  
  -- Validation: Check for orphaned parents
  SELECT 
    'Orphaned Parent Check' AS validation_type,
    FORMAT(COUNT(*), 0) AS orphaned_records,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM sass_page_clean c
  WHERE c.page_parent_id > 0
    AND NOT EXISTS (
      SELECT 1 FROM sass_page_clean p WHERE p.page_id = c.page_parent_id
    );
  
  -- Validation: Check parent-child level relationships
  SELECT 
    'Level Relationship Check' AS validation_type,
    FORMAT(COUNT(*), 0) AS level_violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM sass_page_clean c
  JOIN sass_page_clean p ON c.page_parent_id = p.page_id
  WHERE c.page_parent_id > 0
    AND p.page_dag_level != c.page_dag_level - 1;

END//

DELIMITER ;

-- ========================================
-- SIMPLE CONVERSION PROCEDURE
-- ========================================

DELIMITER //

DROP PROCEDURE IF EXISTS ConvertSASSPageCleanWithIdentitySimple//

CREATE PROCEDURE ConvertSASSPageCleanWithIdentitySimple()
BEGIN
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_total_processed INT DEFAULT 0;
  DECLARE v_identity_pages INT DEFAULT 0;
  DECLARE v_preserved_hierarchy_pages INT DEFAULT 0;
  
  SET v_start_time = UNIX_TIMESTAMP();
  
  -- Clear target tables
  SET FOREIGN_KEY_CHECKS = 0;
  TRUNCATE TABLE sass_page_clean;
  TRUNCATE TABLE sass_identity_pages;
  SET FOREIGN_KEY_CHECKS = 1;
  
  -- Create temporary table with representative mapping
  CREATE TEMPORARY TABLE temp_page_mapping AS
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
        CASE WHEN sp.page_dag_level <= 2 THEN 0 ELSE 1 END ASC,
        sp.page_dag_level DESC, 
        sp.page_is_leaf ASC, 
        sp.page_id ASC
      ROWS UNBOUNDED PRECEDING
    ) as representative_page_id
  FROM sass_page sp;
  
  -- Insert representative pages into sass_page_clean
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
  FROM temp_page_mapping tmp1
  WHERE tmp1.page_id = tmp1.representative_page_id;
  
  SET v_total_processed = ROW_COUNT();
  
  -- Insert into sass_identity_pages
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
    page_id,
    page_title,
    page_parent_id,
    page_root_id,
    page_dag_level,
    page_is_leaf,
    representative_page_id
  FROM temp_page_mapping;
  
  SET v_identity_pages = ROW_COUNT();
  
  DROP TEMPORARY TABLE temp_page_mapping;
  
  -- Count preserved hierarchy representatives
  SELECT COUNT(DISTINCT representative_page_id) INTO v_preserved_hierarchy_pages
  FROM sass_identity_pages sip
  JOIN sass_page_clean spc ON sip.representative_page_id = spc.page_id
  WHERE spc.page_dag_level <= 2;
  
  -- Remap parent IDs
  CALL remap_parent_ids();
  
  -- Summary report
  SELECT 
    'Simple Conversion with Parent Remapping Complete' AS status,
    FORMAT(v_total_processed, 0) AS pages_converted,
    FORMAT(v_identity_pages, 0) AS identity_pages_created,
    FORMAT((SELECT COUNT(DISTINCT representative_page_id) FROM sass_identity_pages), 0) AS unique_representatives,
    FORMAT(v_preserved_hierarchy_pages, 0) AS hierarchy_representatives_preserved,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_time_sec;
  
  -- Validation checks
  SELECT 
    'Orphaned Parent Check' AS validation_type,
    FORMAT(COUNT(*), 0) AS orphaned_records,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM sass_page_clean c
  WHERE c.page_parent_id > 0
    AND NOT EXISTS (
      SELECT 1 FROM sass_page_clean p WHERE p.page_id = c.page_parent_id
    );

END//

DELIMITER ;

-- ========================================
-- ENHANCED VALIDATION PROCEDURES
-- ========================================

DELIMITER //

DROP PROCEDURE IF EXISTS ValidateHierarchyPreservation//

CREATE PROCEDURE ValidateHierarchyPreservation()
BEGIN
  -- Check levels 0-2 preservation
  SELECT 
    'Levels 0-2 Preservation Check' AS check_type,
    COUNT(*) AS total_hierarchy_pages,
    COUNT(CASE WHEN sip.page_id = sip.representative_page_id THEN 1 END) AS preserved_as_representatives,
    CONCAT(ROUND(100.0 * COUNT(CASE WHEN sip.page_id = sip.representative_page_id THEN 1 END) / COUNT(*), 1), '%') AS preservation_rate,
    CASE 
      WHEN COUNT(*) = COUNT(CASE WHEN sip.page_id = sip.representative_page_id THEN 1 END) 
      THEN 'PASS - All hierarchy pages preserved'
      ELSE 'FAIL - Some hierarchy pages demoted'
    END AS validation_status
  FROM sass_identity_pages sip
  JOIN sass_page_clean spc ON sip.page_id = spc.page_id
  WHERE spc.page_dag_level <= 2;
  
  -- Check orphaned parents
  SELECT 
    'Orphaned Parent References' AS check_type,
    FORMAT(COUNT(*), 0) AS orphaned_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM sass_page_clean c
  WHERE c.page_parent_id > 0
    AND NOT EXISTS (
      SELECT 1 FROM sass_page_clean p WHERE p.page_id = c.page_parent_id
    );
  
  -- Check level relationships
  SELECT 
    'Parent-Child Level Relationships' AS check_type,
    FORMAT(COUNT(*), 0) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM sass_page_clean c
  JOIN sass_page_clean p ON c.page_parent_id = p.page_id
  WHERE c.page_parent_id > 0
    AND p.page_dag_level != c.page_dag_level - 1;
  
  -- Level distribution
  SELECT 
    'Level Distribution' AS report_type,
    page_dag_level,
    FORMAT(COUNT(*), 0) AS page_count,
    FORMAT(COUNT(DISTINCT page_parent_id), 0) AS unique_parents
  FROM sass_page_clean
  GROUP BY page_dag_level
  ORDER BY page_dag_level;

END//

DELIMITER ;

-- ========================================
-- UTILITY PROCEDURES
-- ========================================

DELIMITER //

DROP PROCEDURE IF EXISTS TestEnhancedTitleCleaning//

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

DELIMITER //

DROP PROCEDURE IF EXISTS AnalyzeIdentityMapping//

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
    CONCAT(ROUND(100.0 * COUNT(CASE WHEN sip.page_id = sip.representative_page_id THEN 1 END) / COUNT(*), 1), '%') AS preservation_rate
  FROM sass_identity_pages sip
  JOIN sass_page_clean spc ON sip.page_id = spc.page_id
  WHERE spc.page_dag_level <= 2
  GROUP BY spc.page_dag_level
  ORDER BY spc.page_dag_level;
  
  -- Sample identity groups
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
  ORDER BY rep.page_dag_level ASC, COUNT(*) DESC
  LIMIT 15;

END//

DELIMITER ;

-- ========================================
-- USAGE EXAMPLES
-- ========================================

/*
-- CONVERSION WITH PARENT REMAPPING

-- Standard batched conversion:
CALL ConvertSASSPageCleanWithIdentity(100000);

-- Simple one-pass conversion:
CALL ConvertSASSPageCleanWithIdentitySimple();

-- Validate results:
CALL ValidateHierarchyPreservation();

-- Analyze identity mapping:
CALL AnalyzeIdentityMapping();

-- Check conversion status:
SELECT * FROM clean_build_state ORDER BY updated_at DESC;

-- Test specific page hierarchy after conversion:
SELECT 
  c.page_id,
  c.page_title,
  c.page_dag_level,
  c.page_parent_id,
  p.page_title AS parent_title,
  p.page_dag_level AS parent_level
FROM sass_page_clean c
LEFT JOIN sass_page_clean p ON c.page_parent_id = p.page_id
WHERE LOWER(c.page_title) = 'naturalists';

KEY FEATURES:
1. Preserves levels 0-2 pages as representatives
2. Remaps all parent_ids to point to valid representatives
3. Enforces parent.level = child.level - 1 constraint
4. Handles orphaned pages by finding valid parents
5. Comprehensive validation of hierarchy integrity

VALIDATION CHECKS:
- Zero orphaned parent references
- All parent-child relationships maintain level constraints
- Levels 0-2 preserved as representatives
*/
