-- SASS Wikipedia Page Title Cleaner - Refactored
-- Converts sass_page to sass_page_clean with standardized titles
-- Deduplicates to one representative per unique title
-- Filters weak branches (categories with <9 children)
-- Repairs orphan references and removes self-references
-- Processes levels 0-7 only

-- ========================================
-- TABLE DEFINITIONS
-- ========================================

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

CREATE FUNCTION IF NOT EXISTS clean_page_title_enhanced(
  input_title VARCHAR(255)
) RETURNS VARCHAR(255)
DETERMINISTIC
BEGIN
  DECLARE cleaned_title VARCHAR(255);
  
  SET cleaned_title = input_title;
  
  -- Step 1: Replace underscores with spaces for processing
  SET cleaned_title = REPLACE(cleaned_title, '_', ' ');
  
  -- Step 2: Normalize whitespace
  SET cleaned_title = TRIM(cleaned_title);
  SET cleaned_title = REGEXP_REPLACE(cleaned_title, '\\s+', ' ');
  
  -- Step 3: Handle parenthetical content
  SET cleaned_title = REGEXP_REPLACE(cleaned_title, '\\s*\\([^)]*\\)\\s*', ' ');
  
  -- Step 4: Replace spaces back to underscores
  SET cleaned_title = REPLACE(cleaned_title, ' ', '_');
  
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
-- MAIN CONVERSION PROCEDURE
-- ========================================

DROP PROCEDURE IF EXISTS ConvertSASSPageClean;

DELIMITER //

CREATE PROCEDURE ConvertSASSPageClean()
BEGIN
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_total_processed INT DEFAULT 0;
  DECLARE v_orphans_repaired INT DEFAULT 0;
  DECLARE v_self_ref_repaired INT DEFAULT 0;
  DECLARE v_self_ref_unresolved INT DEFAULT 0;
  
  SET v_start_time = UNIX_TIMESTAMP();
  
  -- Clear target table
  TRUNCATE TABLE sass_page_clean;
  
  -- ========================================
  -- PHASE 1: BUILD sass_page_clean WITH DEDUPLICATION
  -- ========================================
  
  INSERT INTO clean_build_state (state_key, state_value, state_text) 
  VALUES ('build_phase', 1, 'Building sass_page_clean with deduplication')
  ON DUPLICATE KEY UPDATE state_value = 1, state_text = 'Building sass_page_clean with deduplication';
  
  -- Create temporary table for representative selection
  CREATE TEMPORARY TABLE temp_representatives AS
  SELECT 
    page_title,
    page_id as representative_page_id
  FROM (
    SELECT 
      clean_page_title_enhanced(page_title) as page_title,
      page_id,
      page_dag_level,
      page_is_leaf,
      ROW_NUMBER() OVER (
        PARTITION BY clean_page_title_enhanced(page_title)
        ORDER BY 
          CASE WHEN page_dag_level <= 2 THEN 0 ELSE 1 END ASC,  -- Prioritize levels 0-2
          page_dag_level DESC,                                   -- Then deepest
          page_is_leaf ASC,                                      -- Then categories
          page_id ASC                                            -- Then oldest
      ) as rn
    FROM sass_page
  ) ranked
  WHERE rn = 1;
  
  -- Build sass_page_clean with representatives only (GROUP BY to handle duplicates)
  INSERT INTO sass_page_clean (
    page_id,
    page_title,
    page_parent_id,
    page_root_id,
    page_dag_level,
    page_is_leaf
  )
  SELECT 
    tr.representative_page_id,
    tr.page_title,
    MIN(sp.page_parent_id) as page_parent_id,
    MIN(sp.page_root_id) as page_root_id,
    MAX(sp.page_dag_level) as page_dag_level,
    MIN(sp.page_is_leaf) as page_is_leaf
  FROM temp_representatives tr
  JOIN sass_page sp ON tr.representative_page_id = sp.page_id
  GROUP BY tr.representative_page_id, tr.page_title;
  
  SET v_total_processed = ROW_COUNT();
  
  DROP TEMPORARY TABLE temp_representatives;
  
  SELECT 
    'Phase 1 Complete: Deduplication' AS status,
    FORMAT(v_total_processed, 0) AS representatives_created,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 1) AS elapsed_sec;
  
  -- ========================================
  -- PHASE 2: AUTO-REPAIR ORPHAN REFERENCES
  -- ========================================
  
  INSERT INTO clean_build_state (state_key, state_value, state_text) 
  VALUES ('build_phase', 2, 'Auto-repairing orphan references')
  ON DUPLICATE KEY UPDATE state_value = 2, state_text = 'Auto-repairing orphan references';
  
  -- Find representative for each orphaned parent
  CREATE TEMPORARY TABLE temp_orphan_mappings AS
  SELECT DISTINCT
    c.page_id,
    rep.page_id as new_parent_id
  FROM sass_page_clean c
  LEFT JOIN sass_page_clean p ON c.page_parent_id = p.page_id
  JOIN sass_page sp_parent ON c.page_parent_id = sp_parent.page_id
  JOIN sass_page_clean rep ON clean_page_title_enhanced(sp_parent.page_title) = rep.page_title
  WHERE c.page_dag_level > 0
    AND c.page_dag_level <= 7
    AND p.page_id IS NULL
    AND c.page_id != rep.page_id;  -- Prevent self-references
  
  -- Update orphans to point to representative parents
  UPDATE sass_page_clean c
  JOIN temp_orphan_mappings t ON c.page_id = t.page_id
  SET c.page_parent_id = t.new_parent_id;
  
  SET v_orphans_repaired = ROW_COUNT();
  
  DROP TEMPORARY TABLE temp_orphan_mappings;
  
  -- Handle self-reference orphans with grandparent fallback
  CREATE TEMPORARY TABLE temp_self_ref_orphans AS
  SELECT 
    c.page_id,
    COALESCE(gp_clean.page_id, c.page_parent_id) as new_parent_id
  FROM sass_page_clean c
  LEFT JOIN sass_page_clean p ON c.page_parent_id = p.page_id
  LEFT JOIN sass_page sp_orig ON c.page_parent_id = sp_orig.page_id
  LEFT JOIN sass_page sp_grandparent ON sp_orig.page_parent_id = sp_grandparent.page_id
  LEFT JOIN sass_page_clean gp_clean ON sp_grandparent.page_id = gp_clean.page_id
  WHERE c.page_dag_level > 0
    AND c.page_dag_level <= 7
    AND p.page_id IS NULL;
  
  UPDATE sass_page_clean c
  JOIN temp_self_ref_orphans t ON c.page_id = t.page_id
  SET c.page_parent_id = t.new_parent_id
  WHERE t.new_parent_id != c.page_id;
  
  SET v_self_ref_repaired = ROW_COUNT();
  
  -- Count unresolved
  SELECT COUNT(*) INTO v_self_ref_unresolved
  FROM sass_page_clean c
  LEFT JOIN sass_page_clean p ON c.page_parent_id = p.page_id
  WHERE c.page_dag_level > 0 AND c.page_dag_level <= 7 AND p.page_id IS NULL;
  
  DROP TEMPORARY TABLE temp_self_ref_orphans;
  
  -- Remove any self-references
  UPDATE sass_page_clean c
  LEFT JOIN sass_page_clean p ON c.page_parent_id = p.page_id
  SET c.page_parent_id = 0
  WHERE c.page_id = c.page_parent_id AND c.page_dag_level > 0;
  
  SELECT 
    'Phase 2 Complete: Orphan Repair' AS status,
    FORMAT(v_orphans_repaired, 0) AS standard_orphans_fixed,
    FORMAT(v_self_ref_repaired, 0) AS self_ref_fixed,
    FORMAT(v_self_ref_unresolved, 0) AS unresolved_orphans,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 1) AS elapsed_sec;
  
  -- Update build state
  INSERT INTO clean_build_state (state_key, state_value, state_text) 
  VALUES ('build_status', 100, 'Conversion completed - ready for weak branch filtering')
  ON DUPLICATE KEY UPDATE state_value = 100;
  
  INSERT INTO clean_build_state (state_key, state_value) 
  VALUES ('total_representatives', v_total_processed)
  ON DUPLICATE KEY UPDATE state_value = v_total_processed;
  
  -- Final report
  SELECT 
    'COMPLETE - SASS Page Deduplication' AS final_status,
    FORMAT(v_total_processed, 0) AS representatives_created,
    FORMAT(v_orphans_repaired + v_self_ref_repaired, 0) AS total_orphans_repaired,
    FORMAT(v_self_ref_unresolved, 0) AS unresolved_orphans,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_time_sec;
  
  -- Verification
  SELECT 
    'Data Integrity Check' AS check_type,
    'Self-references' as issue,
    COUNT(*) as count
  FROM sass_page_clean 
  WHERE page_id = page_parent_id AND page_dag_level > 0
  
  UNION ALL
  
  SELECT 
    'Data Integrity Check',
    'Orphans',
    COUNT(*)
  FROM sass_page_clean c 
  LEFT JOIN sass_page_clean p ON c.page_parent_id = p.page_id
  WHERE c.page_dag_level > 0 AND c.page_dag_level <= 7 AND p.page_id IS NULL;

END//

DELIMITER ;

-- ========================================
-- WEAK BRANCH FILTERING PROCEDURE
-- ========================================

DROP PROCEDURE IF EXISTS FilterWeakBranches;

DELIMITER //

CREATE PROCEDURE FilterWeakBranches()
BEGIN
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_iteration INT DEFAULT 0;
  DECLARE v_changes INT DEFAULT 1;
  DECLARE v_total_filtered INT DEFAULT 0;
  DECLARE v_current_level INT;
  DECLARE v_filtered_this_pass INT;
  DECLARE v_reparented INT;
  
  SET v_start_time = UNIX_TIMESTAMP();
  
  INSERT INTO clean_build_state (state_key, state_value, state_text) 
  VALUES ('filter_phase', 1, 'Filtering weak branches')
  ON DUPLICATE KEY UPDATE state_value = 1, state_text = 'Filtering weak branches';
  
  -- Iterate until no more changes (max 3 iterations)
  WHILE v_changes > 0 AND v_iteration < 3 DO
    SET v_iteration = v_iteration + 1;
    SET v_changes = 0;
    SET v_filtered_this_pass = 0;
    
    SELECT CONCAT('Iteration ', v_iteration, ': Filtering weak categories') AS status;
    
    -- Process levels 7 down to 3 (protect 0-2)
    SET v_current_level = 7;
    
    WHILE v_current_level >= 3 DO
      
      -- Create temp table of weak categories at this level
      CREATE TEMPORARY TABLE IF NOT EXISTS temp_weak_categories (
        page_id INT UNSIGNED PRIMARY KEY,
        child_count INT
      ) ENGINE=MEMORY;
      
      TRUNCATE TABLE temp_weak_categories;
      
      -- Find categories with <9 children
      INSERT INTO temp_weak_categories (page_id, child_count)
      SELECT 
        p.page_id,
        COUNT(c.page_id) as child_count
      FROM sass_page_clean p
      LEFT JOIN sass_page_clean c ON p.page_id = c.page_parent_id
      WHERE p.page_dag_level = v_current_level
        AND p.page_is_leaf = 0
      GROUP BY p.page_id
      HAVING COUNT(c.page_id) < 9;
      
      -- Count affected categories
      SET @weak_count = (SELECT COUNT(*) FROM temp_weak_categories);
      
      IF @weak_count > 0 THEN
        
        -- Reparent children to grandparent
        UPDATE sass_page_clean c
        JOIN temp_weak_categories wc ON c.page_parent_id = wc.page_id
        JOIN sass_page_clean weak_parent ON wc.page_id = weak_parent.page_id
        SET c.page_parent_id = weak_parent.page_parent_id,
            c.page_dag_level = c.page_dag_level - 1;
        
        SET v_reparented = ROW_COUNT();
        
        -- Convert weak categories to leaves
        UPDATE sass_page_clean p
        JOIN temp_weak_categories wc ON p.page_id = wc.page_id
        SET p.page_is_leaf = 1;
        
        SET v_changes = v_changes + @weak_count;
        SET v_filtered_this_pass = v_filtered_this_pass + @weak_count;
        
        SELECT 
          CONCAT('  Level ', v_current_level) AS level_status,
          FORMAT(@weak_count, 0) AS categories_filtered,
          FORMAT(v_reparented, 0) AS children_reparented,
          ROUND(UNIX_TIMESTAMP() - v_start_time, 1) AS elapsed_sec;
      END IF;
      
      DROP TEMPORARY TABLE IF EXISTS temp_weak_categories;
      SET v_current_level = v_current_level - 1;
      
    END WHILE;
    
    SET v_total_filtered = v_total_filtered + v_filtered_this_pass;
    
    SELECT 
      CONCAT('Iteration ', v_iteration, ' Complete') AS iteration_status,
      FORMAT(v_filtered_this_pass, 0) AS categories_filtered_this_pass,
      FORMAT(v_total_filtered, 0) AS total_filtered_so_far,
      CASE WHEN v_changes = 0 THEN 'STABLE - No more changes' ELSE 'CONTINUING' END AS convergence_status,
      ROUND(UNIX_TIMESTAMP() - v_start_time, 1) AS elapsed_sec;
    
  END WHILE;
  
  -- Delete orphaned leaves (pages whose entire ancestor chain was filtered)
  DELETE FROM sass_page_clean
  WHERE page_is_leaf = 1
    AND page_dag_level > 0
    AND NOT EXISTS (
      SELECT 1 FROM sass_page_clean p 
      WHERE p.page_id = sass_page_clean.page_parent_id
    );
  
  SET @deleted_orphans = ROW_COUNT();
  
  -- Update build state
  INSERT INTO clean_build_state (state_key, state_value, state_text) 
  VALUES ('filter_status', 100, 'Weak branch filtering completed')
  ON DUPLICATE KEY UPDATE state_value = 100;
  
  INSERT INTO clean_build_state (state_key, state_value) 
  VALUES ('total_filtered', v_total_filtered)
  ON DUPLICATE KEY UPDATE state_value = v_total_filtered;
  
  INSERT INTO clean_build_state (state_key, state_value) 
  VALUES ('filter_iterations', v_iteration)
  ON DUPLICATE KEY UPDATE state_value = v_iteration;
  
  -- Final report
  SELECT 
    'COMPLETE - Weak Branch Filtering' AS final_status,
    FORMAT(v_total_filtered, 0) AS total_categories_filtered,
    FORMAT(@deleted_orphans, 0) AS orphaned_leaves_deleted,
    v_iteration AS iterations_completed,
    FORMAT((SELECT COUNT(*) FROM sass_page_clean), 0) AS final_representative_count,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_time_sec;
  
  -- Category size distribution after filtering
  SELECT 
    'Category Size After Filtering' AS distribution_type,
    CASE 
      WHEN child_count = 0 THEN '0 (leaves)'
      WHEN child_count < 9 THEN '1-8 (should not exist for levels 3+)'
      WHEN child_count < 20 THEN '9-19'
      WHEN child_count < 50 THEN '20-49'
      WHEN child_count < 100 THEN '50-99'
      ELSE '100+'
    END as size_range,
    COUNT(*) as category_count
  FROM (
    SELECT p.page_id, p.page_dag_level, COUNT(c.page_id) as child_count
    FROM sass_page_clean p
    LEFT JOIN sass_page_clean c ON p.page_id = c.page_parent_id
    WHERE p.page_is_leaf = 0
    GROUP BY p.page_id, p.page_dag_level
  ) counts
  GROUP BY size_range
  ORDER BY 
    CASE size_range
      WHEN '0 (leaves)' THEN 1
      WHEN '1-8 (should not exist for levels 3+)' THEN 2
      WHEN '9-19' THEN 3
      WHEN '20-49' THEN 4
      WHEN '50-99' THEN 5
      ELSE 6
    END;

END//

DELIMITER ;

-- ========================================
-- COMBINED PROCEDURE
-- ========================================

DROP PROCEDURE IF EXISTS BuildSASSPageCleanComplete;

DELIMITER //

CREATE PROCEDURE BuildSASSPageCleanComplete()
BEGIN
  DECLARE v_overall_start DECIMAL(14,3);
  
  SET v_overall_start = UNIX_TIMESTAMP();
  
  SELECT 'Starting Complete SASS Page Clean Build' AS status;
  
  -- Phase 1: Deduplication and orphan repair
  CALL ConvertSASSPageClean();
  
  -- Phase 2: Weak branch filtering
  CALL FilterWeakBranches();
  
  -- Overall summary
  SELECT 
    'BUILD COMPLETE - sass_page_clean Ready' AS final_status,
    FORMAT((SELECT COUNT(*) FROM sass_page_clean), 0) AS final_page_count,
    FORMAT((SELECT COUNT(*) FROM sass_page_clean WHERE page_is_leaf = 1), 0) AS articles,
    FORMAT((SELECT COUNT(*) FROM sass_page_clean WHERE page_is_leaf = 0), 0) AS categories,
    ROUND(UNIX_TIMESTAMP() - v_overall_start, 2) AS total_build_time_sec;
  
  -- Level distribution
  SELECT 
    'Final Level Distribution' AS distribution_type,
    page_dag_level AS level,
    FORMAT(COUNT(*), 0) AS page_count,
    CONCAT(ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM sass_page_clean), 1), '%') AS percentage
  FROM sass_page_clean
  GROUP BY page_dag_level
  ORDER BY page_dag_level;

END//

DELIMITER ;

-- ========================================
-- UTILITY PROCEDURES
-- ========================================

DROP PROCEDURE IF EXISTS ValidateSASSPageClean;

DELIMITER //

CREATE PROCEDURE ValidateSASSPageClean()
BEGIN
  SELECT 
    'Validation: Self-References' AS check_type,
    COUNT(*) as count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status
  FROM sass_page_clean 
  WHERE page_id = page_parent_id AND page_dag_level > 0
  
  UNION ALL
  
  SELECT 
    'Validation: Orphans (levels 1-7)',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END
  FROM sass_page_clean c 
  LEFT JOIN sass_page_clean p ON c.page_parent_id = p.page_id
  WHERE c.page_dag_level > 0 AND c.page_dag_level <= 7 AND p.page_id IS NULL
  
  UNION ALL
  
  SELECT 
    'Validation: Small Categories (levels 3-7)',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
  FROM (
    SELECT p.page_id, p.page_dag_level, COUNT(c.page_id) as child_count
    FROM sass_page_clean p
    LEFT JOIN sass_page_clean c ON p.page_id = c.page_parent_id
    WHERE p.page_is_leaf = 0 AND p.page_dag_level >= 3
    GROUP BY p.page_id, p.page_dag_level
    HAVING COUNT(c.page_id) < 9
  ) small_cats;

END//

DELIMITER ;

-- ========================================
-- USAGE EXAMPLES
-- ========================================

/*
-- Load and run complete build
source convert_sass_page_clean.sql;
CALL BuildSASSPageCleanComplete();

-- Or run phases separately
source convert_sass_page_clean.sql;
CALL ConvertSASSPageClean();
CALL FilterWeakBranches();

-- Validate results
CALL ValidateSASSPageClean();

-- Check build status
SELECT * FROM clean_build_state ORDER BY updated_at DESC;

-- Verify final counts
SELECT 
  COUNT(*) as total,
  COUNT(CASE WHEN page_is_leaf = 1 THEN 1 END) as articles,
  COUNT(CASE WHEN page_is_leaf = 0 THEN 1 END) as categories
FROM sass_page_clean;

-- Check deduplication effectiveness
SELECT 
  'Deduplication Summary' as metric,
  FORMAT((SELECT COUNT(*) FROM sass_page), 0) as original_rows,
  FORMAT((SELECT COUNT(DISTINCT page_id) FROM sass_page), 0) as unique_page_ids,
  FORMAT((SELECT COUNT(*) FROM sass_page_clean), 0) as representatives_created,
  CONCAT(ROUND(100.0 * (SELECT COUNT(*) FROM sass_page_clean) / 
    (SELECT COUNT(DISTINCT page_id) FROM sass_page), 1), '%') as retention_rate;

KEY FEATURES:
- Fixed duplicate key error with GROUP BY in Phase 1
- No sass_identity_pages table (direct deduplication)
- Processes levels 0-7 only
- Weak branch filtering with 9-child threshold
- Automatic orphan repair with grandparent fallback
- Self-reference prevention
- Iterative filtering (converges in 2-3 passes)
- Comprehensive validation

ESTIMATED RUNTIME:
- Deduplication + orphan repair: 15-20 minutes
- Weak branch filtering: 10-15 minutes
- Total: 25-35 minutes
*/
