-- BSTEM Wikipedia Category Tree Builder - Phase 1 (Incremental)
-- Builds materialized DAG tree of BSTEM categories and articles
-- Supports incremental builds with level ranges

-- ========================================
-- TABLE DEFINITIONS
-- ========================================

-- Main BSTEM page table with Wikipedia metadata + DAG fields
CREATE TABLE IF NOT EXISTS bstem_page (
  page_id INT UNSIGNED NOT NULL,
  page_namespace INT NOT NULL,
  page_title VARCHAR(255) NOT NULL,
  page_is_redirect TINYINT UNSIGNED NOT NULL DEFAULT 0,
  page_is_new TINYINT UNSIGNED NOT NULL DEFAULT 0,
  page_random DOUBLE UNSIGNED NOT NULL DEFAULT 0,
  page_touched BINARY(14) NOT NULL,
  page_links_updated BINARY(14) NULL,
  page_latest INT UNSIGNED NOT NULL DEFAULT 0,
  page_len INT UNSIGNED NOT NULL DEFAULT 0,
  page_content_model VARCHAR(32) NULL,
  page_lang VARCHAR(35) NULL,
  min_level INT NOT NULL,
  root_categories TEXT NOT NULL,
  is_leaf BOOLEAN NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  PRIMARY KEY (page_id),
  INDEX idx_namespace (page_namespace),
  INDEX idx_title (page_title),
  INDEX idx_redirect (page_is_redirect),
  INDEX idx_min_level (min_level),
  INDEX idx_leaf (is_leaf),
  INDEX idx_root_categories (root_categories(255)),
  INDEX idx_ns_level (page_namespace, min_level)
) ENGINE=InnoDB;

-- Temporary working table for current build iteration
CREATE TABLE IF NOT EXISTS bstem_work (
  page_id INT UNSIGNED NOT NULL,
  root_category VARCHAR(255) NOT NULL,
  level INT NOT NULL,
  
  PRIMARY KEY (page_id, root_category),
  INDEX idx_level (level),
  INDEX idx_page_id (page_id)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED;

-- Progress tracking table
CREATE TABLE IF NOT EXISTS build_progress (
  iteration INT AUTO_INCREMENT PRIMARY KEY,
  begin_level INT,
  end_level INT,
  level INT,
  root_category VARCHAR(255),
  pages_added INT,
  execution_time_sec DECIMAL(10,3),
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_timestamp (timestamp),
  INDEX idx_levels (begin_level, end_level)
) ENGINE=InnoDB;

-- Build state tracking
CREATE TABLE IF NOT EXISTS build_state (
  state_key VARCHAR(50) PRIMARY KEY,
  state_value TEXT,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Error logging table
CREATE TABLE IF NOT EXISTS build_errors (
  error_id INT AUTO_INCREMENT PRIMARY KEY,
  begin_level INT,
  end_level INT,
  level INT,
  error_message TEXT,
  sql_state VARCHAR(5),
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- ========================================
-- PERFORMANCE INDEXES ON SOURCE TABLES
-- ========================================

-- Critical indexes for fast traversal
CREATE INDEX IF NOT EXISTS idx_cl_target_type ON categorylinks (cl_target_id, cl_type);
CREATE INDEX IF NOT EXISTS idx_cl_from ON categorylinks (cl_from);
CREATE INDEX IF NOT EXISTS idx_cl_type_target ON categorylinks (cl_type, cl_target_id);

-- Page table optimizations
CREATE INDEX IF NOT EXISTS idx_page_ns_title ON page (page_namespace, page_title);
CREATE INDEX IF NOT EXISTS idx_page_id_ns ON page (page_id, page_namespace);

-- ========================================
-- HELPER PROCEDURES
-- ========================================

DROP PROCEDURE IF EXISTS GetBuildLevelRange;
DELIMITER $$
CREATE PROCEDURE GetBuildLevelRange(
  IN p_begin_level INT,
  IN p_end_level INT,
  OUT v_actual_begin INT,
  OUT v_actual_end INT,
  OUT v_has_existing_data BOOLEAN
)
BEGIN
  DECLARE v_max_existing_level INT DEFAULT -1;
  DECLARE v_min_existing_level INT DEFAULT 999;
  
  -- Check what levels already exist
  SELECT COALESCE(MIN(min_level), 999), COALESCE(MAX(min_level), -1)
  INTO v_min_existing_level, v_max_existing_level
  FROM bstem_page;
  
  SET v_has_existing_data = (v_max_existing_level >= 0);
  
  IF v_has_existing_data THEN
    -- For incremental builds, validate range
    SET v_actual_begin = GREATEST(p_begin_level, 0);
    SET v_actual_end = p_end_level;
    
    -- If begin_level is within existing data, start from next level
    IF p_begin_level <= v_max_existing_level THEN
      SET v_actual_begin = v_max_existing_level + 1;
    END IF;
  ELSE
    -- Fresh build - start from level 0
    SET v_actual_begin = 0;
    SET v_actual_end = p_end_level;
  END IF;
END$$
DELIMITER ;

-- ========================================
-- MAIN BUILD PROCEDURE
-- ========================================

DROP PROCEDURE IF EXISTS BuildBSTEMPageTree;

DELIMITER $$
CREATE PROCEDURE BuildBSTEMPageTree(
  IN p_begin_level INT DEFAULT 0,
  IN p_end_level INT DEFAULT 12,
  IN p_batch_size INT DEFAULT 50000
)
BEGIN
  DECLARE v_current_level INT;
  DECLARE v_actual_begin INT;
  DECLARE v_actual_end INT;
  DECLARE v_has_existing_data BOOLEAN DEFAULT FALSE;
  DECLARE v_rows_added INT DEFAULT 0;
  DECLARE v_total_new_pages INT DEFAULT 0;
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_level_start_time DECIMAL(14,3);
  DECLARE v_continue BOOLEAN DEFAULT TRUE;
  DECLARE v_parent_pages INT DEFAULT 0;
  
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1
      @error_message = MESSAGE_TEXT,
      @sql_state = RETURNED_SQLSTATE;
    
    INSERT INTO build_errors (begin_level, end_level, level, error_message, sql_state)
    VALUES (p_begin_level, p_end_level, v_current_level, @error_message, @sql_state);
    
    ROLLBACK;
    RESIGNAL;
  END;

  SET v_start_time = UNIX_TIMESTAMP(3);
  
  -- Determine actual level range
  CALL GetBuildLevelRange(p_begin_level, p_end_level, v_actual_begin, v_actual_end, v_has_existing_data);
  
  -- Clear working table
  TRUNCATE TABLE bstem_work;
  
  -- Log build start
  INSERT INTO build_progress (begin_level, end_level, level, root_category, pages_added, execution_time_sec)
  VALUES (p_begin_level, p_end_level, -1, 'BUILD_START', 0, 0);
  
  -- ========================================
  -- STEP 1: Initialize seed data
  -- ========================================
  
  START TRANSACTION;
  SET v_level_start_time = UNIX_TIMESTAMP(3);
  
  IF v_actual_begin = 0 AND NOT v_has_existing_data THEN
    -- Fresh build: start with root categories
    INSERT INTO bstem_work (page_id, root_category, level)
    SELECT 
      p.page_id,
      p.page_title,
      0
    FROM page p
    WHERE p.page_namespace = 14
      AND p.page_title IN ('Business', 'Science', 'Technology', 'Engineering', 'Mathematics');
    
    SET v_parent_pages = ROW_COUNT();
    
    IF v_parent_pages = 0 THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'No root BSTEM categories found';
    END IF;
    
  ELSE
    -- Incremental build: use existing data as parents
    INSERT INTO bstem_work (page_id, root_category, level)
    SELECT 
      bp.page_id,
      SUBSTRING_INDEX(bp.root_categories, ',', 1) AS primary_root,
      bp.min_level
    FROM bstem_page bp
    WHERE bp.min_level = v_actual_begin - 1;
    
    SET v_parent_pages = ROW_COUNT();
    
    IF v_parent_pages = 0 THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'No parent pages found for incremental build';
    END IF;
  END IF;
  
  COMMIT;
  
  -- Log initialization
  INSERT INTO build_progress (begin_level, end_level, level, root_category, pages_added, execution_time_sec)
  VALUES (p_begin_level, p_end_level, v_actual_begin, 'INITIALIZATION', v_parent_pages, 
          UNIX_TIMESTAMP(3) - v_level_start_time);
  
  -- ========================================
  -- STEP 2: Level-by-level traversal
  -- ========================================
  
  SET v_current_level = v_actual_begin;
  
  WHILE v_current_level <= v_actual_end AND v_continue DO
    SET v_level_start_time = UNIX_TIMESTAMP(3);
    
    START TRANSACTION;
    
    -- Create temporary table for this level's results
    CREATE TEMPORARY TABLE level_candidates (
      page_id INT UNSIGNED NOT NULL,
      root_category VARCHAR(255) NOT NULL,
      PRIMARY KEY (page_id, root_category)
    ) ENGINE=MEMORY;
    
    -- Find children of current level parents, batch by batch
    INSERT INTO level_candidates (page_id, root_category)
    SELECT DISTINCT
      cl.cl_from,
      w.root_category
    FROM bstem_work w
    INNER JOIN categorylinks cl ON w.page_id = cl.cl_target_id
    INNER JOIN page p ON cl.cl_from = p.page_id
    WHERE w.level = (CASE WHEN v_current_level = 0 THEN 0 ELSE v_current_level - 1 END)
      AND cl.cl_type IN ('page', 'subcat')
      AND p.page_namespace IN (0, 14)
      -- Exclude pages already in bstem_page
      AND NOT EXISTS (
        SELECT 1 FROM bstem_page bp WHERE bp.page_id = cl.cl_from
      );
    
    -- Add new candidates to work table
    INSERT IGNORE INTO bstem_work (page_id, root_category, level)
    SELECT page_id, root_category, v_current_level
    FROM level_candidates;
    
    SET v_rows_added = ROW_COUNT();
    
    -- Process candidates in batches and add to final table
    INSERT IGNORE INTO bstem_page (
      page_id, page_namespace, page_title, page_is_redirect, page_is_new,
      page_random, page_touched, page_links_updated, page_latest, page_len,
      page_content_model, page_lang, min_level, root_categories, is_leaf
    )
    SELECT 
      p.page_id,
      p.page_namespace,
      p.page_title,
      p.page_is_redirect,
      p.page_is_new,
      p.page_random,
      p.page_touched,
      p.page_links_updated,
      p.page_latest,
      p.page_len,
      p.page_content_model,
      p.page_lang,
      v_current_level AS min_level,
      GROUP_CONCAT(DISTINCT lc.root_category ORDER BY lc.root_category SEPARATOR ',') AS root_categories,
      (p.page_namespace = 0) AS is_leaf
    FROM level_candidates lc
    INNER JOIN page p ON lc.page_id = p.page_id
    GROUP BY p.page_id
    LIMIT p_batch_size;
    
    SET v_total_new_pages = v_total_new_pages + ROW_COUNT();
    
    DROP TEMPORARY TABLE level_candidates;
    
    -- Log level progress
    INSERT INTO build_progress (begin_level, end_level, level, root_category, pages_added, execution_time_sec)
    VALUES (p_begin_level, p_end_level, v_current_level, 'ALL', v_rows_added, 
            UNIX_TIMESTAMP(3) - v_level_start_time);
    
    COMMIT;
    
    -- Check continuation condition
    IF v_rows_added = 0 THEN
      SET v_continue = FALSE;
    ELSE
      SET v_current_level = v_current_level + 1;
    END IF;
    
  END WHILE;
  
  -- ========================================
  -- STEP 3: Update build state
  -- ========================================
  
  INSERT INTO build_state (state_key, state_value) 
  VALUES ('last_completed_level', v_current_level - 1)
  ON DUPLICATE KEY UPDATE state_value = v_current_level - 1;
  
  INSERT INTO build_state (state_key, state_value)
  VALUES ('total_pages', (SELECT COUNT(*) FROM bstem_page))
  ON DUPLICATE KEY UPDATE state_value = (SELECT COUNT(*) FROM bstem_page);
  
  -- Final summary
  INSERT INTO build_progress (begin_level, end_level, level, root_category, pages_added, execution_time_sec)
  VALUES (p_begin_level, p_end_level, -2, 'BUILD_COMPLETE', v_total_new_pages, 
          UNIX_TIMESTAMP(3) - v_start_time);
  
  -- Report results
  SELECT 
    'Incremental Build Complete' AS status,
    p_begin_level AS requested_begin_level,
    p_end_level AS requested_end_level,
    v_actual_begin AS actual_begin_level,
    v_current_level - 1 AS actual_end_level,
    v_total_new_pages AS new_pages_added,
    (SELECT COUNT(*) FROM bstem_page) AS total_pages_now,
    ROUND(UNIX_TIMESTAMP(3) - v_start_time, 2) AS total_execution_time_sec;

END$$
DELIMITER ;

-- ========================================
-- MONITORING VIEWS
-- ========================================

-- Current build state
CREATE OR REPLACE VIEW build_status AS
SELECT 
  bs1.state_value AS last_completed_level,
  bs2.state_value AS total_pages,
  (SELECT MAX(min_level) FROM bstem_page) AS current_max_level,
  (SELECT MIN(min_level) FROM bstem_page) AS current_min_level,
  (SELECT COUNT(DISTINCT SUBSTRING_INDEX(root_categories, ',', 1)) FROM bstem_page) AS root_domains
FROM build_state bs1
LEFT JOIN build_state bs2 ON bs2.state_key = 'total_pages'
WHERE bs1.state_key = 'last_completed_level';

-- Summary by root category  
CREATE OR REPLACE VIEW bstem_summary AS
SELECT 
  SUBSTRING_INDEX(root_categories, ',', 1) AS primary_root,
  COUNT(*) AS total_pages,
  SUM(CASE WHEN is_leaf = TRUE THEN 1 ELSE 0 END) AS articles,
  SUM(CASE WHEN is_leaf = FALSE THEN 1 ELSE 0 END) AS categories,
  MIN(min_level) AS min_depth,
  MAX(min_level) AS max_depth,
  ROUND(AVG(page_len), 0) AS avg_page_length
FROM bstem_page
GROUP BY SUBSTRING_INDEX(root_categories, ',', 1)
ORDER BY total_pages DESC;

-- Level distribution
CREATE OR REPLACE VIEW bstem_level_distribution AS
SELECT 
  min_level AS level,
  COUNT(*) AS page_count,
  SUM(CASE WHEN is_leaf = TRUE THEN 1 ELSE 0 END) AS articles,
  SUM(CASE WHEN is_leaf = FALSE THEN 1 ELSE 0 END) AS categories,
  ROUND(AVG(page_len), 0) AS avg_page_length
FROM bstem_page
GROUP BY min_level
ORDER BY min_level;

-- Recent build performance
CREATE OR REPLACE VIEW build_performance AS
SELECT 
  begin_level,
  end_level,
  level,
  pages_added,
  execution_time_sec,
  CASE 
    WHEN execution_time_sec > 0 THEN ROUND(pages_added / execution_time_sec, 0)
    ELSE NULL 
  END AS pages_per_sec,
  timestamp
FROM build_progress
WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
ORDER BY timestamp DESC;

-- ========================================
-- UTILITY PROCEDURES
-- ========================================

-- Quick status check
DROP PROCEDURE IF EXISTS CheckBuildStatus;
DELIMITER $$
CREATE PROCEDURE CheckBuildStatus()
BEGIN
  SELECT * FROM build_status;
  SELECT 'Recent Performance' AS section;
  SELECT * FROM build_performance LIMIT 10;
  SELECT 'Level Distribution' AS section;
  SELECT * FROM bstem_level_distribution;
END$$
DELIMITER ;

-- Resume build from last completed level
DROP PROCEDURE IF EXISTS ResumeBuild;
DELIMITER $$
CREATE PROCEDURE ResumeBuild(IN p_target_level INT DEFAULT 12)
BEGIN
  DECLARE v_last_level INT DEFAULT -1;
  
  SELECT COALESCE(state_value, -1) INTO v_last_level
  FROM build_state 
  WHERE state_key = 'last_completed_level';
  
  CALL BuildBSTEMPageTree(v_last_level + 1, p_target_level);
END$$
DELIMITER ;

-- ========================================
-- EXAMPLE USAGE
-- ========================================

/*
-- Initial build (levels 0-5):
CALL BuildBSTEMPageTree(0, 5);

-- Continue building (levels 6-10):  
CALL BuildBSTEMPageTree(6, 10);

-- Resume from last completed level:
CALL ResumeBuild(12);

-- Check current status:
CALL CheckBuildStatus();

-- View summaries:
SELECT * FROM bstem_summary;
SELECT * FROM bstem_level_distribution;
*/
