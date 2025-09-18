-- GSSS Wikipedia Category Tree Builder - Phase 1 (Incremental)
-- Builds materialized DAG tree of GSSS categories and articles
-- Supports incremental builds with level ranges

-- ========================================
-- TABLE DEFINITIONS
-- ========================================

-- Main GSSS page table matching schemas.md specification
CREATE TABLE IF NOT EXISTS gsss_page (
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

-- Temporary working table for current build iteration
CREATE TABLE IF NOT EXISTS gsss_work (
  page_id INT UNSIGNED NOT NULL,
  parent_id INT UNSIGNED NOT NULL,
  root_id INT NOT NULL,
  level INT NOT NULL,
  
  PRIMARY KEY (page_id, parent_id),
  INDEX idx_level (level),
  INDEX idx_root (root_id),
  INDEX idx_parent (parent_id)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED;

-- Root category mapping
CREATE TABLE IF NOT EXISTS gsss_roots (
  root_id INT PRIMARY KEY,
  root_name VARCHAR(255) NOT NULL,
  page_id INT UNSIGNED NOT NULL,
  INDEX idx_name (root_name),
  INDEX idx_page_id (page_id)
) ENGINE=InnoDB;

-- Build state tracking table
CREATE TABLE IF NOT EXISTS build_state (
  state_key VARCHAR(255) PRIMARY KEY,
  state_value INT NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Cycle detection results table
CREATE TABLE IF NOT EXISTS gsss_cycles (
  page_id INT UNSIGNED NOT NULL,
  ancestor_id INT UNSIGNED NOT NULL,
  path_length INT NOT NULL,
  detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_page (page_id),
  INDEX idx_ancestor (ancestor_id),
  INDEX idx_path_length (path_length)
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
-- INITIALIZATION
-- ========================================

-- Initialize root category mapping
INSERT IGNORE INTO gsss_roots (root_id, root_name, page_id)
SELECT 1, 'Business', page_id FROM page WHERE page_namespace = 14 AND page_title = 'Business'
UNION ALL
SELECT 2, 'Science', page_id FROM page WHERE page_namespace = 14 AND page_title = 'Science'  
UNION ALL
SELECT 3, 'Technology', page_id FROM page WHERE page_namespace = 14 AND page_title = 'Technology'
UNION ALL
SELECT 4, 'Engineering', page_id FROM page WHERE page_namespace = 14 AND page_title = 'Engineering'
UNION ALL
SELECT 5, 'Mathematics', page_id FROM page WHERE page_namespace = 14 AND page_title = 'Mathematics';

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
  DECLARE v_interrupted_level INT DEFAULT -1;
  
  -- Check what levels already exist
  SELECT COALESCE(MAX(page_dag_level), -1)
  INTO v_max_existing_level
  FROM gsss_page;
  
  -- Check for interrupted build
  SELECT COALESCE(state_value, -1) INTO v_interrupted_level
  FROM build_state 
  WHERE state_key = 'last_attempted_level';
  
  SET v_has_existing_data = (v_max_existing_level >= 0);
  
  IF v_has_existing_data THEN
    -- For incremental/restart builds
    SET v_actual_begin = GREATEST(p_begin_level, 0);
    SET v_actual_end = p_end_level;
    
    -- If resuming from interruption, start from interrupted level
    IF v_interrupted_level > v_max_existing_level THEN
      SET v_actual_begin = v_interrupted_level;
    ELSEIF p_begin_level <= v_max_existing_level THEN
      SET v_actual_begin = v_max_existing_level + 1;
    END IF;
  ELSE
    -- Fresh build
    SET v_actual_begin = 0;
    SET v_actual_end = p_end_level;
  END IF;
END$$
DELIMITER ;

-- Cycle detection procedure
DROP PROCEDURE IF EXISTS DetectCycles;
DELIMITER $$
CREATE PROCEDURE DetectCycles(IN p_max_depth INT DEFAULT 10)
BEGIN
  TRUNCATE TABLE gsss_cycles;
  
  -- Find potential cycles using recursive path checking
  INSERT INTO gsss_cycles (page_id, ancestor_id, path_length)
  WITH RECURSIVE cycle_check (page_id, ancestor_id, path_length) AS (
    -- Base case: direct parent relationships
    SELECT page_id, page_parent_id, 1
    FROM gsss_page 
    WHERE page_parent_id > 0
    
    UNION ALL
    
    -- Recursive case: follow parent chain
    SELECT cc.page_id, bp.page_parent_id, cc.path_length + 1
    FROM cycle_check cc
    JOIN gsss_page bp ON cc.ancestor_id = bp.page_id
    WHERE bp.page_parent_id > 0 
      AND cc.path_length < p_max_depth
      AND bp.page_parent_id != cc.page_id  -- Detect immediate cycle
  )
  SELECT page_id, ancestor_id, path_length
  FROM cycle_check
  WHERE page_id = ancestor_id  -- Cycle detected
    AND path_length > 1;
  
  -- Report results
  SELECT 
    COUNT(*) AS cycles_detected,
    AVG(path_length) AS avg_cycle_length
  FROM gsss_cycles;
END$$
DELIMITER ;

-- ========================================
-- MAIN BUILD PROCEDURE
-- ========================================

DROP PROCEDURE IF EXISTS BuildGSSSPageTree;

DELIMITER $$
CREATE PROCEDURE BuildGSSSPageTree(
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
    -- Save interrupted level for restart
    INSERT INTO build_state (state_key, state_value) 
    VALUES ('last_attempted_level', v_current_level)
    ON DUPLICATE KEY UPDATE state_value = v_current_level;
    
    ROLLBACK;
    RESIGNAL;
  END;

  SET v_start_time = UNIX_TIMESTAMP(3);
  
  -- Determine actual level range
  CALL GetBuildLevelRange(p_begin_level, p_end_level, v_actual_begin, v_actual_end, v_has_existing_data);
  
  -- Clear working table and update build state
  TRUNCATE TABLE gsss_work;
  
  -- Track build attempt
  INSERT INTO build_state (state_key, state_value) 
  VALUES ('last_attempted_level', v_actual_begin)
  ON DUPLICATE KEY UPDATE state_value = v_actual_begin;
  
  -- ========================================
  -- STEP 1: Initialize seed data
  -- ========================================
  
  START TRANSACTION;
  SET v_level_start_time = UNIX_TIMESTAMP(3);
  
  IF v_actual_begin = 0 AND NOT v_has_existing_data THEN
    -- Fresh build: start with root categories
    INSERT INTO gsss_work (page_id, parent_id, root_id, level)
    SELECT 
      br.page_id,
      0,  -- Root categories have no parent
      br.root_id,
      0
    FROM gsss_roots br;
    
    SET v_parent_pages = ROW_COUNT();
    
    IF v_parent_pages = 0 THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'No root GSSS categories found';
    END IF;
    
  ELSE
    -- Incremental build: use existing data as parents
    INSERT INTO gsss_work (page_id, parent_id, root_id, level)
    SELECT 
      bp.page_id,
      bp.page_parent_id,
      bp.page_root_id,
      bp.page_dag_level
    FROM gsss_page bp
    WHERE bp.page_dag_level = v_actual_begin - 1;
    
    SET v_parent_pages = ROW_COUNT();
    
    IF v_parent_pages = 0 THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'No parent pages found for incremental build';
    END IF;
  END IF;
  
  COMMIT;
  
  -- ========================================
  -- STEP 2: Level-by-level traversal
  -- ========================================
  
  SET v_current_level = v_actual_begin;
  
  WHILE v_current_level <= v_actual_end AND v_continue DO
    SET v_level_start_time = UNIX_TIMESTAMP(3);
    
    -- Track current level attempt
    INSERT INTO build_state (state_key, state_value) 
    VALUES ('last_attempted_level', v_current_level)
    ON DUPLICATE KEY UPDATE state_value = v_current_level;
    
    START TRANSACTION;
    
    -- Create temporary table for this level's results
    CREATE TEMPORARY TABLE level_candidates (
      page_id INT UNSIGNED NOT NULL,
      parent_id INT UNSIGNED NOT NULL,
      root_id INT NOT NULL,
      PRIMARY KEY (page_id, parent_id, root_id)
    ) ENGINE=MEMORY;
    
    -- Find children of current level parents, explicitly exclude files
    INSERT INTO level_candidates (page_id, parent_id, root_id)
    SELECT DISTINCT
      cl.cl_from,
      w.page_id,
      w.root_id
    FROM gsss_work w
    INNER JOIN categorylinks cl ON w.page_id = cl.cl_target_id
    INNER JOIN page p ON cl.cl_from = p.page_id
    WHERE w.level = (CASE WHEN v_current_level = 0 THEN 0 ELSE v_current_level - 1 END)
      AND cl.cl_type IN ('page', 'subcat')  -- Include pages and subcategories
      AND cl.cl_type != 'file'              -- Explicitly exclude files
      AND p.page_namespace IN (0, 14)       -- Articles and categories only
      -- Exclude pages already in gsss_page
      AND NOT EXISTS (
        SELECT 1 FROM gsss_page bp WHERE bp.page_id = cl.cl_from
      );
    
    -- Add new candidates to work table
    INSERT IGNORE INTO gsss_work (page_id, parent_id, root_id, level)
    SELECT page_id, parent_id, root_id, v_current_level
    FROM level_candidates;
    
    SET v_rows_added = ROW_COUNT();
    
    -- Process candidates and add to final table (handle multiple parents by selecting first)
    INSERT IGNORE INTO gsss_page (
      page_id, page_title, page_parent_id, page_root_id, page_dag_level, page_is_leaf
    )
    SELECT 
      p.page_id,
      p.page_title,
      MIN(lc.parent_id) AS page_parent_id,  -- Select first parent for DAG simplification
      MIN(lc.root_id) AS page_root_id,      -- Select first root for primary classification
      v_current_level AS page_dag_level,
      (p.page_namespace = 0) AS page_is_leaf
    FROM level_candidates lc
    INNER JOIN page p ON lc.page_id = p.page_id
    GROUP BY p.page_id
    LIMIT p_batch_size;
    
    SET v_total_new_pages = v_total_new_pages + ROW_COUNT();
    
    DROP TEMPORARY TABLE level_candidates;
    
    COMMIT;
    
    -- Check continuation condition
    IF v_rows_added = 0 THEN
      SET v_continue = FALSE;
    ELSE
      SET v_current_level = v_current_level + 1;
    END IF;
    
  END WHILE;
  
  -- Update completed level
  INSERT INTO build_state (state_key, state_value) 
  VALUES ('last_completed_level', v_current_level - 1)
  ON DUPLICATE KEY UPDATE state_value = v_current_level - 1;
  
  -- Report results
  SELECT 
    'Incremental Build Complete' AS status,
    p_begin_level AS requested_begin_level,
    p_end_level AS requested_end_level,
    v_actual_begin AS actual_begin_level,
    v_current_level - 1 AS actual_end_level,
    v_total_new_pages AS new_pages_added,
    (SELECT COUNT(*) FROM gsss_page) AS total_pages_now,
    ROUND(UNIX_TIMESTAMP(3) - v_start_time, 2) AS total_execution_time_sec;

END$$
DELIMITER ;

-- ========================================
-- UTILITY PROCEDURES
-- ========================================

-- Resume build from last completed level
DROP PROCEDURE IF EXISTS ResumeBuild;
DELIMITER $$
CREATE PROCEDURE ResumeBuild(IN p_target_level INT DEFAULT 12)
BEGIN
  DECLARE v_last_level INT DEFAULT -1;
  
  SELECT COALESCE(state_value, -1) INTO v_last_level
  FROM build_state 
  WHERE state_key = 'last_completed_level';
  
  CALL BuildGSSSPageTree(v_last_level + 1, p_target_level);
END$$
DELIMITER ;

-- ========================================
-- EXAMPLE USAGE
-- ========================================

/*
-- Initial build (levels 0-5):
CALL BuildGSSSPageTree(0, 5);

-- Continue building (levels 6-10):  
CALL BuildGSSSPageTree(6, 10);

-- Resume from last completed level:
CALL ResumeBuild(12);

-- Check for cycles:
CALL DetectCycles(15);
SELECT * FROM gsss_cycles;
*/
