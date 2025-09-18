-- BSTEM Wikipedia Category Tree Builder - Phase 1 (Incremental)
-- Builds materialized DAG tree of BSTEM categories and articles
-- Supports incremental builds with level ranges

-- ========================================
-- TABLE DEFINITIONS
-- ========================================

-- Main BSTEM page table matching schemas.md specification
CREATE TABLE IF NOT EXISTS bstem_page (
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
CREATE TABLE IF NOT EXISTS bstem_work (
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
CREATE TABLE IF NOT EXISTS bstem_roots (
  root_id INT PRIMARY KEY,
  root_name VARCHAR(255) NOT NULL,
  page_id INT UNSIGNED NOT NULL,
  INDEX idx_name (root_name),
  INDEX idx_page_id (page_id)
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
INSERT IGNORE INTO bstem_roots (root_id, root_name, page_id)
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
  DECLARE v_min_existing_level INT DEFAULT 999;
  
  -- Check what levels already exist
  SELECT COALESCE(MIN(page_dag_level), 999), COALESCE(MAX(page_dag_level), -1)
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
    ROLLBACK;
    RESIGNAL;
  END;

  SET v_start_time = UNIX_TIMESTAMP(3);
  
  -- Determine actual level range
  CALL GetBuildLevelRange(p_begin_level, p_end_level, v_actual_begin, v_actual_end, v_has_existing_data);
  
  -- Clear working table
  TRUNCATE TABLE bstem_work;
  
  -- ========================================
  -- STEP 1: Initialize seed data
  -- ========================================
  
  START TRANSACTION;
  SET v_level_start_time = UNIX_TIMESTAMP(3);
  
  IF v_actual_begin = 0 AND NOT v_has_existing_data THEN
    -- Fresh build: start with root categories
    INSERT INTO bstem_work (page_id, parent_id, root_id, level)
    SELECT 
      br.page_id,
      0,  -- Root categories have no parent
      br.root_id,
      0
    FROM bstem_roots br;
    
    SET v_parent_pages = ROW_COUNT();
    
    IF v_parent_pages = 0 THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'No root BSTEM categories found';
    END IF;
    
  ELSE
    -- Incremental build: use existing data as parents
    INSERT INTO bstem_work (page_id, parent_id, root_id, level)
    SELECT 
      bp.page_id,
      bp.page_parent_id,
      bp.page_root_id,
      bp.page_dag_level
    FROM bstem_page bp
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
    
    START TRANSACTION;
    
    -- Create temporary table for this level's results
    CREATE TEMPORARY TABLE level_candidates (
      page_id INT UNSIGNED NOT NULL,
      parent_id INT UNSIGNED NOT NULL,
      root_id INT NOT NULL,
      PRIMARY KEY (page_id, parent_id, root_id)
    ) ENGINE=MEMORY;
    
    -- Find children of current level parents, batch by batch
    INSERT INTO level_candidates (page_id, parent_id, root_id)
    SELECT DISTINCT
      cl.cl_from,
      w.page_id,
      w.root_id
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
    INSERT IGNORE INTO bstem_work (page_id, parent_id, root_id, level)
    SELECT page_id, parent_id, root_id, v_current_level
    FROM level_candidates;
    
    SET v_rows_added = ROW_COUNT();
    
    -- Process candidates and add to final table (handle multiple parents by selecting first)
    INSERT IGNORE INTO bstem_page (
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

END$
DELIMITER ;

-- ========================================
-- EXAMPLE USAGE
-- ========================================

/*
-- Initial build (levels 0-5):
CALL BuildBSTEMPageTree(0, 5);

-- Continue building (levels 6-10):  
CALL BuildBSTEMPageTree(6, 10);
*/
