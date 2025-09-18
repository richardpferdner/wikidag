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

-- Critical indexes for fast traversal (skip if already exist)
-- These may already exist from previous runs

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
-- SIMPLIFIED BUILD PROCEDURE
-- ========================================

DROP PROCEDURE IF EXISTS BuildGSSSPageTree;

DELIMITER //

CREATE PROCEDURE BuildGSSSPageTree(
  IN p_begin_level INT,
  IN p_end_level INT,
  IN p_batch_size INT
)
BEGIN
  DECLARE v_current_level INT;
  DECLARE v_rows_added INT DEFAULT 0;
  DECLARE v_total_new_pages INT DEFAULT 0;
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_continue TINYINT(1) DEFAULT 1;
  
  -- Set defaults
  IF p_begin_level IS NULL THEN SET p_begin_level = 0; END IF;
  IF p_end_level IS NULL THEN SET p_end_level = 12; END IF;
  IF p_batch_size IS NULL THEN SET p_batch_size = 50000; END IF;

  SET v_start_time = UNIX_TIMESTAMP(3);
  SET v_current_level = p_begin_level;
  
  -- Clear working table
  TRUNCATE TABLE gsss_work;
  
  -- Initialize with root categories for level 0
  IF p_begin_level = 0 THEN
    INSERT INTO gsss_work (page_id, parent_id, root_id, level)
    SELECT page_id, 0, root_id, 0 FROM gsss_roots;
    
    -- Add root categories to gsss_page
    INSERT IGNORE INTO gsss_page (page_id, page_title, page_parent_id, page_root_id, page_dag_level, page_is_leaf)
    SELECT p.page_id, p.page_title, 0, gr.root_id, 0, 0
    FROM gsss_roots gr
    JOIN page p ON gr.page_id = p.page_id;
    
    SET v_total_new_pages = ROW_COUNT();
    SET v_current_level = 1;
  END IF;
  
  -- Level-by-level traversal
  WHILE v_current_level <= p_end_level AND v_continue = 1 DO
    
    -- Track current level
    INSERT INTO build_state (state_key, state_value) 
    VALUES ('last_attempted_level', v_current_level)
    ON DUPLICATE KEY UPDATE state_value = v_current_level;
    
    -- Find children of current level parents
    INSERT INTO gsss_work (page_id, parent_id, root_id, level)
    SELECT DISTINCT
      cl.cl_from,
      w.page_id,
      w.root_id,
      v_current_level
    FROM gsss_work w
    JOIN categorylinks cl ON w.page_id = cl.cl_target_id
    JOIN page p ON cl.cl_from = p.page_id
    WHERE w.level = v_current_level - 1
      AND cl.cl_type IN ('page', 'subcat')
      AND p.page_namespace IN (0, 14)
      AND NOT EXISTS (SELECT 1 FROM gsss_page bp WHERE bp.page_id = cl.cl_from);
    
    SET v_rows_added = ROW_COUNT();
    
    -- Add new pages to gsss_page (handle multiple parents by selecting first)
    INSERT IGNORE INTO gsss_page (page_id, page_title, page_parent_id, page_root_id, page_dag_level, page_is_leaf)
    SELECT 
      p.page_id,
      p.page_title,
      MIN(w.parent_id),
      MIN(w.root_id),
      v_current_level,
      CASE WHEN p.page_namespace = 0 THEN 1 ELSE 0 END
    FROM gsss_work w
    JOIN page p ON w.page_id = p.page_id
    WHERE w.level = v_current_level
    GROUP BY p.page_id
    LIMIT p_batch_size;
    
    SET v_total_new_pages = v_total_new_pages + ROW_COUNT();
    
    -- Check if we should continue
    IF v_rows_added = 0 THEN
      SET v_continue = 0;
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
    'Build Complete' AS status,
    p_begin_level AS requested_begin_level,
    p_end_level AS requested_end_level,
    v_current_level - 1 AS actual_end_level,
    v_total_new_pages AS new_pages_added,
    (SELECT COUNT(*) FROM gsss_page) AS total_pages_now,
    ROUND(UNIX_TIMESTAMP(3) - v_start_time, 2) AS total_execution_time_sec;

END//

DELIMITER ;

-- ========================================
-- EXAMPLE USAGE
-- ========================================

/*
-- Initial build (levels 0-5):
CALL BuildGSSSPageTree(0, 5, NULL);

-- Continue building (levels 6-10):  
CALL BuildGSSSPageTree(6, 10, NULL);

-- Check current status:
SELECT * FROM build_state;
SELECT COUNT(*) AS total_pages, MAX(page_dag_level) AS max_level FROM gsss_page;
*/
