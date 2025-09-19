-- GSSS Wikipedia Category Tree Builder - Phase 1 (Fixed)
-- Builds materialized DAG tree of GSSS categories and articles
-- Processes ALL valid records per level (no truncation)

-- ========================================
-- TABLE DEFINITIONS (unchanged)
-- ========================================

-- Phase 2 Preparation: Clean title table for lexical search
CREATE TABLE IF NOT EXISTS gsss_clean_titles (
  page_id INT UNSIGNED PRIMARY KEY,
  clean_title VARCHAR(255) NOT NULL,
  INDEX idx_clean_title (clean_title)
) ENGINE=InnoDB;

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
-- INITIALIZATION - Find Root Categories
-- ========================================

-- Initialize root category mapping with binary literals
INSERT IGNORE INTO gsss_roots (root_id, root_name, page_id)
SELECT 1, 'Geography', page_id FROM page 
WHERE page_namespace = 14 
  AND page_content_model = 'wikitext' 
  AND page_title = 0x47656F677261706879
UNION ALL
SELECT 2, 'Science', page_id FROM page 
WHERE page_namespace = 14 
  AND page_content_model = 'wikitext'
  AND page_title = 0x536369656E6365
UNION ALL
SELECT 3, 'Social_sciences', page_id FROM page 
WHERE page_namespace = 14 
  AND page_content_model = 'wikitext'
  AND page_title = 0x536F6369616C5F736369656E636573;

-- ========================================
-- FIXED BUILD PROCEDURE - NO TRUNCATION
-- ========================================

DROP PROCEDURE IF EXISTS BuildGSSSPageTree;

DELIMITER //

CREATE PROCEDURE BuildGSSSPageTree(
  IN p_begin_level INT,
  IN p_end_level INT,
  IN p_batch_size INT  -- Now used for chunked processing, not limits
)
BEGIN
  DECLARE v_current_level INT;
  DECLARE v_rows_added INT DEFAULT 0;
  DECLARE v_total_new_pages INT DEFAULT 0;
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_continue TINYINT(1) DEFAULT 1;
  DECLARE v_batch_offset INT DEFAULT 0;
  DECLARE v_level_complete TINYINT(1) DEFAULT 0;
  
  -- Set defaults
  IF p_begin_level IS NULL THEN SET p_begin_level = 0; END IF;
  IF p_end_level IS NULL THEN SET p_end_level = 12; END IF;
  IF p_batch_size IS NULL THEN SET p_batch_size = 100000; END IF; -- Smaller default for chunking

  SET v_start_time = UNIX_TIMESTAMP();
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
    
    -- Find children of current level parents (all records, no limit)
    INSERT IGNORE INTO gsss_work (page_id, parent_id, root_id, level)
    SELECT DISTINCT
      cl.cl_from,
      w.page_id,
      w.root_id,
      v_current_level
    FROM gsss_work w
    JOIN page parent_page ON w.page_id = parent_page.page_id
    JOIN categorylinks cl ON parent_page.page_title = cl.cl_to
    JOIN page p ON cl.cl_from = p.page_id
    WHERE w.level = v_current_level - 1
      AND cl.cl_type IN ('page', 'subcat')
      AND p.page_namespace IN (0, 14)
      AND p.page_content_model = 'wikitext'
      AND NOT EXISTS (SELECT 1 FROM gsss_page bp WHERE bp.page_id = cl.cl_from);
    
    SET v_rows_added = ROW_COUNT();
    
    -- Process all new pages in batches (if many records)
    SET v_batch_offset = 0;
    SET v_level_complete = 0;
    
    WHILE v_level_complete = 0 DO
      -- Add pages to gsss_page in batches
      INSERT IGNORE INTO gsss_page (page_id, page_title, page_parent_id, page_root_id, page_dag_level, page_is_leaf)
      SELECT 
        p.page_id,
        CONVERT(p.page_title, CHAR) as clean_title,
        MIN(w.parent_id),
        MIN(w.root_id),
        v_current_level,
        CASE WHEN p.page_namespace = 0 THEN 1 ELSE 0 END
      FROM gsss_work w
      JOIN page p ON w.page_id = p.page_id
      WHERE w.level = v_current_level
        AND p.page_content_model = 'wikitext'
      GROUP BY p.page_id
      ORDER BY p.page_id  -- Deterministic ordering
      LIMIT p_batch_size OFFSET v_batch_offset;
      
      -- Check if this batch added any rows
      IF ROW_COUNT() = 0 THEN
        SET v_level_complete = 1;
      ELSE
        SET v_total_new_pages = v_total_new_pages + ROW_COUNT();
        SET v_batch_offset = v_batch_offset + p_batch_size;
      END IF;
    END WHILE;
    
    -- Check if we should continue to next level
    IF v_rows_added = 0 THEN
      SET v_continue = 0;
    ELSE
      SET v_current_level = v_current_level + 1;
    END IF;
    
    -- Report progress
    SELECT 
      CONCAT('Level ', v_current_level - 1, ' completed') AS status,
      v_rows_added AS pages_found,
      (SELECT COUNT(*) FROM gsss_page WHERE page_dag_level = v_current_level - 1) AS pages_added_this_level,
      (SELECT COUNT(*) FROM gsss_page) AS total_pages_so_far,
      ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS elapsed_time_sec;
    
  END WHILE;
  
  -- Update completed level
  INSERT INTO build_state (state_key, state_value) 
  VALUES ('last_completed_level', v_current_level - 1)
  ON DUPLICATE KEY UPDATE state_value = v_current_level - 1;
  
  -- Final report with level breakdown
  SELECT 
    'Build Complete' AS status,
    p_begin_level AS requested_begin_level,
    p_end_level AS requested_end_level,
    v_current_level - 1 AS actual_end_level,
    v_total_new_pages AS new_pages_added,
    (SELECT COUNT(*) FROM gsss_page) AS total_pages_now,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_execution_time_sec;

  -- Show level breakdown
  SELECT 
    page_dag_level,
    FORMAT(COUNT(*), 0) as pages_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM gsss_page), 2) as percentage
  FROM gsss_page 
  GROUP BY page_dag_level 
  ORDER BY page_dag_level;

END//

DELIMITER ;

-- ========================================
-- ALTERNATIVE: Simple Non-Batched Version
-- ========================================

DROP PROCEDURE IF EXISTS BuildGSSSPageTreeSimple;

DELIMITER //

CREATE PROCEDURE BuildGSSSPageTreeSimple(
  IN p_begin_level INT,
  IN p_end_level INT
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

  SET v_start_time = UNIX_TIMESTAMP();
  SET v_current_level = p_begin_level;
  
  -- Clear working table
  TRUNCATE TABLE gsss_work;
  
  -- Initialize with root categories for level 0
  IF p_begin_level = 0 THEN
    INSERT INTO gsss_work (page_id, parent_id, root_id, level)
    SELECT page_id, 0, root_id, 0 FROM gsss_roots;
    
    INSERT IGNORE INTO gsss_page (page_id, page_title, page_parent_id, page_root_id, page_dag_level, page_is_leaf)
    SELECT p.page_id, p.page_title, 0, gr.root_id, 0, 0
    FROM gsss_roots gr
    JOIN page p ON gr.page_id = p.page_id;
    
    SET v_total_new_pages = ROW_COUNT();
    SET v_current_level = 1;
  END IF;
  
  -- Level-by-level traversal - COMPLETE PROCESSING
  WHILE v_current_level <= p_end_level AND v_continue = 1 DO
    
    -- Find ALL children (no limits)
    INSERT IGNORE INTO gsss_work (page_id, parent_id, root_id, level)
    SELECT DISTINCT
      cl.cl_from,
      w.page_id,
      w.root_id,
      v_current_level
    FROM gsss_work w
    JOIN page parent_page ON w.page_id = parent_page.page_id
    JOIN categorylinks cl ON parent_page.page_title = cl.cl_to
    JOIN page p ON cl.cl_from = p.page_id
    WHERE w.level = v_current_level - 1
      AND cl.cl_type IN ('page', 'subcat')
      AND p.page_namespace IN (0, 14)
      AND p.page_content_model = 'wikitext'
      AND NOT EXISTS (SELECT 1 FROM gsss_page bp WHERE bp.page_id = cl.cl_from);
    
    SET v_rows_added = ROW_COUNT();
    
    -- Add ALL new pages (no batch limit)
    INSERT IGNORE INTO gsss_page (page_id, page_title, page_parent_id, page_root_id, page_dag_level, page_is_leaf)
    SELECT 
      p.page_id,
      CONVERT(p.page_title, CHAR) as clean_title,
      MIN(w.parent_id),
      MIN(w.root_id),
      v_current_level,
      CASE WHEN p.page_namespace = 0 THEN 1 ELSE 0 END
    FROM gsss_work w
    JOIN page p ON w.page_id = p.page_id
    WHERE w.level = v_current_level
      AND p.page_content_model = 'wikitext'
    GROUP BY p.page_id;
    -- NO LIMIT CLAUSE!
    
    SET v_total_new_pages = v_total_new_pages + ROW_COUNT();
    
    -- Progress report
    SELECT 
      CONCAT('Level ', v_current_level, ' completed') AS status,
      FORMAT(v_rows_added, 0) AS candidates_found,
      FORMAT(ROW_COUNT(), 0) AS pages_added,
      FORMAT((SELECT COUNT(*) FROM gsss_page), 0) AS total_pages,
      ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS elapsed_sec;
    
    IF v_rows_added = 0 THEN
      SET v_continue = 0;
    ELSE
      SET v_current_level = v_current_level + 1;
    END IF;
    
  END WHILE;
  
  -- Final summary
  SELECT 
    'COMPLETE - All Valid Records Processed' AS final_status,
    v_current_level - 1 AS max_level_reached,
    FORMAT(v_total_new_pages, 0) AS total_pages_added,
    FORMAT((SELECT COUNT(*) FROM gsss_page), 0) AS final_page_count,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_time_sec;

  -- Level distribution
  SELECT 
    page_dag_level as level,
    FORMAT(COUNT(*), 0) as page_count,
    CONCAT(ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM gsss_page), 1), '%') as percentage
  FROM gsss_page 
  GROUP BY page_dag_level 
  ORDER BY page_dag_level;

END//

DELIMITER ;

-- ========================================
-- USAGE EXAMPLES
-- ========================================

/*
-- For complete processing without truncation:
CALL BuildGSSSPageTreeSimple(0, 12);

-- For batched processing (if memory constraints):
CALL BuildGSSSPageTree(0, 12, 50000);

-- Check results:
SELECT page_dag_level, FORMAT(COUNT(*), 0) as pages 
FROM gsss_page 
GROUP BY page_dag_level 
ORDER BY page_dag_level;
*/
