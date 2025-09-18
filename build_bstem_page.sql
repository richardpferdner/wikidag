-- BSTEM Wikipedia Category Tree Builder - Phase 1
-- Builds materialized DAG tree of BSTEM categories and articles
-- Combines category traversal and page metadata in single solution

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
  INDEX idx_root_categories (root_categories(255))
) ENGINE=InnoDB;

-- Temporary staging table for DAG traversal
CREATE TABLE IF NOT EXISTS bstem_traversal (
  page_id INT UNSIGNED NOT NULL,
  root_category VARCHAR(255) NOT NULL,
  level INT NOT NULL,
  processed BOOLEAN DEFAULT FALSE,
  
  PRIMARY KEY (page_id, root_category),
  INDEX idx_level_processed (level, processed),
  INDEX idx_page_id (page_id)
) ENGINE=InnoDB;

-- Progress tracking table
CREATE TABLE IF NOT EXISTS build_progress (
  iteration INT AUTO_INCREMENT PRIMARY KEY,
  level INT,
  root_category VARCHAR(255),
  pages_added INT,
  execution_time_sec DECIMAL(10,3),
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_timestamp (timestamp)
) ENGINE=InnoDB;

-- Error logging table
CREATE TABLE IF NOT EXISTS build_errors (
  error_id INT AUTO_INCREMENT PRIMARY KEY,
  level INT,
  error_message TEXT,
  sql_state VARCHAR(5),
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- ========================================
-- INDEXES ON SOURCE TABLES
-- ========================================

-- Optimize categorylinks for traversal
CREATE INDEX IF NOT EXISTS idx_cl_target_type ON categorylinks (cl_target_id, cl_type);
CREATE INDEX IF NOT EXISTS idx_cl_from ON categorylinks (cl_from);

-- Optimize page table lookups
CREATE INDEX IF NOT EXISTS idx_page_ns_title ON page (page_namespace, page_title);

-- ========================================
-- MAIN BUILD PROCEDURE
-- ========================================

DROP PROCEDURE IF EXISTS BuildBSTEMPageTree;

DELIMITER $$
CREATE PROCEDURE BuildBSTEMPageTree(
  IN p_max_level INT DEFAULT 12,
  IN p_batch_size INT DEFAULT 100000
)
BEGIN
  DECLARE v_current_level INT DEFAULT 0;
  DECLARE v_rows_added INT;
  DECLARE v_total_rows INT DEFAULT 0;
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_continue BOOLEAN DEFAULT TRUE;
  DECLARE v_root_count INT;
  
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1
      @error_message = MESSAGE_TEXT,
      @sql_state = RETURNED_SQLSTATE;
    
    INSERT INTO build_errors (level, error_message, sql_state)
    VALUES (v_current_level, @error_message, @sql_state);
    
    ROLLBACK;
    RESIGNAL;
  END;

  -- Clear existing data for fresh build
  TRUNCATE TABLE bstem_traversal;
  TRUNCATE TABLE bstem_page;
  TRUNCATE TABLE build_progress;
  
  START TRANSACTION;
  
  -- ========================================
  -- STEP 1: Initialize root BSTEM categories
  -- ========================================
  
  SET v_start_time = UNIX_TIMESTAMP(3);
  
  -- Insert root categories into traversal table
  INSERT INTO bstem_traversal (page_id, root_category, level, processed)
  SELECT 
    p.page_id,
    p.page_title,
    0,
    FALSE
  FROM page p
  WHERE p.page_namespace = 14
    AND p.page_title IN ('Business', 'Science', 'Technology', 'Engineering', 'Mathematics');
  
  SET v_root_count = ROW_COUNT();
  
  IF v_root_count = 0 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'No root BSTEM categories found';
  END IF;
  
  -- Log initialization
  INSERT INTO build_progress (level, root_category, pages_added, execution_time_sec)
  VALUES (0, 'INITIALIZATION', v_root_count, UNIX_TIMESTAMP(3) - v_start_time);
  
  COMMIT;
  
  -- ========================================
  -- STEP 2: Traverse category DAG level by level
  -- ========================================
  
  WHILE v_current_level <= p_max_level AND v_continue DO
    SET v_start_time = UNIX_TIMESTAMP(3);
    
    START TRANSACTION;
    
    -- Find all child pages of current level categories
    INSERT IGNORE INTO bstem_traversal (page_id, root_category, level, processed)
    SELECT DISTINCT
      cl.cl_from AS page_id,
      t.root_category,
      v_current_level + 1,
      FALSE
    FROM bstem_traversal t
    JOIN categorylinks cl ON t.page_id = cl.cl_target_id
    JOIN page p ON cl.cl_from = p.page_id
    WHERE t.level = v_current_level 
      AND t.processed = FALSE
      AND cl.cl_type IN ('page', 'subcat')  -- Exclude files
      AND p.page_namespace IN (0, 14)  -- Articles and categories only
    LIMIT p_batch_size;
    
    SET v_rows_added = ROW_COUNT();
    SET v_total_rows = v_total_rows + v_rows_added;
    
    -- Mark current level as processed
    UPDATE bstem_traversal 
    SET processed = TRUE 
    WHERE level = v_current_level AND processed = FALSE;
    
    -- Log progress
    INSERT INTO build_progress (level, root_category, pages_added, execution_time_sec)
    VALUES (v_current_level + 1, 'ALL', v_rows_added, UNIX_TIMESTAMP(3) - v_start_time);
    
    COMMIT;
    
    -- Check if we should continue
    IF v_rows_added = 0 THEN
      SET v_continue = FALSE;
    ELSE
      SET v_current_level = v_current_level + 1;
    END IF;
    
  END WHILE;
  
  -- ========================================
  -- STEP 3: Build final bstem_page table
  -- ========================================
  
  START TRANSACTION;
  SET v_start_time = UNIX_TIMESTAMP(3);
  
  -- Aggregate traversal results and join with page metadata
  INSERT INTO bstem_page (
    page_id, 
    page_namespace, 
    page_title, 
    page_is_redirect, 
    page_is_new,
    page_random, 
    page_touched, 
    page_links_updated, 
    page_latest, 
    page_len,
    page_content_model, 
    page_lang, 
    min_level, 
    root_categories, 
    is_leaf
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
    MIN(t.level) AS min_level,
    GROUP_CONCAT(DISTINCT t.root_category ORDER BY t.root_category SEPARATOR ',') AS root_categories,
    (p.page_namespace = 0) AS is_leaf  -- TRUE for articles, FALSE for categories
  FROM bstem_traversal t
  JOIN page p ON t.page_id = p.page_id
  GROUP BY p.page_id;
  
  SET v_rows_added = ROW_COUNT();
  
  -- Log final build
  INSERT INTO build_progress (level, root_category, pages_added, execution_time_sec)
  VALUES (-1, 'FINAL_BUILD', v_rows_added, UNIX_TIMESTAMP(3) - v_start_time);
  
  COMMIT;
  
  -- ========================================
  -- STEP 4: Cleanup and report
  -- ========================================
  
  -- Optional: Drop traversal table after successful build
  -- DROP TABLE IF EXISTS bstem_traversal;
  
  -- Final statistics
  SELECT 
    'Build Complete' AS status,
    COUNT(*) AS total_pages,
    SUM(CASE WHEN page_namespace = 0 THEN 1 ELSE 0 END) AS articles,
    SUM(CASE WHEN page_namespace = 14 THEN 1 ELSE 0 END) AS categories,
    SUM(page_is_redirect) AS redirects,
    MAX(min_level) AS max_depth,
    COUNT(DISTINCT SUBSTRING_INDEX(root_categories, ',', 1)) AS root_domains
  FROM bstem_page;

END$$
DELIMITER ;

-- ========================================
-- MONITORING VIEWS
-- ========================================

-- Summary by root category
CREATE OR REPLACE VIEW bstem_summary AS
SELECT 
  SUBSTRING_INDEX(root_categories, ',', 1) AS primary_root,
  COUNT(*) AS total_pages,
  SUM(CASE WHEN is_leaf = TRUE THEN 1 ELSE 0 END) AS articles,
  SUM(CASE WHEN is_leaf = FALSE THEN 1 ELSE 0 END) AS categories,
  MIN(min_level) AS min_depth,
  MAX(min_level) AS max_depth,
  AVG(page_len) AS avg_page_length
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
  AVG(page_len) AS avg_page_length
FROM bstem_page
GROUP BY min_level
ORDER BY min_level;

-- Build performance metrics
CREATE OR REPLACE VIEW build_performance AS
SELECT 
  level,
  pages_added,
  execution_time_sec,
  ROUND(pages_added / NULLIF(execution_time_sec, 0), 0) AS pages_per_sec,
  timestamp
FROM build_progress
ORDER BY timestamp;

-- ========================================
-- EXECUTION
-- ========================================

-- Run the build process
CALL BuildBSTEMPageTree(12, 100000);

-- Verify results
SELECT * FROM bstem_summary;
SELECT * FROM bstem_level_distribution;
SELECT * FROM build_performance;
