-- SASS Wikipedia Category Tree Builder - Simplified
-- Builds materialized DAG tree of SASS categories and articles
-- Uses pre-computed wiki_top3_levels for levels 0-2, recursive for levels 3-10
-- Applies optional filtering to exclude maintenance/administrative categories
-- Processes levels 0-10 only

-- ========================================
-- TABLE DEFINITIONS
-- ========================================

-- Main SASS page table with duplicates
CREATE TABLE IF NOT EXISTS sass_page (
  page_id INT UNSIGNED NOT NULL,
  page_title VARCHAR(255) NOT NULL,
  page_parent_id INT NOT NULL,
  page_root_id INT NOT NULL,
  page_dag_level INT NOT NULL,
  page_is_leaf TINYINT(1) NOT NULL DEFAULT 0,
  
  PRIMARY KEY (page_id, page_parent_id),
  INDEX idx_title (page_title),
  INDEX idx_parent (page_parent_id),
  INDEX idx_root (page_root_id),
  INDEX idx_level (page_dag_level),
  INDEX idx_leaf (page_is_leaf)
) ENGINE=InnoDB;

-- Root category mapping
CREATE TABLE IF NOT EXISTS sass_roots (
  root_id INT PRIMARY KEY,
  root_name VARCHAR(255) NOT NULL,
  page_id INT UNSIGNED NOT NULL,
  INDEX idx_name (root_name),
  INDEX idx_page_id (page_id)
) ENGINE=InnoDB;

-- Cycle detection results
CREATE TABLE IF NOT EXISTS sass_cycles (
  page_id INT UNSIGNED NOT NULL,
  ancestor_id INT UNSIGNED NOT NULL,
  path_length INT NOT NULL,
  detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_page (page_id),
  INDEX idx_ancestor (ancestor_id),
  INDEX idx_path_length (path_length)
) ENGINE=InnoDB;

-- Category filtering exclusion patterns
CREATE TABLE IF NOT EXISTS sass_filter_patterns (
  pattern_id INT AUTO_INCREMENT PRIMARY KEY,
  pattern_text VARCHAR(255) NOT NULL,
  pattern_type ENUM('contains', 'starts_with', 'exact') NOT NULL,
  confidence_level ENUM('high', 'medium', 'low') NOT NULL,
  size_threshold INT DEFAULT 0,
  description VARCHAR(500),
  is_active TINYINT(1) DEFAULT 1,
  INDEX idx_pattern_type (pattern_type),
  INDEX idx_confidence (confidence_level),
  INDEX idx_active (is_active)
) ENGINE=InnoDB;

-- ========================================
-- FILTERING FUNCTION
-- ========================================

DROP FUNCTION IF EXISTS should_filter_category;

DELIMITER //

CREATE FUNCTION should_filter_category(
  category_title VARBINARY(255),
  page_length INT,
  page_ns INT
)
RETURNS INT
READS SQL DATA
DETERMINISTIC
BEGIN
  DECLARE filter_count INT DEFAULT 0;
  DECLARE title_lower VARCHAR(255);
  
  -- Skip filtering for articles (namespace 0)
  IF page_ns = 0 THEN
    RETURN 0;
  END IF;
  
  SET title_lower = LOWER(CONVERT(category_title, CHAR));
  
  -- Check high confidence patterns (always filter)
  SELECT COUNT(*) INTO filter_count
  FROM sass_filter_patterns 
  WHERE is_active = 1 
    AND confidence_level = 'high'
    AND (
      (pattern_type = 'contains' AND title_lower LIKE CONCAT('%', LOWER(pattern_text), '%'))
      OR (pattern_type = 'starts_with' AND title_lower LIKE CONCAT(LOWER(pattern_text), '%'))
      OR (pattern_type = 'exact' AND title_lower = LOWER(pattern_text))
    );
  
  IF filter_count > 0 THEN
    RETURN 1;
  END IF;
  
  -- Check medium/low confidence patterns with size thresholds
  SELECT COUNT(*) INTO filter_count
  FROM sass_filter_patterns 
  WHERE is_active = 1 
    AND confidence_level IN ('medium', 'low')
    AND page_length < size_threshold
    AND (
      (pattern_type = 'contains' AND title_lower LIKE CONCAT('%', LOWER(pattern_text), '%'))
      OR (pattern_type = 'starts_with' AND title_lower LIKE CONCAT(LOWER(pattern_text), '%'))
      OR (pattern_type = 'exact' AND title_lower = LOWER(pattern_text))
    );
  
  IF filter_count > 0 THEN
    RETURN 1;
  END IF;
  
  RETURN 0;
END//

DELIMITER ;

-- ========================================
-- MAIN BUILD PROCEDURE
-- ========================================

DROP PROCEDURE IF EXISTS BuildSASSPageTreeFiltered;

DELIMITER //

CREATE PROCEDURE BuildSASSPageTreeFiltered(
  IN p_begin_level INT,
  IN p_end_level INT,
  IN p_enable_filtering TINYINT(1)
)
BEGIN
  DECLARE v_current_level INT;
  DECLARE v_rows_added INT DEFAULT 0;
  DECLARE v_total_pages INT DEFAULT 0;
  DECLARE v_filtered_count INT DEFAULT 0;
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_continue TINYINT(1) DEFAULT 1;
  DECLARE v_levels_012_count INT DEFAULT 0;
  
  -- Set defaults
  IF p_begin_level IS NULL THEN SET p_begin_level = 0; END IF;
  IF p_end_level IS NULL THEN SET p_end_level = 10; END IF;
  IF p_enable_filtering IS NULL THEN SET p_enable_filtering = 1; END IF;

  SET v_start_time = UNIX_TIMESTAMP();
  SET v_current_level = p_begin_level;
  
  -- Clear main table
  TRUNCATE TABLE sass_page;
  
  -- Create temporary working table
  CREATE TEMPORARY TABLE IF NOT EXISTS sass_work_temp (
    page_id INT UNSIGNED NOT NULL,
    parent_id INT UNSIGNED NOT NULL,
    root_id INT NOT NULL,
    level INT NOT NULL,
    
    PRIMARY KEY (page_id, parent_id),
    INDEX idx_level (level)
  ) ENGINE=InnoDB;
  
  TRUNCATE TABLE sass_work_temp;
  
  -- Initialize with pre-computed 3-level hierarchy
  IF p_begin_level <= 2 THEN
    
    SELECT 'Building Levels 0-2 from wiki_top3_levels' AS status;
    
    -- Level 0: Root categories
    INSERT IGNORE INTO sass_page (page_id, page_title, page_parent_id, page_root_id, page_dag_level, page_is_leaf)
    SELECT DISTINCT
      w3l.parent_page_id,
      w3l.parent_title,
      0,
      w3l.parent_page_id,
      0,
      CASE WHEN p.page_namespace = 0 THEN 1 ELSE 0 END
    FROM wiki_top3_levels w3l
    JOIN page p ON w3l.parent_page_id = p.page_id
    WHERE w3l.parent_page_id IS NOT NULL
      AND p.page_content_model = 'wikitext';
    
    SET v_levels_012_count = v_levels_012_count + ROW_COUNT();
    
    -- Level 1: Children
    INSERT IGNORE INTO sass_page (page_id, page_title, page_parent_id, page_root_id, page_dag_level, page_is_leaf)
    SELECT DISTINCT
      w3l.child_page_id,
      w3l.child_title,
      w3l.parent_page_id,
      w3l.parent_page_id,
      1,
      CASE WHEN p.page_namespace = 0 THEN 1 ELSE 0 END
    FROM wiki_top3_levels w3l
    JOIN page p ON w3l.child_page_id = p.page_id
    WHERE w3l.child_page_id IS NOT NULL
      AND p.page_content_model = 'wikitext'
      AND (p_enable_filtering = 0 OR should_filter_category(w3l.child_title, p.page_len, p.page_namespace) = 0);
    
    SET v_levels_012_count = v_levels_012_count + ROW_COUNT();
    
    -- Level 2: Grandchildren
    INSERT IGNORE INTO sass_page (page_id, page_title, page_parent_id, page_root_id, page_dag_level, page_is_leaf)
    SELECT DISTINCT
      w3l.grandchild_page_id,
      w3l.grandchild_title,
      w3l.child_page_id,
      w3l.parent_page_id,
      2,
      CASE WHEN p.page_namespace = 0 THEN 1 ELSE 0 END
    FROM wiki_top3_levels w3l
    JOIN page p ON w3l.grandchild_page_id = p.page_id
    WHERE w3l.grandchild_page_id IS NOT NULL
      AND p.page_content_model = 'wikitext'
      AND (p_enable_filtering = 0 OR should_filter_category(w3l.grandchild_title, p.page_len, p.page_namespace) = 0);
    
    SET v_levels_012_count = v_levels_012_count + ROW_COUNT();
    
    -- Initialize working table with level 2
    INSERT INTO sass_work_temp (page_id, parent_id, root_id, level)
    SELECT page_id, page_parent_id, page_root_id, 2
    FROM sass_page
    WHERE page_dag_level = 2;
    
    -- Progress report
    SELECT 
      'Levels 0-2 Complete' AS status,
      FORMAT(v_levels_012_count, 0) AS precomputed_pages,
      ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS elapsed_sec;
    
    SET v_current_level = 3;
    
  ELSE
    -- Starting beyond level 2
    INSERT INTO sass_work_temp (page_id, parent_id, root_id, level)
    SELECT page_id, page_parent_id, page_root_id, page_dag_level
    FROM sass_page
    WHERE page_dag_level = p_begin_level - 1;
    
    SET v_current_level = p_begin_level;
  END IF;
  
  -- Build levels 3-10 recursively
  WHILE v_continue = 1 AND v_current_level <= p_end_level DO
    
    -- Clear working table for new level
    TRUNCATE TABLE sass_work_temp;
    
    -- Get candidates for next level
    INSERT INTO sass_work_temp (page_id, parent_id, root_id, level)
    SELECT DISTINCT
      cl.cl_from,
      parent_page.page_id,
      parent_page.page_root_id,
      v_current_level
    FROM sass_page parent_page
    JOIN categorylinks cl ON parent_page.page_title = cl.cl_to
    JOIN page p ON cl.cl_from = p.page_id
    WHERE parent_page.page_dag_level = v_current_level - 1
      AND cl.cl_type IN ('page', 'subcat')
      AND p.page_namespace IN (0, 14)
      AND p.page_content_model = 'wikitext'
      AND NOT EXISTS (SELECT 1 FROM sass_page bp WHERE bp.page_id = cl.cl_from)
      AND (p_enable_filtering = 0 OR should_filter_category(p.page_title, p.page_len, p.page_namespace) = 0);
    
    SET v_rows_added = ROW_COUNT();
    
    -- Count filtered if enabled
    IF p_enable_filtering = 1 THEN
      SELECT COUNT(*) INTO v_filtered_count
      FROM sass_page parent_page
      JOIN categorylinks cl ON parent_page.page_title = cl.cl_to
      JOIN page p ON cl.cl_from = p.page_id
      WHERE parent_page.page_dag_level = v_current_level - 1
        AND cl.cl_type IN ('page', 'subcat')
        AND p.page_namespace IN (0, 14)
        AND p.page_content_model = 'wikitext'
        AND NOT EXISTS (SELECT 1 FROM sass_page bp WHERE bp.page_id = cl.cl_from)
        AND should_filter_category(p.page_title, p.page_len, p.page_namespace) = 1;
    END IF;
    
    -- Add pages to main table
    INSERT IGNORE INTO sass_page (page_id, page_title, page_parent_id, page_root_id, page_dag_level, page_is_leaf)
    SELECT 
      p.page_id,
      CONVERT(p.page_title, CHAR),
      w.parent_id,
      w.root_id,
      v_current_level,
      CASE WHEN p.page_namespace = 0 THEN 1 ELSE 0 END
    FROM sass_work_temp w
    JOIN page p ON w.page_id = p.page_id
    WHERE w.level = v_current_level
      AND p.page_content_model = 'wikitext';
    
    SET v_total_pages = v_total_pages + ROW_COUNT();
    
    -- Progress report
    SELECT 
      CONCAT('Level ', v_current_level, ' Complete') AS status,
      FORMAT(v_rows_added, 0) AS candidates_found,
      FORMAT(ROW_COUNT(), 0) AS pages_added,
      FORMAT(v_filtered_count, 0) AS pages_filtered,
      CASE WHEN p_enable_filtering = 1 AND (v_rows_added + v_filtered_count) > 0
        THEN CONCAT(ROUND(100.0 * v_filtered_count / (v_rows_added + v_filtered_count), 1), '%')
        ELSE 'N/A'
      END AS filter_rate,
      FORMAT((SELECT COUNT(DISTINCT page_id) FROM sass_page), 0) AS unique_pages,
      FORMAT((SELECT COUNT(*) FROM sass_page), 0) AS total_rows,
      ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS elapsed_sec;
    
    IF v_rows_added = 0 THEN
      SET v_continue = 0;
    ELSE
      SET v_current_level = v_current_level + 1;
    END IF;
    
  END WHILE;
  
  -- Drop temporary table
  DROP TEMPORARY TABLE IF EXISTS sass_work_temp;
  
  -- Final summary
  SELECT 
    'Build Complete' AS final_status,
    CASE WHEN p_enable_filtering = 1 THEN 'ENABLED' ELSE 'DISABLED' END AS filtering_status,
    v_current_level - 1 AS max_level_reached,
    FORMAT(v_levels_012_count, 0) AS precomputed_pages,
    FORMAT(v_total_pages, 0) AS recursive_pages,
    FORMAT((SELECT COUNT(DISTINCT page_id) FROM sass_page), 0) AS unique_pages,
    FORMAT((SELECT COUNT(*) FROM sass_page), 0) AS total_rows_with_duplicates,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_time_sec;

  -- Level distribution
  SELECT 
    page_dag_level AS level,
    FORMAT(COUNT(*), 0) AS page_count,
    CONCAT(ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM sass_page), 1), '%') AS percentage,
    FORMAT(SUM(CASE WHEN page_is_leaf = 1 THEN 1 ELSE 0 END), 0) AS articles,
    FORMAT(SUM(CASE WHEN page_is_leaf = 0 THEN 1 ELSE 0 END), 0) AS categories
  FROM sass_page 
  GROUP BY page_dag_level 
  ORDER BY page_dag_level;

  -- Root domain distribution
  SELECT 
    'Root Domain Distribution' AS report_type,
    sr.root_name,
    sr.root_id,
    FORMAT(COUNT(sp.page_id), 0) AS total_pages,
    CONCAT(ROUND(100.0 * COUNT(sp.page_id) / (SELECT COUNT(*) FROM sass_page), 1), '%') AS percentage
  FROM sass_roots sr
  LEFT JOIN sass_page sp ON sr.root_id = sp.page_root_id
  GROUP BY sr.root_id, sr.root_name
  ORDER BY COUNT(sp.page_id) DESC;

END//

DELIMITER ;

-- ========================================
-- USAGE EXAMPLES
-- ========================================

/*
-- Drop existing data
DROP TABLE IF EXISTS sass_identity_pages;DROP TABLE IF EXISTS sass_page_clean;DROP TABLE IF EXISTS sass_page;DROP TABLE IF EXISTS sass_cycles;

-- Standard build with filtering (levels 0-10)
source build_sass_page.sql;
CALL BuildSASSPageTreeFiltered(0, 10, 1);

-- Build without filtering
CALL BuildSASSPageTreeFiltered(0, 10, 0);

-- Resume from level 5
CALL BuildSASSPageTreeFiltered(5, 10, 1);

-- Check results
SELECT 
  page_dag_level,
  COUNT(*) AS total_rows,
  COUNT(DISTINCT page_id) AS unique_pages,
  COUNT(DISTINCT page_title) AS unique_titles
FROM sass_page
GROUP BY page_dag_level
ORDER BY page_dag_level;

-- Find pages with multiple parents
SELECT 
  page_id,
  page_title,
  COUNT(*) AS parent_count
FROM sass_page
GROUP BY page_id, page_title
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC
LIMIT 20;

PREREQUISITES:
1. wiki_top3_levels table must exist
2. sass_filter_patterns should be populated if filtering enabled
3. sass_roots should contain root category mappings

NOTES:
- Creates ~9.4M rows (with duplicates for multi-parent pages)
- Levels 0-2 built from wiki_top3_levels (faster, more accurate)
- Levels 3-10 built recursively from categorylinks
- Optional filtering excludes maintenance/administrative categories
- Console logging shows progress after each level
- No persistent tracking tables (restart from scratch on failure)
*/
