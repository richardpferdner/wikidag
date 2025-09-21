-- SASS Wikipedia Associative Link Builder - Phase 3
-- Builds associative relationship mapping for SASS knowledge domain
-- Combines pagelinks and categorylinks into unified relationship network
-- Maintains link directionality and relationship type classification

-- FUTURE:
--  Note: in page_sass, the MIN(parent_id) + GROUP BY page_id logic in the build procedure 
--        permanently discards the other parent relationships. Only one parent per page 
--        survives into sass_page. 
--        Later, build_sass_associative_link.sql will be updated capture these page 
--        and parent relationships as third al_type 'parentlink' and remove 'both' from al_type.

-- ========================================
-- TABLE DEFINITIONS
-- ========================================

-- Main associative link table matching schemas.md specification
CREATE TABLE IF NOT EXISTS sass_associative_link (
  al_from_page_id INT UNSIGNED NOT NULL,
  al_to_page_id INT UNSIGNED NOT NULL,
  al_type ENUM('pagelink','categorylink','both') NOT NULL,
  
  PRIMARY KEY (al_from_page_id, al_to_page_id),
  INDEX idx_from_page (al_from_page_id),
  INDEX idx_to_page (al_to_page_id),
  INDEX idx_type (al_type)
) ENGINE=InnoDB;

-- Temporary working table for pagelink processing
CREATE TABLE IF NOT EXISTS sass_pagelink_work (
  pl_from_page_id INT UNSIGNED NOT NULL,
  pl_to_page_id INT UNSIGNED NOT NULL,
  
  PRIMARY KEY (pl_from_page_id, pl_to_page_id),
  INDEX idx_from (pl_from_page_id),
  INDEX idx_to (pl_to_page_id)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED;

-- Temporary working table for categorylink processing
CREATE TABLE IF NOT EXISTS sass_categorylink_work (
  cl_from_page_id INT UNSIGNED NOT NULL,
  cl_to_page_id INT UNSIGNED NOT NULL,
  
  PRIMARY KEY (cl_from_page_id, cl_to_page_id),
  INDEX idx_from (cl_from_page_id),
  INDEX idx_to (cl_to_page_id)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED;

-- Build progress tracking
CREATE TABLE IF NOT EXISTS associative_build_state (
  state_key VARCHAR(255) PRIMARY KEY,
  state_value INT NOT NULL,
  state_text VARCHAR(500) NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Link type distribution tracking
CREATE TABLE IF NOT EXISTS sass_link_stats (
  stat_id INT AUTO_INCREMENT PRIMARY KEY,
  stat_type VARCHAR(100) NOT NULL,
  stat_value INT NOT NULL,
  stat_percentage DECIMAL(5,2) NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_type (stat_type)
) ENGINE=InnoDB;

-- ========================================
-- MAIN BUILD PROCEDURE
-- ========================================

DROP PROCEDURE IF EXISTS BuildSASSAssociativeLinks;

DELIMITER //

CREATE PROCEDURE BuildSASSAssociativeLinks(
  IN p_batch_size INT,
  IN p_enable_progress_reports TINYINT(1)
)
BEGIN
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_pagelinks_processed INT DEFAULT 0;
  DECLARE v_categorylinks_processed INT DEFAULT 0;
  DECLARE v_both_relationships INT DEFAULT 0;
  DECLARE v_total_links INT DEFAULT 0;
  DECLARE v_self_links_excluded INT DEFAULT 0;
  
  -- Set defaults
  IF p_batch_size IS NULL THEN SET p_batch_size = 1000000; END IF;
  IF p_enable_progress_reports IS NULL THEN SET p_enable_progress_reports = 1; END IF;
  
  SET v_start_time = UNIX_TIMESTAMP();
  
  -- Clear target and working tables
  TRUNCATE TABLE sass_associative_link;
  TRUNCATE TABLE sass_pagelink_work;
  TRUNCATE TABLE sass_categorylink_work;
  TRUNCATE TABLE sass_link_stats;
  
  -- Initialize build state
  INSERT INTO associative_build_state (state_key, state_value, state_text) 
  VALUES ('build_status', 0, 'Starting associative link build')
  ON DUPLICATE KEY UPDATE state_value = 0, state_text = 'Starting associative link build';
  
  -- ========================================
  -- PHASE 1: PROCESS PAGELINKS
  -- ========================================
  
  INSERT INTO associative_build_state (state_key, state_value, state_text) 
  VALUES ('current_phase', 1, 'Processing pagelinks')
  ON DUPLICATE KEY UPDATE state_value = 1, state_text = 'Processing pagelinks';
  
  -- Extract valid pagelinks where both source and target exist in SASS
  INSERT IGNORE INTO sass_pagelink_work (pl_from_page_id, pl_to_page_id)
  SELECT DISTINCT
    pl.pl_from as pl_from_page_id,
    pl.pl_target_id as pl_to_page_id
  FROM pagelinks pl
  JOIN sass_page sp_from ON pl.pl_from = sp_from.page_id          -- Source must be in SASS
  JOIN sass_page sp_to ON pl.pl_target_id = sp_to.page_id         -- Target must be in SASS
  WHERE pl.pl_from != pl.pl_target_id;                           -- Exclude self-links
  
  SET v_pagelinks_processed = ROW_COUNT();
  
  IF p_enable_progress_reports = 1 THEN
    SELECT 
      'Phase 1: Pagelinks' AS status,
      FORMAT(v_pagelinks_processed, 0) AS pagelinks_extracted,
      ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS elapsed_sec;
  END IF;
  
  -- ========================================
  -- PHASE 2: PROCESS CATEGORYLINKS
  -- ========================================
  
  INSERT INTO associative_build_state (state_key, state_value, state_text) 
  VALUES ('current_phase', 2, 'Processing categorylinks')
  ON DUPLICATE KEY UPDATE state_value = 2, state_text = 'Processing categorylinks';
  
  -- Extract valid categorylinks where both source and target exist in SASS
  INSERT IGNORE INTO sass_categorylink_work (cl_from_page_id, cl_to_page_id)
  SELECT DISTINCT
    cl.cl_from as cl_from_page_id,
    cl.cl_target_id as cl_to_page_id
  FROM categorylinks cl
  JOIN sass_page sp_from ON cl.cl_from = sp_from.page_id          -- Source must be in SASS
  JOIN sass_page sp_to ON cl.cl_target_id = sp_to.page_id         -- Target must be in SASS
  WHERE cl.cl_from != cl.cl_target_id                            -- Exclude self-links
    AND cl.cl_type IN ('page', 'subcat');                        -- Exclude files
  
  SET v_categorylinks_processed = ROW_COUNT();
  
  IF p_enable_progress_reports = 1 THEN
    SELECT 
      'Phase 2: Categorylinks' AS status,
      FORMAT(v_categorylinks_processed, 0) AS categorylinks_extracted,
      ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS elapsed_sec;
  END IF;
  
  -- ========================================
  -- PHASE 3: MERGE AND CLASSIFY RELATIONSHIPS
  -- ========================================
  
  INSERT INTO associative_build_state (state_key, state_value, state_text) 
  VALUES ('current_phase', 3, 'Merging relationships')
  ON DUPLICATE KEY UPDATE state_value = 3, state_text = 'Merging relationships';
  
  -- Insert pagelink-only relationships
  INSERT IGNORE INTO sass_associative_link (al_from_page_id, al_to_page_id, al_type)
  SELECT 
    plw.pl_from_page_id,
    plw.pl_to_page_id,
    'pagelink' as al_type
  FROM sass_pagelink_work plw
  WHERE NOT EXISTS (
    SELECT 1 FROM sass_categorylink_work clw 
    WHERE clw.cl_from_page_id = plw.pl_from_page_id 
      AND clw.cl_to_page_id = plw.pl_to_page_id
  );
  
  -- Insert categorylink-only relationships
  INSERT IGNORE INTO sass_associative_link (al_from_page_id, al_to_page_id, al_type)
  SELECT 
    clw.cl_from_page_id,
    clw.cl_to_page_id,
    'categorylink' as al_type
  FROM sass_categorylink_work clw
  WHERE NOT EXISTS (
    SELECT 1 FROM sass_pagelink_work plw 
    WHERE plw.pl_from_page_id = clw.cl_from_page_id 
      AND plw.pl_to_page_id = clw.cl_to_page_id
  );
  
  -- Insert relationships that exist in both (mark as 'both')
  INSERT IGNORE INTO sass_associative_link (al_from_page_id, al_to_page_id, al_type)
  SELECT 
    plw.pl_from_page_id,
    plw.pl_to_page_id,
    'both' as al_type
  FROM sass_pagelink_work plw
  JOIN sass_categorylink_work clw ON plw.pl_from_page_id = clw.cl_from_page_id 
    AND plw.pl_to_page_id = clw.cl_to_page_id;
  
  SET v_total_links = (SELECT COUNT(*) FROM sass_associative_link);
  SET v_both_relationships = (SELECT COUNT(*) FROM sass_associative_link WHERE al_type = 'both');
  
  IF p_enable_progress_reports = 1 THEN
    SELECT 
      'Phase 3: Relationship Merge' AS status,
      FORMAT(v_total_links, 0) AS total_relationships_created,
      FORMAT(v_both_relationships, 0) AS dual_relationships,
      ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS elapsed_sec;
  END IF;
  
  -- ========================================
  -- PHASE 4: GENERATE STATISTICS
  -- ========================================
  
  INSERT INTO associative_build_state (state_key, state_value, state_text) 
  VALUES ('current_phase', 4, 'Generating statistics')
  ON DUPLICATE KEY UPDATE state_value = 4, state_text = 'Generating statistics';
  
  -- Store link type distribution
  INSERT INTO sass_link_stats (stat_type, stat_value, stat_percentage)
  SELECT 
    CONCAT(al_type, '_links') as stat_type,
    COUNT(*) as stat_value,
    ROUND(100.0 * COUNT(*) / v_total_links, 2) as stat_percentage
  FROM sass_associative_link
  GROUP BY al_type;
  
  -- Store overall statistics
  INSERT INTO sass_link_stats (stat_type, stat_value) VALUES
  ('total_associative_links', v_total_links),
  ('unique_source_pages', (SELECT COUNT(DISTINCT al_from_page_id) FROM sass_associative_link)),
  ('unique_target_pages', (SELECT COUNT(DISTINCT al_to_page_id) FROM sass_associative_link)),
  ('pagelinks_processed', v_pagelinks_processed),
  ('categorylinks_processed', v_categorylinks_processed);
  
  -- Update final build state
  INSERT INTO associative_build_state (state_key, state_value, state_text) 
  VALUES ('build_status', 100, 'Build completed successfully')
  ON DUPLICATE KEY UPDATE state_value = 100, state_text = 'Build completed successfully';
  
  INSERT INTO associative_build_state (state_key, state_value) 
  VALUES ('total_associative_links', v_total_links)
  ON DUPLICATE KEY UPDATE state_value = v_total_links;
  
  -- ========================================
  -- FINAL SUMMARY REPORT
  -- ========================================
  
  SELECT 
    'COMPLETE - Associative Link Network' AS final_status,
    FORMAT(v_pagelinks_processed, 0) AS pagelinks_processed,
    FORMAT(v_categorylinks_processed, 0) AS categorylinks_processed,
    FORMAT(v_total_links, 0) AS total_associative_links,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_time_sec;
  
  -- Link type distribution
  SELECT 
    'Link Type Distribution' AS metric_type,
    sal.al_type AS link_type,
    FORMAT(COUNT(*), 0) AS link_count,
    CONCAT(ROUND(100.0 * COUNT(*) / v_total_links, 1), '%') AS percentage
  FROM sass_associative_link sal
  GROUP BY sal.al_type
  ORDER BY COUNT(*) DESC;
  
  -- Connectivity metrics
  SELECT 
    'Network Connectivity Metrics' AS metric_type,
    FORMAT(COUNT(DISTINCT sal.al_from_page_id), 0) AS unique_source_pages,
    FORMAT(COUNT(DISTINCT sal.al_to_page_id), 0) AS unique_target_pages,
    FORMAT(COUNT(DISTINCT sal.al_from_page_id) + COUNT(DISTINCT sal.al_to_page_id), 0) AS total_connected_pages,
    ROUND(COUNT(*) / COUNT(DISTINCT sal.al_from_page_id), 1) AS avg_outbound_links,
    ROUND(COUNT(*) / COUNT(DISTINCT sal.al_to_page_id), 1) AS avg_inbound_links
  FROM sass_associative_link sal;
  
  -- Sample associative relationships
  SELECT 
    'Sample Associative Relationships' AS sample_type,
    CONVERT(sp_from.page_title, CHAR) AS source_title,
    CONVERT(sp_to.page_title, CHAR) AS target_title,
    sal.al_type AS relationship_type,
    sp_from.page_dag_level AS source_level,
    sp_to.page_dag_level AS target_level,
    CASE WHEN sp_from.page_is_leaf = 1 THEN 'Article' ELSE 'Category' END AS source_type,
    CASE WHEN sp_to.page_is_leaf = 1 THEN 'Article' ELSE 'Category' END AS target_type
  FROM sass_associative_link sal
  JOIN sass_page sp_from ON sal.al_from_page_id = sp_from.page_id
  JOIN sass_page sp_to ON sal.al_to_page_id = sp_to.page_id
  ORDER BY RAND()
  LIMIT 15;
  
END//

DELIMITER ;

-- ========================================
-- BATCHED BUILD PROCEDURE WITH PROGRESS TRACKING
-- ========================================

DROP PROCEDURE IF EXISTS BuildSASSAssociativeLinksOptimized;

DELIMITER //

CREATE PROCEDURE BuildSASSAssociativeLinksOptimized(
  IN p_batch_size INT
)
BEGIN
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_total_links INT DEFAULT 0;
  DECLARE v_batch_links INT DEFAULT 0;
  DECLARE v_current_batch INT DEFAULT 0;
  DECLARE v_min_page_id INT DEFAULT 0;
  DECLARE v_max_page_id INT DEFAULT 0;
  DECLARE v_batch_start INT DEFAULT 0;
  DECLARE v_batch_end INT DEFAULT 0;
  DECLARE v_last_processed_id INT DEFAULT 0;
  DECLARE v_continue TINYINT(1) DEFAULT 1;
  DECLARE v_pagelinks_done INT DEFAULT 0;
  DECLARE v_categorylinks_done INT DEFAULT 0;
  
  -- Set defaults
  IF p_batch_size IS NULL THEN SET p_batch_size = 500000; END IF;
  
  SET v_start_time = UNIX_TIMESTAMP();
  
  -- Get page ID range for batching
  SELECT MIN(page_id), MAX(page_id) INTO v_min_page_id, v_max_page_id FROM sass_page;
  
  -- Check for resume capability
  SELECT COALESCE(state_value, 0) INTO v_last_processed_id 
  FROM associative_build_state 
  WHERE state_key = 'last_pagelink_batch_end';
  
  IF v_last_processed_id = 0 THEN
    -- Fresh start - clear tables
    TRUNCATE TABLE sass_associative_link;
    
    INSERT INTO associative_build_state (state_key, state_value, state_text) 
    VALUES ('build_phase', 1, 'Processing pagelinks in batches')
    ON DUPLICATE KEY UPDATE state_value = 1, state_text = 'Processing pagelinks in batches';
  ELSE
    -- Resume from last position
    SELECT 'Resuming from last position' AS status, v_last_processed_id AS last_batch_end;
  END IF;
  
  -- ========================================
  -- PHASE 1: BATCHED PAGELINKS PROCESSING
  -- ========================================
  
  SET v_batch_start = GREATEST(v_min_page_id, v_last_processed_id);
  
  WHILE v_batch_start <= v_max_page_id AND v_continue = 1 DO
    SET v_batch_end = LEAST(v_batch_start + p_batch_size - 1, v_max_page_id);
    SET v_current_batch = v_current_batch + 1;
    
    -- Process pagelinks for current batch
    INSERT IGNORE INTO sass_associative_link (al_from_page_id, al_to_page_id, al_type)
    SELECT DISTINCT
      pl.pl_from as al_from_page_id,
      pl.pl_target_id as al_to_page_id,
      'pagelink' as al_type
    FROM pagelinks pl
    JOIN sass_page sp_from ON pl.pl_from = sp_from.page_id
    JOIN sass_page sp_to ON pl.pl_target_id = sp_to.page_id
    WHERE pl.pl_from BETWEEN v_batch_start AND v_batch_end
      AND pl.pl_from != pl.pl_target_id
      AND NOT EXISTS (
        SELECT 1 FROM categorylinks cl2
        WHERE cl2.cl_from = pl.pl_from 
          AND cl2.cl_target_id = pl.pl_target_id
          AND cl2.cl_type IN ('page', 'subcat')
      );
    
    SET v_batch_links = ROW_COUNT();
    SET v_total_links = v_total_links + v_batch_links;
    
    -- Update progress
    INSERT INTO associative_build_state (state_key, state_value) 
    VALUES ('last_pagelink_batch_end', v_batch_end)
    ON DUPLICATE KEY UPDATE state_value = v_batch_end;
    
    INSERT INTO associative_build_state (state_key, state_value) 
    VALUES ('pagelinks_processed', v_total_links)
    ON DUPLICATE KEY UPDATE state_value = v_total_links;
    
    -- Progress report every batch
    SELECT 
      CONCAT('Pagelinks Batch ', v_current_batch) AS status,
      CONCAT(v_batch_start, ' - ', v_batch_end) AS page_range,
      FORMAT(v_batch_links, 0) AS batch_links,
      FORMAT(v_total_links, 0) AS total_links,
      CONCAT(ROUND(100.0 * (v_batch_end - v_min_page_id) / (v_max_page_id - v_min_page_id), 1), '%') AS progress,
      ROUND(UNIX_TIMESTAMP() - v_start_time, 1) AS elapsed_sec;
    
    SET v_batch_start = v_batch_end + 1;
  END WHILE;
  
  SET v_pagelinks_done = v_total_links;
  
  -- ========================================
  -- PHASE 2: BATCHED CATEGORYLINKS PROCESSING
  -- ========================================
  
  INSERT INTO associative_build_state (state_key, state_value, state_text) 
  VALUES ('build_phase', 2, 'Processing categorylinks in batches')
  ON DUPLICATE KEY UPDATE state_value = 2, state_text = 'Processing categorylinks in batches';
  
  -- Reset for categorylinks
  SET v_batch_start = v_min_page_id;
  SET v_current_batch = 0;
  
  WHILE v_batch_start <= v_max_page_id DO
    SET v_batch_end = LEAST(v_batch_start + p_batch_size - 1, v_max_page_id);
    SET v_current_batch = v_current_batch + 1;
    
    -- Process categorylinks for current batch
    INSERT IGNORE INTO sass_associative_link (al_from_page_id, al_to_page_id, al_type)
    SELECT DISTINCT
      cl.cl_from as al_from_page_id,
      cl.cl_target_id as al_to_page_id,
      'categorylink' as al_type
    FROM categorylinks cl
    JOIN sass_page sp_from ON cl.cl_from = sp_from.page_id
    JOIN sass_page sp_to ON cl.cl_target_id = sp_to.page_id
    WHERE cl.cl_from BETWEEN v_batch_start AND v_batch_end
      AND cl.cl_from != cl.cl_target_id
      AND cl.cl_type IN ('page', 'subcat')
      AND NOT EXISTS (
        SELECT 1 FROM pagelinks pl2
        WHERE pl2.pl_from = cl.cl_from 
          AND pl2.pl_target_id = cl.cl_target_id
      );
    
    SET v_batch_links = ROW_COUNT();
    SET v_total_links = v_total_links + v_batch_links;
    
    -- Update progress
    INSERT INTO associative_build_state (state_key, state_value) 
    VALUES ('categorylinks_processed', v_total_links - v_pagelinks_done)
    ON DUPLICATE KEY UPDATE state_value = v_total_links - v_pagelinks_done;
    
    -- Progress report every batch
    SELECT 
      CONCAT('Categorylinks Batch ', v_current_batch) AS status,
      CONCAT(v_batch_start, ' - ', v_batch_end) AS page_range,
      FORMAT(v_batch_links, 0) AS batch_links,
      FORMAT(v_total_links, 0) AS total_links,
      CONCAT(ROUND(100.0 * (v_batch_end - v_min_page_id) / (v_max_page_id - v_min_page_id), 1), '%') AS progress,
      ROUND(UNIX_TIMESTAMP() - v_start_time, 1) AS elapsed_sec;
    
    SET v_batch_start = v_batch_end + 1;
  END WHILE;
  
  SET v_categorylinks_done = v_total_links - v_pagelinks_done;
  
  -- ========================================
  -- PHASE 3: BATCHED 'BOTH' RELATIONSHIPS
  -- ========================================
  
  INSERT INTO associative_build_state (state_key, state_value, state_text) 
  VALUES ('build_phase', 3, 'Processing dual relationships in batches')
  ON DUPLICATE KEY UPDATE state_value = 3, state_text = 'Processing dual relationships in batches';
  
  -- Reset for 'both' processing
  SET v_batch_start = v_min_page_id;
  SET v_current_batch = 0;
  
  WHILE v_batch_start <= v_max_page_id DO
    SET v_batch_end = LEAST(v_batch_start + p_batch_size - 1, v_max_page_id);
    SET v_current_batch = v_current_batch + 1;
    
    -- Update existing pagelinks to 'both' where categorylink also exists
    UPDATE sass_associative_link sal
    SET sal.al_type = 'both'
    WHERE sal.al_from_page_id BETWEEN v_batch_start AND v_batch_end
      AND sal.al_type = 'pagelink'
      AND EXISTS (
        SELECT 1 FROM categorylinks cl
        WHERE cl.cl_from = sal.al_from_page_id
          AND cl.cl_target_id = sal.al_to_page_id
          AND cl.cl_type IN ('page', 'subcat')
      );
    
    SET v_batch_links = ROW_COUNT();
    
    -- Progress report every batch
    SELECT 
      CONCAT('Dual Relationships Batch ', v_current_batch) AS status,
      CONCAT(v_batch_start, ' - ', v_batch_end) AS page_range,
      FORMAT(v_batch_links, 0) AS updated_to_both,
      CONCAT(ROUND(100.0 * (v_batch_end - v_min_page_id) / (v_max_page_id - v_min_page_id), 1), '%') AS progress,
      ROUND(UNIX_TIMESTAMP() - v_start_time, 1) AS elapsed_sec;
    
    SET v_batch_start = v_batch_end + 1;
  END WHILE;
  
  -- Final count
  SET v_total_links = (SELECT COUNT(*) FROM sass_associative_link);
  
  -- Update final state
  INSERT INTO associative_build_state (state_key, state_value, state_text) 
  VALUES ('build_phase', 4, 'Build completed successfully')
  ON DUPLICATE KEY UPDATE state_value = 4, state_text = 'Build completed successfully';
  
  INSERT INTO associative_build_state (state_key, state_value) 
  VALUES ('total_associative_links', v_total_links)
  ON DUPLICATE KEY UPDATE state_value = v_total_links;
  
  -- Final summary
  SELECT 
    'Batched Associative Build Complete' AS status,
    FORMAT(v_pagelinks_done, 0) AS pagelinks_added,
    FORMAT(v_categorylinks_done, 0) AS categorylinks_added,
    FORMAT(v_total_links, 0) AS total_links_created,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_time_sec;
  
  -- Link type distribution
  SELECT 
    al_type,
    FORMAT(COUNT(*), 0) AS count,
    CONCAT(ROUND(100.0 * COUNT(*) / v_total_links, 1), '%') AS percentage
  FROM sass_associative_link
  GROUP BY al_type
  ORDER BY COUNT(*) DESC;

END//

DELIMITER ;

-- ========================================
-- UTILITY PROCEDURES
-- ========================================

-- Procedure to analyze link patterns and quality
DROP PROCEDURE IF EXISTS AnalyzeAssociativeLinkPatterns;

DELIMITER //

CREATE PROCEDURE AnalyzeAssociativeLinkPatterns()
BEGIN
  -- Link direction analysis
  SELECT 
    'Link Direction Analysis' AS analysis_type,
    CASE 
      WHEN sp_from.page_is_leaf = 1 AND sp_to.page_is_leaf = 1 THEN 'Article → Article'
      WHEN sp_from.page_is_leaf = 1 AND sp_to.page_is_leaf = 0 THEN 'Article → Category'
      WHEN sp_from.page_is_leaf = 0 AND sp_to.page_is_leaf = 1 THEN 'Category → Article'
      WHEN sp_from.page_is_leaf = 0 AND sp_to.page_is_leaf = 0 THEN 'Category → Category'
    END AS link_direction,
    FORMAT(COUNT(*), 0) AS link_count,
    CONCAT(ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM sass_associative_link), 1), '%') AS percentage
  FROM sass_associative_link sal
  JOIN sass_page sp_from ON sal.al_from_page_id = sp_from.page_id
  JOIN sass_page sp_to ON sal.al_to_page_id = sp_to.page_id
  GROUP BY sp_from.page_is_leaf, sp_to.page_is_leaf
  ORDER BY COUNT(*) DESC;
  
  -- Cross-domain analysis
  SELECT 
    'Cross-Domain Link Analysis' AS analysis_type,
    CASE 
      WHEN sp_from.page_root_id = sp_to.page_root_id THEN 'Same Domain'
      ELSE 'Cross Domain'
    END AS domain_relationship,
    FORMAT(COUNT(*), 0) AS link_count,
    CONCAT(ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM sass_associative_link), 1), '%') AS percentage
  FROM sass_associative_link sal
  JOIN sass_page sp_from ON sal.al_from_page_id = sp_from.page_id
  JOIN sass_page sp_to ON sal.al_to_page_id = sp_to.page_id
  GROUP BY CASE WHEN sp_from.page_root_id = sp_to.page_root_id THEN 'Same Domain' ELSE 'Cross Domain' END
  ORDER BY COUNT(*) DESC;
  
  -- Most connected pages
  SELECT 
    'Most Connected Pages (Outbound)' AS connection_type,
    CONVERT(sp.page_title, CHAR) AS page_title,
    COUNT(*) AS outbound_links,
    sp.page_dag_level AS page_level,
    CASE WHEN sp.page_is_leaf = 1 THEN 'Article' ELSE 'Category' END AS page_type
  FROM sass_associative_link sal
  JOIN sass_page sp ON sal.al_from_page_id = sp.page_id
  GROUP BY sal.al_from_page_id
  ORDER BY COUNT(*) DESC
  LIMIT 10;

END//

DELIMITER ;

-- Procedure to test associative search functionality
DROP PROCEDURE IF EXISTS TestAssociativeSearch;

DELIMITER //

CREATE PROCEDURE TestAssociativeSearch(
  IN p_page_title VARCHAR(255),
  IN p_link_type VARCHAR(20)
)
BEGIN
  DECLARE v_page_id INT;
  
  -- Find the page ID
  SELECT page_id INTO v_page_id
  FROM sass_page 
  WHERE CONVERT(page_title, CHAR) = p_page_title
  LIMIT 1;
  
  IF v_page_id IS NULL THEN
    SELECT CONCAT('Page not found: ', p_page_title) AS error_message;
  ELSE
    -- Show outbound links
    SELECT 
      'Outbound Associative Links' AS search_type,
      CONVERT(sp_to.page_title, CHAR) AS linked_to_title,
      sal.al_type AS relationship_type,
      sp_to.page_dag_level AS target_level,
      CASE WHEN sp_to.page_is_leaf = 1 THEN 'Article' ELSE 'Category' END AS target_type
    FROM sass_associative_link sal
    JOIN sass_page sp_to ON sal.al_to_page_id = sp_to.page_id
    WHERE sal.al_from_page_id = v_page_id
      AND (p_link_type IS NULL OR sal.al_type = p_link_type)
    ORDER BY sal.al_type, sp_to.page_title
    LIMIT 20;
    
    -- Show inbound links
    SELECT 
      'Inbound Associative Links' AS search_type,
      CONVERT(sp_from.page_title, CHAR) AS linked_from_title,
      sal.al_type AS relationship_type,
      sp_from.page_dag_level AS source_level,
      CASE WHEN sp_from.page_is_leaf = 1 THEN 'Article' ELSE 'Category' END AS source_type
    FROM sass_associative_link sal
    JOIN sass_page sp_from ON sal.al_from_page_id = sp_from.page_id
    WHERE sal.al_to_page_id = v_page_id
      AND (p_link_type IS NULL OR sal.al_type = p_link_type)
    ORDER BY sal.al_type, sp_from.page_title
    LIMIT 20;
  END IF;

END//

DELIMITER ;

-- Procedure to validate data integrity
DROP PROCEDURE IF EXISTS ValidateAssociativeLinkIntegrity;

DELIMITER //

CREATE PROCEDURE ValidateAssociativeLinkIntegrity()
BEGIN
  DECLARE v_orphaned_sources INT DEFAULT 0;
  DECLARE v_orphaned_targets INT DEFAULT 0;
  DECLARE v_self_links INT DEFAULT 0;
  DECLARE v_invalid_types INT DEFAULT 0;
  
  -- Check for orphaned source pages
  SELECT COUNT(*) INTO v_orphaned_sources
  FROM sass_associative_link sal
  WHERE NOT EXISTS (
    SELECT 1 FROM sass_page sp WHERE sp.page_id = sal.al_from_page_id
  );
  
  -- Check for orphaned target pages
  SELECT COUNT(*) INTO v_orphaned_targets
  FROM sass_associative_link sal
  WHERE NOT EXISTS (
    SELECT 1 FROM sass_page sp WHERE sp.page_id = sal.al_to_page_id
  );
  
  -- Check for self-links
  SELECT COUNT(*) INTO v_self_links
  FROM sass_associative_link sal
  WHERE sal.al_from_page_id = sal.al_to_page_id;
  
  -- Check for invalid link types
  SELECT COUNT(*) INTO v_invalid_types
  FROM sass_associative_link sal
  WHERE sal.al_type NOT IN ('pagelink', 'categorylink', 'both');
  
  -- Report validation results
  SELECT 
    'Data Integrity Validation' AS validation_type,
    v_orphaned_sources AS orphaned_sources,
    v_orphaned_targets AS orphaned_targets,
    v_self_links AS self_links,
    v_invalid_types AS invalid_types,
    CASE 
      WHEN v_orphaned_sources = 0 AND v_orphaned_targets = 0 AND v_self_links = 0 AND v_invalid_types = 0 
      THEN 'PASS' 
      ELSE 'FAIL' 
    END AS validation_status;

END//

DELIMITER ;

-- ========================================
-- USAGE EXAMPLES AND DOCUMENTATION
-- ========================================

/*
-- ASSOCIATIVE LINK BUILD EXAMPLES

-- Standard build with progress reporting:
CALL BuildSASSAssociativeLinks(1000000, 1);

-- Optimized build for large datasets:
CALL BuildSASSAssociativeLinksOptimized(1);

-- Analyze link patterns:
CALL AnalyzeAssociativeLinkPatterns();

-- Test associative search:
CALL TestAssociativeSearch('Machine_learning', NULL);
CALL TestAssociativeSearch('Artificial_intelligence', 'pagelink');
CALL TestAssociativeSearch('Computer_science', 'categorylink');

-- Validate data integrity:
CALL ValidateAssociativeLinkIntegrity();

-- Check build status:
SELECT * FROM associative_build_state ORDER BY updated_at DESC;

-- Sample queries on final data:

-- Find all pages linked to "Machine Learning":
SELECT 
  CONVERT(sp_from.page_title, CHAR) as source_title,
  sal.al_type as relationship_type,
  sp_from.page_dag_level as source_level
FROM sass_associative_link sal
JOIN sass_page sp_from ON sal.al_from_page_id = sp_from.page_id
JOIN sass_page sp_to ON sal.al_to_page_id = sp_to.page_id
WHERE CONVERT(sp_to.page_title, CHAR) = 'Machine_learning'
ORDER BY sal.al_type, source_title;

-- Most interconnected categories:
SELECT 
  CONVERT(sp.page_title, CHAR) as category_title,
  COUNT(DISTINCT sal_out.al_to_page_id) as outbound_links,
  COUNT(DISTINCT sal_in.al_from_page_id) as inbound_links,
  COUNT(DISTINCT sal_out.al_to_page_id) + COUNT(DISTINCT sal_in.al_from_page_id) as total_connections
FROM sass_page sp
LEFT JOIN sass_associative_link sal_out ON sp.page_id = sal_out.al_from_page_id
LEFT JOIN sass_associative_link sal_in ON sp.page_id = sal_in.al_to_page_id
WHERE sp.page_is_leaf = 0
GROUP BY sp.page_id
ORDER BY total_connections DESC
LIMIT 20;

-- Cross-domain knowledge bridges:
SELECT 
  CONVERT(sp_from.page_title, CHAR) as source_title,
  CONVERT(sp_to.page_title, CHAR) as target_title,
  sal.al_type,
  sp_from.page_root_id as source_domain,
  sp_to.page_root_id as target_domain
FROM sass_associative_link sal
JOIN sass_page sp_from ON sal.al_from_page_id = sp_from.page_id
JOIN sass_page sp_to ON sal.al_to_page_id = sp_to.page_id
WHERE sp_from.page_root_id != sp_to.page_root_id
  AND sp_from.page_is_leaf = 1 
  AND sp_to.page_is_leaf = 1
ORDER BY RAND()
LIMIT 20;

PERFORMANCE NOTES:
- Standard build uses working tables for memory efficiency
- Optimized build uses single union query (faster for sufficient RAM)
- Both versions maintain referential integrity with SASS pages only
- Self-links are explicitly excluded during extraction

QUALITY METRICS:
- Relationship type classification enables targeted queries
- Cross-domain links identify knowledge bridges between SASS domains
- Bidirectional connectivity analysis reveals hub pages and isolated content
- Data validation ensures consistency with source Wikipedia structures
*/
