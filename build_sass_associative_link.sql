-- SASS Wikipedia Associative Link Builder - Streaming Representative Resolution
-- Builds associative relationship mapping with representative page resolution
-- Implements streaming approach for large-scale pagelinks/categorylinks processing
-- Optimized for Mac Studio M4 with aggressive pre-filtering and memory management

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

-- Streaming buffer table for batch processing
CREATE TABLE IF NOT EXISTS sass_associative_buffer (
  al_from_page_id INT UNSIGNED NOT NULL,
  al_to_page_id INT UNSIGNED NOT NULL,
  al_type ENUM('pagelink','categorylink') NOT NULL,
  al_batch_id INT NOT NULL,
  
  PRIMARY KEY (al_from_page_id, al_to_page_id, al_type),
  INDEX idx_batch (al_batch_id),
  INDEX idx_from (al_from_page_id),
  INDEX idx_to (al_to_page_id)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED;

-- Build progress tracking
CREATE TABLE IF NOT EXISTS associative_build_state (
  state_key VARCHAR(255) PRIMARY KEY,
  state_value BIGINT NOT NULL,
  state_text VARCHAR(500) NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Link processing statistics
CREATE TABLE IF NOT EXISTS sass_link_processing_stats (
  stat_id INT AUTO_INCREMENT PRIMARY KEY,
  processing_phase VARCHAR(100) NOT NULL,
  stat_type VARCHAR(100) NOT NULL,
  stat_value BIGINT NOT NULL,
  stat_percentage DECIMAL(8,3) NULL,
  batch_id INT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_phase (processing_phase),
  INDEX idx_type (stat_type),
  INDEX idx_batch (batch_id)
) ENGINE=InnoDB;

-- Representative consolidation tracking
CREATE TABLE IF NOT EXISTS sass_representative_consolidation (
  consolidation_id INT AUTO_INCREMENT PRIMARY KEY,
  original_source_id INT UNSIGNED NOT NULL,
  original_target_id INT UNSIGNED NOT NULL,
  representative_source_id INT UNSIGNED NOT NULL,
  representative_target_id INT UNSIGNED NOT NULL,
  link_type ENUM('pagelink','categorylink') NOT NULL,
  is_consolidated TINYINT(1) NOT NULL DEFAULT 0,
  INDEX idx_original_source (original_source_id),
  INDEX idx_representative_source (representative_source_id),
  INDEX idx_consolidated (is_consolidated)
) ENGINE=InnoDB;

-- ========================================
-- STREAMING REPRESENTATIVE RESOLUTION PROCEDURE
-- ========================================

DROP PROCEDURE IF EXISTS BuildSASSAssociativeLinksStreaming;

DELIMITER //

CREATE PROCEDURE BuildSASSAssociativeLinksStreaming(
  IN p_batch_size INT,
  IN p_enable_progress_reports TINYINT(1),
  IN p_enable_consolidation_tracking TINYINT(1)
)
BEGIN
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_pagelinks_candidates BIGINT DEFAULT 0;
  DECLARE v_categorylinks_candidates BIGINT DEFAULT 0;
  DECLARE v_pagelinks_processed BIGINT DEFAULT 0;
  DECLARE v_categorylinks_processed BIGINT DEFAULT 0;
  DECLARE v_total_links_created BIGINT DEFAULT 0;
  DECLARE v_consolidations_detected BIGINT DEFAULT 0;
  DECLARE v_self_links_excluded BIGINT DEFAULT 0;
  DECLARE v_current_batch INT DEFAULT 0;
  DECLARE v_continue TINYINT(1) DEFAULT 1;
  DECLARE v_phase VARCHAR(100);
  
  -- Set defaults
  IF p_batch_size IS NULL THEN SET p_batch_size = 1000000; END IF;
  IF p_enable_progress_reports IS NULL THEN SET p_enable_progress_reports = 1; END IF;
  IF p_enable_consolidation_tracking IS NULL THEN SET p_enable_consolidation_tracking = 0; END IF;
  
  SET v_start_time = UNIX_TIMESTAMP();
  
  -- Clear target and working tables
  TRUNCATE TABLE sass_associative_link;
  TRUNCATE TABLE sass_associative_buffer;
  TRUNCATE TABLE sass_link_processing_stats;
  
  IF p_enable_consolidation_tracking = 1 THEN
    TRUNCATE TABLE sass_representative_consolidation;
  END IF;
  
  -- Initialize build state
  INSERT INTO associative_build_state (state_key, state_value, state_text) 
  VALUES ('build_status', 0, 'Starting streaming associative link build with representative resolution')
  ON DUPLICATE KEY UPDATE state_value = 0, state_text = 'Starting streaming associative link build with representative resolution';
  
  -- ========================================
  -- PHASE 1: AGGRESSIVE PRE-FILTERING AND CANDIDATE ANALYSIS
  -- ========================================
  
  SET v_phase = 'Phase 1: Pre-filtering';
  
  INSERT INTO associative_build_state (state_key, state_value, state_text) 
  VALUES ('current_phase', 1, 'Analyzing candidates with SASS domain filtering')
  ON DUPLICATE KEY UPDATE state_value = 1, state_text = 'Analyzing candidates with SASS domain filtering';
  
  -- Count pagelink candidates (both source and target in SASS)
  SELECT COUNT(*) INTO v_pagelinks_candidates
  FROM pagelinks pl
  WHERE EXISTS (SELECT 1 FROM sass_identity_pages sip_src WHERE pl.pl_from = sip_src.page_id)
    AND EXISTS (SELECT 1 FROM sass_identity_pages sip_tgt WHERE pl.pl_target_id = sip_tgt.page_id)
    AND pl.pl_from != pl.pl_target_id;
  
  -- Count categorylink candidates (both source and target in SASS)
  SELECT COUNT(*) INTO v_categorylinks_candidates
  FROM categorylinks cl
  WHERE EXISTS (SELECT 1 FROM sass_identity_pages sip_src WHERE cl.cl_from = sip_src.page_id)
    AND EXISTS (SELECT 1 FROM sass_identity_pages sip_tgt WHERE cl.cl_target_id = sip_tgt.page_id)
    AND cl.cl_from != cl.cl_target_id
    AND cl.cl_type IN ('page', 'subcat');
  
  -- Record pre-filtering statistics
  INSERT INTO sass_link_processing_stats (processing_phase, stat_type, stat_value)
  VALUES 
    (v_phase, 'pagelink_candidates', v_pagelinks_candidates),
    (v_phase, 'categorylink_candidates', v_categorylinks_candidates),
    (v_phase, 'total_candidates', v_pagelinks_candidates + v_categorylinks_candidates);
  
  IF p_enable_progress_reports = 1 THEN
    SELECT 
      'Phase 1: Domain Pre-filtering Complete' AS status,
      FORMAT(v_pagelinks_candidates, 0) AS pagelink_candidates,
      FORMAT(v_categorylinks_candidates, 0) AS categorylink_candidates,
      FORMAT(v_pagelinks_candidates + v_categorylinks_candidates, 0) AS total_candidates,
      CONCAT(ROUND(100.0 * (v_pagelinks_candidates + v_categorylinks_candidates) / 1700000000, 2), '%') AS estimated_reduction,
      ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS elapsed_sec;
  END IF;
  
  -- ========================================
  -- PHASE 2A: STREAMING PAGELINKS PROCESSING
  -- ========================================
  
  SET v_phase = 'Phase 2A: Pagelinks';
  SET v_current_batch = 0;
  
  INSERT INTO associative_build_state (state_key, state_value, state_text) 
  VALUES ('current_phase', 2, 'Streaming pagelinks with representative resolution')
  ON DUPLICATE KEY UPDATE state_value = 2, state_text = 'Streaming pagelinks with representative resolution';
  
  -- Process pagelinks in streaming batches
  SET @pagelinks_offset = 0;
  
  WHILE v_continue = 1 DO
    SET v_current_batch = v_current_batch + 1;
    
    -- Stream batch with immediate representative resolution
    INSERT IGNORE INTO sass_associative_buffer (al_from_page_id, al_to_page_id, al_type, al_batch_id)
    SELECT DISTINCT
      sip_src.representative_page_id as al_from_page_id,
      sip_tgt.representative_page_id as al_to_page_id,
      'pagelink' as al_type,
      v_current_batch as al_batch_id
    FROM (
      SELECT pl.pl_from, pl.pl_target_id
      FROM pagelinks pl
      WHERE EXISTS (SELECT 1 FROM sass_identity_pages sip WHERE pl.pl_from = sip.page_id)
        AND EXISTS (SELECT 1 FROM sass_identity_pages sip WHERE pl.pl_target_id = sip.page_id)
        AND pl.pl_from != pl.pl_target_id
      ORDER BY pl.pl_from, pl.pl_target_id
      LIMIT p_batch_size OFFSET @pagelinks_offset
    ) pl_batch
    JOIN sass_identity_pages sip_src ON pl_batch.pl_from = sip_src.page_id
    JOIN sass_identity_pages sip_tgt ON pl_batch.pl_target_id = sip_tgt.page_id
    WHERE sip_src.representative_page_id != sip_tgt.representative_page_id;  -- Exclude self-links at representative level
    
    SET v_pagelinks_processed = v_pagelinks_processed + ROW_COUNT();
    SET @pagelinks_offset = @pagelinks_offset + p_batch_size;
    
    -- Track consolidations if enabled
    IF p_enable_consolidation_tracking = 1 AND ROW_COUNT() > 0 THEN
      INSERT INTO sass_representative_consolidation (
        original_source_id, original_target_id, 
        representative_source_id, representative_target_id, 
        link_type, is_consolidated
      )
      SELECT 
        pl_batch.pl_from, pl_batch.pl_target_id,
        sip_src.representative_page_id, sip_tgt.representative_page_id,
        'pagelink',
        CASE WHEN pl_batch.pl_from != sip_src.representative_page_id 
                  OR pl_batch.pl_target_id != sip_tgt.representative_page_id 
             THEN 1 ELSE 0 END
      FROM (
        SELECT pl.pl_from, pl.pl_target_id
        FROM pagelinks pl
        WHERE EXISTS (SELECT 1 FROM sass_identity_pages sip WHERE pl.pl_from = sip.page_id)
          AND EXISTS (SELECT 1 FROM sass_identity_pages sip WHERE pl.pl_target_id = sip.page_id)
          AND pl.pl_from != pl.pl_target_id
        ORDER BY pl.pl_from, pl.pl_target_id
        LIMIT p_batch_size OFFSET (@pagelinks_offset - p_batch_size)
      ) pl_batch
      JOIN sass_identity_pages sip_src ON pl_batch.pl_from = sip_src.page_id
      JOIN sass_identity_pages sip_tgt ON pl_batch.pl_target_id = sip_tgt.page_id;
    END IF;
    
    -- Progress report
    IF p_enable_progress_reports = 1 AND v_current_batch % 10 = 0 THEN
      SELECT 
        CONCAT('Pagelinks Batch ', v_current_batch) AS status,
        FORMAT(ROW_COUNT(), 0) AS links_in_batch,
        FORMAT(v_pagelinks_processed, 0) AS total_pagelinks_processed,
        CONCAT(ROUND(100.0 * v_pagelinks_processed / v_pagelinks_candidates, 1), '%') AS pagelinks_progress,
        ROUND(UNIX_TIMESTAMP() - v_start_time, 1) AS elapsed_sec;
    END IF;
    
    -- Check if done with pagelinks
    IF ROW_COUNT() < p_batch_size THEN
      SET v_continue = 0;
    END IF;
    
  END WHILE;
  
  -- Record pagelinks statistics
  INSERT INTO sass_link_processing_stats (processing_phase, stat_type, stat_value)
  VALUES 
    (v_phase, 'pagelinks_processed', v_pagelinks_processed),
    (v_phase, 'pagelinks_batches', v_current_batch);
  
  -- ========================================
  -- PHASE 2B: STREAMING CATEGORYLINKS PROCESSING
  -- ========================================
  
  SET v_phase = 'Phase 2B: Categorylinks';
  SET v_current_batch = 0;
  SET v_continue = 1;
  
  INSERT INTO associative_build_state (state_key, state_value, state_text) 
  VALUES ('current_phase', 3, 'Streaming categorylinks with representative resolution')
  ON DUPLICATE KEY UPDATE state_value = 3, state_text = 'Streaming categorylinks with representative resolution';
  
  -- Process categorylinks in streaming batches
  SET @categorylinks_offset = 0;
  
  WHILE v_continue = 1 DO
    SET v_current_batch = v_current_batch + 1;
    
    -- Stream batch with immediate representative resolution
    INSERT IGNORE INTO sass_associative_buffer (al_from_page_id, al_to_page_id, al_type, al_batch_id)
    SELECT DISTINCT
      sip_src.representative_page_id as al_from_page_id,
      sip_tgt.representative_page_id as al_to_page_id,
      'categorylink' as al_type,
      v_current_batch + 10000 as al_batch_id  -- Offset to distinguish from pagelinks
    FROM (
      SELECT cl.cl_from, cl.cl_target_id
      FROM categorylinks cl
      WHERE EXISTS (SELECT 1 FROM sass_identity_pages sip WHERE cl.cl_from = sip.page_id)
        AND EXISTS (SELECT 1 FROM sass_identity_pages sip WHERE cl.cl_target_id = sip.page_id)
        AND cl.cl_from != cl.cl_target_id
        AND cl.cl_type IN ('page', 'subcat')
      ORDER BY cl.cl_from, cl.cl_target_id
      LIMIT p_batch_size OFFSET @categorylinks_offset
    ) cl_batch
    JOIN sass_identity_pages sip_src ON cl_batch.cl_from = sip_src.page_id
    JOIN sass_identity_pages sip_tgt ON cl_batch.cl_target_id = sip_tgt.page_id
    WHERE sip_src.representative_page_id != sip_tgt.representative_page_id;  -- Exclude self-links at representative level
    
    SET v_categorylinks_processed = v_categorylinks_processed + ROW_COUNT();
    SET @categorylinks_offset = @categorylinks_offset + p_batch_size;
    
    -- Track consolidations if enabled
    IF p_enable_consolidation_tracking = 1 AND ROW_COUNT() > 0 THEN
      INSERT INTO sass_representative_consolidation (
        original_source_id, original_target_id, 
        representative_source_id, representative_target_id, 
        link_type, is_consolidated
      )
      SELECT 
        cl_batch.cl_from, cl_batch.cl_target_id,
        sip_src.representative_page_id, sip_tgt.representative_page_id,
        'categorylink',
        CASE WHEN cl_batch.cl_from != sip_src.representative_page_id 
                  OR cl_batch.cl_target_id != sip_tgt.representative_page_id 
             THEN 1 ELSE 0 END
      FROM (
        SELECT cl.cl_from, cl.cl_target_id
        FROM categorylinks cl
        WHERE EXISTS (SELECT 1 FROM sass_identity_pages sip WHERE cl.cl_from = sip.page_id)
          AND EXISTS (SELECT 1 FROM sass_identity_pages sip WHERE cl.cl_target_id = sip.page_id)
          AND cl.cl_from != cl.cl_target_id
          AND cl.cl_type IN ('page', 'subcat')
        ORDER BY cl.cl_from, cl.cl_target_id
        LIMIT p_batch_size OFFSET (@categorylinks_offset - p_batch_size)
      ) cl_batch
      JOIN sass_identity_pages sip_src ON cl_batch.cl_from = sip_src.page_id
      JOIN sass_identity_pages sip_tgt ON cl_batch.cl_target_id = sip_tgt.page_id;
    END IF;
    
    -- Progress report
    IF p_enable_progress_reports = 1 AND v_current_batch % 10 = 0 THEN
      SELECT 
        CONCAT('Categorylinks Batch ', v_current_batch) AS status,
        FORMAT(ROW_COUNT(), 0) AS links_in_batch,
        FORMAT(v_categorylinks_processed, 0) AS total_categorylinks_processed,
        CONCAT(ROUND(100.0 * v_categorylinks_processed / v_categorylinks_candidates, 1), '%') AS categorylinks_progress,
        ROUND(UNIX_TIMESTAMP() - v_start_time, 1) AS elapsed_sec;
    END IF;
    
    -- Check if done with categorylinks
    IF ROW_COUNT() < p_batch_size THEN
      SET v_continue = 0;
    END IF;
    
  END WHILE;
  
  -- Record categorylinks statistics
  INSERT INTO sass_link_processing_stats (processing_phase, stat_type, stat_value)
  VALUES 
    (v_phase, 'categorylinks_processed', v_categorylinks_processed),
    (v_phase, 'categorylinks_batches', v_current_batch);
  
  -- ========================================
  -- PHASE 3: LINK TYPE AGGREGATION AND FINAL ASSEMBLY
  -- ========================================
  
  SET v_phase = 'Phase 3: Aggregation';
  
  INSERT INTO associative_build_state (state_key, state_value, state_text) 
  VALUES ('current_phase', 4, 'Aggregating link types and building final associative links')
  ON DUPLICATE KEY UPDATE state_value = 4, state_text = 'Aggregating link types and building final associative links';
  
  -- Build final associative links with proper type aggregation
  INSERT IGNORE INTO sass_associative_link (al_from_page_id, al_to_page_id, al_type)
  SELECT 
    sab.al_from_page_id,
    sab.al_to_page_id,
    CASE 
      WHEN COUNT(DISTINCT sab.al_type) > 1 THEN 'both'
      ELSE MIN(sab.al_type)
    END as al_type
  FROM sass_associative_buffer sab
  JOIN sass_page_clean spc_src ON sab.al_from_page_id = spc_src.page_id    -- Validate source is representative
  JOIN sass_page_clean spc_tgt ON sab.al_to_page_id = spc_tgt.page_id      -- Validate target is representative
  GROUP BY sab.al_from_page_id, sab.al_to_page_id;
  
  SET v_total_links_created = ROW_COUNT();
  
  -- Calculate consolidation statistics
  IF p_enable_consolidation_tracking = 1 THEN
    SELECT COUNT(*) INTO v_consolidations_detected
    FROM sass_representative_consolidation
    WHERE is_consolidated = 1;
  END IF;
  
  -- Count excluded self-links
  SELECT COUNT(*) INTO v_self_links_excluded
  FROM sass_associative_buffer sab
  WHERE sab.al_from_page_id = sab.al_to_page_id;
  
  -- Record final statistics
  INSERT INTO sass_link_processing_stats (processing_phase, stat_type, stat_value)
  VALUES 
    (v_phase, 'total_links_created', v_total_links_created),
    (v_phase, 'consolidations_detected', v_consolidations_detected),
    (v_phase, 'self_links_excluded', v_self_links_excluded);
  
  -- Update final build state
  INSERT INTO associative_build_state (state_key, state_value, state_text) 
  VALUES ('build_status', 100, 'Streaming associative build completed successfully')
  ON DUPLICATE KEY UPDATE state_value = 100, state_text = 'Streaming associative build completed successfully';
  
  INSERT INTO associative_build_state (state_key, state_value) 
  VALUES ('total_associative_links', v_total_links_created)
  ON DUPLICATE KEY UPDATE state_value = v_total_links_created;
  
  -- ========================================
  -- FINAL COMPREHENSIVE REPORT
  -- ========================================
  
  SELECT 
    'COMPLETE - Streaming Associative Build with Representatives' AS final_status,
    FORMAT(v_pagelinks_candidates, 0) AS pagelink_candidates,
    FORMAT(v_categorylinks_candidates, 0) AS categorylink_candidates,
    FORMAT(v_pagelinks_processed, 0) AS pagelinks_processed,
    FORMAT(v_categorylinks_processed, 0) AS categorylinks_processed,
    FORMAT(v_total_links_created, 0) AS total_associative_links,
    FORMAT(v_consolidations_detected, 0) AS representative_consolidations,
    FORMAT(v_self_links_excluded, 0) AS self_links_excluded,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_time_sec;
  
  -- Link type distribution
  SELECT 
    'Link Type Distribution' AS metric_type,
    sal.al_type AS link_type,
    FORMAT(COUNT(*), 0) AS link_count,
    CONCAT(ROUND(100.0 * COUNT(*) / v_total_links_created, 1), '%') AS percentage
  FROM sass_associative_link sal
  GROUP BY sal.al_type
  ORDER BY COUNT(*) DESC;
  
  -- Representative consolidation impact analysis
  IF p_enable_consolidation_tracking = 1 THEN
    SELECT 
      'Representative Consolidation Impact' AS analysis_type,
      link_type,
      FORMAT(COUNT(*), 0) AS original_relationships,
      FORMAT(SUM(is_consolidated), 0) AS relationships_consolidated,
      CONCAT(ROUND(100.0 * SUM(is_consolidated) / COUNT(*), 1), '%') AS consolidation_rate
    FROM sass_representative_consolidation
    GROUP BY link_type
    ORDER BY link_type;
  END IF;
  
  -- Network connectivity metrics
  SELECT 
    'Network Connectivity Metrics' AS metric_type,
    FORMAT(COUNT(DISTINCT sal.al_from_page_id), 0) AS unique_source_representatives,
    FORMAT(COUNT(DISTINCT sal.al_to_page_id), 0) AS unique_target_representatives,
    FORMAT(COUNT(DISTINCT sal.al_from_page_id) + COUNT(DISTINCT sal.al_to_page_id), 0) AS total_connected_representatives,
    ROUND(COUNT(*) / COUNT(DISTINCT sal.al_from_page_id), 1) AS avg_outbound_links,
    ROUND(COUNT(*) / COUNT(DISTINCT sal.al_to_page_id), 1) AS avg_inbound_links
  FROM sass_associative_link sal;
  
  -- Sample representative relationships
  SELECT 
    'Sample Representative Relationships' AS sample_type,
    CONVERT(sp_from.page_title, CHAR) AS source_representative,
    CONVERT(sp_to.page_title, CHAR) AS target_representative,
    sal.al_type AS relationship_type,
    sp_from.page_dag_level AS source_level,
    sp_to.page_dag_level AS target_level,
    CASE WHEN sp_from.page_is_leaf = 1 THEN 'Article' ELSE 'Category' END AS source_type,
    CASE WHEN sp_to.page_is_leaf = 1 THEN 'Article' ELSE 'Category' END AS target_type
  FROM sass_associative_link sal
  JOIN sass_page_clean sp_from ON sal.al_from_page_id = sp_from.page_id
  JOIN sass_page_clean sp_to ON sal.al_to_page_id = sp_to.page_id
  ORDER BY RAND()
  LIMIT 15;
  
END//

DELIMITER ;

-- ========================================
-- UTILITY PROCEDURES
-- ========================================

-- Procedure to analyze representative consolidation patterns
DROP PROCEDURE IF EXISTS AnalyzeRepresentativeConsolidation;

DELIMITER //

CREATE PROCEDURE AnalyzeRepresentativeConsolidation()
BEGIN
  -- Consolidation rate by link type
  SELECT 
    'Consolidation Analysis by Link Type' AS analysis_type,
    link_type,
    FORMAT(COUNT(*), 0) AS total_original_links,
    FORMAT(SUM(is_consolidated), 0) AS consolidated_links,
    FORMAT(COUNT(*) - SUM(is_consolidated), 0) AS direct_representative_links,
    CONCAT(ROUND(100.0 * SUM(is_consolidated) / COUNT(*), 1), '%') AS consolidation_rate
  FROM sass_representative_consolidation
  GROUP BY link_type;
  
  -- Most consolidated representative pairs
  SELECT 
    'Most Consolidated Representative Pairs' AS analysis_type,
    CONVERT(sp_src.page_title, CHAR) AS source_representative,
    CONVERT(sp_tgt.page_title, CHAR) AS target_representative,
    COUNT(*) AS original_link_variations,
    COUNT(DISTINCT src.link_type) AS link_types_present
  FROM sass_representative_consolidation src
  JOIN sass_page_clean sp_src ON src.representative_source_id = sp_src.page_id
  JOIN sass_page_clean sp_tgt ON src.representative_target_id = sp_tgt.page_id
  WHERE src.is_consolidated = 1
  GROUP BY src.representative_source_id, src.representative_target_id
  ORDER BY COUNT(*) DESC
  LIMIT 20;
  
  -- Source page consolidation distribution
  SELECT 
    'Source Page Consolidation Distribution' AS analysis_type,
    consolidations_per_representative,
    COUNT(*) AS representative_count
  FROM (
    SELECT 
      representative_source_id,
      COUNT(*) AS consolidations_per_representative
    FROM sass_representative_consolidation
    WHERE is_consolidated = 1
    GROUP BY representative_source_id
  ) consolidation_stats
  GROUP BY consolidations_per_representative
  ORDER BY consolidations_per_representative DESC
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
  
  -- Find the representative page ID
  SELECT page_id INTO v_page_id
  FROM sass_page_clean 
  WHERE CONVERT(page_title, CHAR) = p_page_title
  LIMIT 1;
  
  IF v_page_id IS NULL THEN
    SELECT CONCAT('Representative page not found: ', p_page_title) AS error_message;
  ELSE
    -- Show outbound associative links
    SELECT 
      'Outbound Associative Links from Representative' AS search_type,
      CONVERT(sp_to.page_title, CHAR) AS linked_to_representative,
      sal.al_type AS relationship_type,
      sp_to.page_dag_level AS target_level,
      CASE WHEN sp_to.page_is_leaf = 1 THEN 'Article' ELSE 'Category' END AS target_type
    FROM sass_associative_link sal
    JOIN sass_page_clean sp_to ON sal.al_to_page_id = sp_to.page_id
    WHERE sal.al_from_page_id = v_page_id
      AND (p_link_type IS NULL OR sal.al_type = p_link_type)
    ORDER BY sal.al_type, sp_to.page_title
    LIMIT 25;
    
    -- Show inbound associative links
    SELECT 
      'Inbound Associative Links to Representative' AS search_type,
      CONVERT(sp_from.page_title, CHAR) AS linked_from_representative,
      sal.al_type AS relationship_type,
      sp_from.page_dag_level AS source_level,
      CASE WHEN sp_from.page_is_leaf = 1 THEN 'Article' ELSE 'Category' END AS source_type
    FROM sass_associative_link sal
    JOIN sass_page_clean sp_from ON sal.al_from_page_id = sp_from.page_id
    WHERE sal.al_to_page_id = v_page_id
      AND (p_link_type IS NULL OR sal.al_type = p_link_type)
    ORDER BY sal.al_type, sp_from.page_title
    LIMIT 25;
    
    -- Show identity group information
    SELECT 
      'Identity Group Information' AS info_type,
      COUNT(*) AS total_pages_in_identity_group,
      COUNT(CASE WHEN sip.page_id = sip.representative_page_id THEN 1 END) AS is_representative_itself,
      GROUP_CONCAT(DISTINCT CASE WHEN sip.page_is_leaf = 1 THEN 'Articles' ELSE 'Categories' END) AS page_types_in_group
    FROM sass_identity_pages sip
    WHERE sip.representative_page_id = v_page_id;
  END IF;

END//

DELIMITER ;

-- Procedure to validate associative link integrity with representatives
DROP PROCEDURE IF EXISTS ValidateAssociativeLinkIntegrityWithRepresentatives;

DELIMITER //

CREATE PROCEDURE ValidateAssociativeLinkIntegrityWithRepresentatives()
BEGIN
  DECLARE v_orphaned_sources INT DEFAULT 0;
  DECLARE v_orphaned_targets INT DEFAULT 0;
  DECLARE v_non_representative_sources INT DEFAULT 0;
  DECLARE v_non_representative_targets INT DEFAULT 0;
  DECLARE v_self_links INT DEFAULT 0;
  DECLARE v_invalid_types INT DEFAULT 0;
  
  -- Check for orphaned source pages
  SELECT COUNT(*) INTO v_orphaned_sources
  FROM sass_associative_link sal
  WHERE NOT EXISTS (
    SELECT 1 FROM sass_page_clean spc WHERE spc.page_id = sal.al_from_page_id
  );
  
  -- Check for orphaned target pages
  SELECT COUNT(*) INTO v_orphaned_targets
  FROM sass_associative_link sal
  WHERE NOT EXISTS (
    SELECT 1 FROM sass_page_clean spc WHERE spc.page_id = sal.al_to_page_id
  );
  
  -- Check for sources that are not representatives
  SELECT COUNT(*) INTO v_non_representative_sources
  FROM sass_associative_link sal
  JOIN sass_identity_pages sip ON sal.al_from_page_id = sip.page_id
  WHERE sip.page_id != sip.representative_page_id;
  
  -- Check for targets that are not representatives
  SELECT COUNT(*) INTO v_non_representative_targets
  FROM sass_associative_link sal
  JOIN sass_identity_pages sip ON sal.al_to_page_id = sip.page_id
  WHERE sip.page_id != sip.representative_page_id;
  
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
    'Associative Link Representative Integrity Validation' AS validation_type,
    v_orphaned_sources AS orphaned_sources,
    v_orphaned_targets AS orphaned_targets,
    v_non_representative_sources AS non_representative_sources,
    v_non_representative_targets AS non_representative_targets,
    v_self_links AS self_links,
    v_invalid_types AS invalid_types,
    CASE 
      WHEN v_orphaned_sources = 0 AND v_orphaned_targets = 0 AND 
           v_non_representative_sources = 0 AND v_non_representative_targets = 0 AND 
           v_self_links = 0 AND v_invalid_types = 0 
      THEN 'PASS' 
      ELSE 'FAIL' 
    END AS validation_status;

END//

DELIMITER ;

-- ========================================
-- USAGE EXAMPLES AND DOCUMENTATION
-- ========================================

/*
-- STREAMING ASSOCIATIVE BUILD WITH REPRESENTATIVE RESOLUTION

-- Standard build with consolidation tracking:
CALL BuildSASSAssociativeLinksStreaming(1000000, 1, 1);

-- High-performance build (no consolidation tracking):
CALL BuildSASSAssociativeLinksStreaming(2000000, 1, 0);

-- Silent build for production:
CALL BuildSASSAssociativeLinksStreaming(1500000, 0, 0);

-- Analyze representative consolidation patterns:
CALL AnalyzeRepresentativeConsolidation();

-- Test associative search:
CALL TestAssociativeSearch('Machine_learning', NULL);
CALL TestAssociativeSearch('Artificial_intelligence', 'pagelink');
CALL TestAssociativeSearch('Computer_science', 'both');

-- Validate data integrity:
CALL ValidateAssociativeLinkIntegrityWithRepresentatives();

-- Check build status and statistics:
SELECT * FROM associative_build_state ORDER BY updated_at DESC;
SELECT * FROM sass_link_processing_stats ORDER BY created_at DESC;

-- Sample queries on representative-resolved associative links:

-- Find all representatives linked to "Machine Learning":
SELECT 
  CONVERT(sp_from.page_title, CHAR) as source_representative,
  sal.al_type as relationship_type,
  sp_from.page_dag_level as source_level,
  CASE WHEN sp_from.page_is_leaf = 1 THEN 'Article' ELSE 'Category' END as source_type
FROM sass_associative_link sal
JOIN sass_page_clean sp_from ON sal.al_from_page_id = sp_from.page_id
JOIN sass_page_clean sp_to ON sal.al_to_page_id = sp_to.page_id
WHERE CONVERT(sp_to.page_title, CHAR) = 'Machine_learning'
ORDER BY sal.al_type, source_representative;

-- Most interconnected representative categories:
SELECT 
  CONVERT(sp.page_title, CHAR) as representative_title,
  COUNT(DISTINCT sal_out.al_to_page_id) as outbound_links,
  COUNT(DISTINCT sal_in.al_from_page_id) as inbound_links,
  COUNT(DISTINCT sal_out.al_to_page_id) + COUNT(DISTINCT sal_in.al_from_page_id) as total_connections
FROM sass_page_clean sp
LEFT JOIN sass_associative_link sal_out ON sp.page_id = sal_out.al_from_page_id
LEFT JOIN sass_associative_link sal_in ON sp.page_id = sal_in.al_to_page_id
WHERE sp.page_is_leaf = 0
GROUP BY sp.page_id
ORDER BY total_connections DESC
LIMIT 20;

-- Cross-domain knowledge bridges between representatives:
SELECT 
  CONVERT(sp_from.page_title, CHAR) as source_representative,
  CONVERT(sp_to.page_title, CHAR) as target_representative,
  sal.al_type,
  sp_from.page_root_id as source_domain,
  sp_to.page_root_id as target_domain
FROM sass_associative_link sal
JOIN sass_page_clean sp_from ON sal.al_from_page_id = sp_from.page_id
JOIN sass_page_clean sp_to ON sal.al_to_page_id = sp_to.page_id
WHERE sp_from.page_root_id != sp_to.page_root_id
  AND sp_from.page_is_leaf = 1 
  AND sp_to.page_is_leaf = 1
ORDER BY RAND()
LIMIT 20;

STREAMING ARCHITECTURE BENEFITS:
- Memory-efficient processing of 1.5B+ pagelinks through batched streaming
- Immediate representative resolution prevents large intermediate storage
- Aggressive pre-filtering reduces processing by ~95% before representative resolution
- Link type aggregation ensures correct 'both' classification for dual relationships
- Comprehensive consolidation tracking shows impact of representative mapping

PERFORMANCE OPTIMIZATIONS:
- Batch size of 1-2M optimal for Mac Studio M4 memory architecture
- Pre-filtering eliminates majority of non-SASS relationships early
- Streaming prevents memory exhaustion on large pagelinks table
- Representative validation ensures all links point to canonical pages
- Progressive reporting allows monitoring of long-running operations

ESTIMATED RUNTIME ON MAC STUDIO M4:
- Pre-filtering phase: 15-30 minutes
- Pagelinks streaming: 60-90 minutes  
- Categorylinks streaming: 30-45 minutes
- Aggregation phase: 10-20 minutes
- Total estimated time: 2-3 hours

QUALITY IMPROVEMENTS:
- All associative relationships use canonical representative pages
- Eliminates duplicate relationships from title variations
- Maintains proper link type classification through aggregation
- Comprehensive validation ensures referential integrity
- Consolidation tracking provides transparency into representative mapping impact
*/
