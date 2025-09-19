-- SASS Wikipedia Lexical Link Builder - Phase 2
-- Builds semantic equivalence mapping for alternative page titles
-- Implements comprehensive chain resolution for redirect paths
-- Maps redirects to canonical SASS pages for lexical search

-- ========================================
-- TABLE DEFINITIONS
-- ========================================

-- Main lexical link table matching schemas.md specification
CREATE TABLE IF NOT EXISTS sass_lexical_link (
  ll_from_title VARBINARY(255) NOT NULL,
  ll_to_page_id INT(8) UNSIGNED NOT NULL,
  ll_to_fragment VARBINARY(255) NULL,
  
  PRIMARY KEY (ll_from_title, ll_to_page_id),
  INDEX idx_from_title (ll_from_title),
  INDEX idx_to_page (ll_to_page_id),
  INDEX idx_fragment (ll_to_fragment)
) ENGINE=InnoDB;

-- Temporary redirect resolution working table
CREATE TABLE IF NOT EXISTS sass_redirect_work (
  rd_from INT UNSIGNED NOT NULL,
  rd_from_title VARBINARY(255) NOT NULL,
  rd_to_page_id INT UNSIGNED NULL,
  rd_to_fragment VARBINARY(255) NULL,
  resolution_level INT NOT NULL DEFAULT 0,
  is_resolved TINYINT(1) NOT NULL DEFAULT 0,
  is_sass_target TINYINT(1) NOT NULL DEFAULT 0,
  
  PRIMARY KEY (rd_from),
  INDEX idx_resolution_level (resolution_level),
  INDEX idx_resolved (is_resolved),
  INDEX idx_sass_target (is_sass_target),
  INDEX idx_to_page (rd_to_page_id)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED;

-- Redirect chain tracking for cycle detection
CREATE TABLE IF NOT EXISTS sass_redirect_chains (
  chain_id INT AUTO_INCREMENT PRIMARY KEY,
  rd_from INT UNSIGNED NOT NULL,
  rd_to INT UNSIGNED NOT NULL,
  chain_length INT NOT NULL,
  detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_from (rd_from),
  INDEX idx_to (rd_to),
  INDEX idx_length (chain_length)
) ENGINE=InnoDB;

-- Build progress tracking
CREATE TABLE IF NOT EXISTS lexical_build_state (
  state_key VARCHAR(255) PRIMARY KEY,
  state_value INT NOT NULL,
  state_text VARCHAR(500) NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- ========================================
-- SIMPLIFIED BUILD PROCEDURE (MAIN)
-- ========================================

DROP PROCEDURE IF EXISTS BuildSASSLexicalLinks;

DELIMITER //

CREATE PROCEDURE BuildSASSLexicalLinks(
  IN p_max_chain_depth INT,
  IN p_enable_cycle_detection TINYINT(1)
)
BEGIN
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_direct_links INT DEFAULT 0;
  DECLARE v_chain_links INT DEFAULT 0;
  DECLARE v_total_processed INT DEFAULT 0;
  
  -- Set defaults
  IF p_max_chain_depth IS NULL THEN SET p_max_chain_depth = 5; END IF;
  IF p_enable_cycle_detection IS NULL THEN SET p_enable_cycle_detection = 1; END IF;
  
  SET v_start_time = UNIX_TIMESTAMP();
  
  -- Clear target table
  TRUNCATE TABLE sass_lexical_link;
  TRUNCATE TABLE sass_redirect_chains;
  
  -- Initialize build state
  INSERT INTO lexical_build_state (state_key, state_value, state_text) 
  VALUES ('build_status', 0, 'Starting lexical link build')
  ON DUPLICATE KEY UPDATE state_value = 0, state_text = 'Starting lexical link build';
  
  -- ========================================
  -- DIRECT REDIRECTS (Level 0)
  -- ========================================
  
  INSERT IGNORE INTO sass_lexical_link (ll_from_title, ll_to_page_id, ll_to_fragment)
  SELECT DISTINCT
    p_from.page_title as ll_from_title,
    p_target.page_id as ll_to_page_id,
    r.rd_fragment as ll_to_fragment
  FROM redirect r
  JOIN page p_from ON r.rd_from = p_from.page_id
  JOIN sass_page sp_from ON p_from.page_id = sp_from.page_id  -- Source must be in SASS
  JOIN page p_target ON p_target.page_title = r.rd_title 
    AND p_target.page_namespace = r.rd_namespace
  JOIN sass_page sp_target ON p_target.page_id = sp_target.page_id  -- Target must be in SASS
  WHERE r.rd_interwiki IS NULL
    AND p_from.page_content_model = 'wikitext'
    AND p_target.page_content_model = 'wikitext'
    AND r.rd_namespace IN (0, 14);
  
  SET v_direct_links = ROW_COUNT();
  
  -- Progress report for direct redirects
  SELECT 
    'Level 0: Direct Redirects' AS status,
    FORMAT(v_direct_links, 0) AS direct_links_created,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS elapsed_sec;
  
  -- ========================================
  -- REDIRECT CHAINS (Levels 1+)
  -- ========================================
  
  IF p_max_chain_depth > 1 THEN
    
    -- Build redirect chains using recursive approach
    INSERT IGNORE INTO sass_lexical_link (ll_from_title, ll_to_page_id, ll_to_fragment)
    WITH RECURSIVE redirect_chain AS (
      -- Base case: direct redirects from SASS pages
      SELECT 
        p_from.page_title as source_title,
        p_target.page_id as target_page_id,
        COALESCE(r.rd_fragment, '') as fragment,
        r.rd_from as chain_start,
        p_target.page_id as chain_end,
        1 as chain_level
      FROM redirect r
      JOIN page p_from ON r.rd_from = p_from.page_id
      JOIN sass_page sp_from ON p_from.page_id = sp_from.page_id
      JOIN page p_target ON p_target.page_title = r.rd_title 
        AND p_target.page_namespace = r.rd_namespace
      WHERE r.rd_interwiki IS NULL
        AND p_from.page_content_model = 'wikitext'
        AND p_target.page_content_model = 'wikitext'
        AND r.rd_namespace IN (0, 14)
        AND p_target.page_is_redirect = 1  -- Target is also a redirect
        
      UNION ALL
      
      -- Recursive case: follow redirect chains
      SELECT 
        rc.source_title,
        p_final.page_id as target_page_id,
        COALESCE(r2.rd_fragment, rc.fragment) as fragment,
        rc.chain_start,
        p_final.page_id as chain_end,
        rc.chain_level + 1
      FROM redirect_chain rc
      JOIN redirect r2 ON rc.chain_end = r2.rd_from
      JOIN page p_final ON p_final.page_title = r2.rd_title 
        AND p_final.page_namespace = r2.rd_namespace
      WHERE rc.chain_level < p_max_chain_depth
        AND r2.rd_interwiki IS NULL
        AND p_final.page_content_model = 'wikitext'
        AND r2.rd_namespace IN (0, 14)
        AND (p_enable_cycle_detection = 0 OR rc.chain_start != p_final.page_id)  -- Cycle prevention
    )
    SELECT DISTINCT
      rc.source_title as ll_from_title,
      rc.target_page_id as ll_to_page_id,
      NULLIF(rc.fragment, '') as ll_to_fragment
    FROM redirect_chain rc
    JOIN sass_page sp_target ON rc.target_page_id = sp_target.page_id  -- Final target must be in SASS
    WHERE NOT EXISTS (
      SELECT 1 FROM sass_lexical_link sll 
      WHERE sll.ll_from_title = rc.source_title 
        AND sll.ll_to_page_id = rc.target_page_id
    );
    
    SET v_chain_links = ROW_COUNT();
    
    -- Progress report for chain resolution
    SELECT 
      CONCAT('Levels 1-', p_max_chain_depth, ': Chain Resolution') AS status,
      FORMAT(v_chain_links, 0) AS chain_links_created,
      ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS elapsed_sec;
    
  END IF;
  
  SET v_total_processed = v_direct_links + v_chain_links;
  
  -- Update final build state
  INSERT INTO lexical_build_state (state_key, state_value, state_text) 
  VALUES ('build_status', 100, 'Build completed successfully')
  ON DUPLICATE KEY UPDATE state_value = 100, state_text = 'Build completed successfully';
  
  INSERT INTO lexical_build_state (state_key, state_value) 
  VALUES ('total_lexical_links', v_total_processed)
  ON DUPLICATE KEY UPDATE state_value = v_total_processed;
  
  -- ========================================
  -- FINAL SUMMARY REPORT
  -- ========================================
  
  SELECT 
    'COMPLETE - Lexical Link Resolution' AS final_status,
    p_max_chain_depth AS max_chain_depth_used,
    FORMAT(v_direct_links, 0) AS direct_redirects,
    FORMAT(v_chain_links, 0) AS chain_redirects,
    FORMAT(v_total_processed, 0) AS total_lexical_links,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_time_sec;
  
  -- Lexical link statistics
  SELECT 
    'Lexical Link Quality Metrics' AS metric_type,
    FORMAT(COUNT(*), 0) AS total_links,
    FORMAT(COUNT(DISTINCT ll_from_title), 0) AS unique_source_titles,
    FORMAT(COUNT(DISTINCT ll_to_page_id), 0) AS unique_target_pages,
    FORMAT(SUM(CASE WHEN ll_to_fragment IS NOT NULL THEN 1 ELSE 0 END), 0) AS links_with_fragments,
    CONCAT(ROUND(100.0 * SUM(CASE WHEN ll_to_fragment IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1), '%') AS fragment_rate
  FROM sass_lexical_link;
  
  -- Sample lexical mappings
  SELECT 
    'Sample Lexical Mappings' AS sample_type,
    CONVERT(sll.ll_from_title, CHAR) AS redirect_title,
    CONVERT(sp.page_title, CHAR) AS target_title,
    sp.page_dag_level AS target_level,
    CASE WHEN sp.page_is_leaf = 1 THEN 'Article' ELSE 'Category' END AS target_type,
    CONVERT(sll.ll_to_fragment, CHAR) AS fragment
  FROM sass_lexical_link sll
  JOIN sass_page sp ON sll.ll_to_page_id = sp.page_id
  ORDER BY RAND()
  LIMIT 15;
  
END//

DELIMITER ;

-- ========================================
-- SIMPLE BUILD PROCEDURE (FALLBACK)
-- ========================================

DROP PROCEDURE IF EXISTS BuildSASSLexicalLinksSimple;

DELIMITER //

CREATE PROCEDURE BuildSASSLexicalLinksSimple()
BEGIN
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_total_processed INT DEFAULT 0;
  
  SET v_start_time = UNIX_TIMESTAMP();
  
  -- Clear target table
  TRUNCATE TABLE sass_lexical_link;
  
  -- Direct approach: resolve redirects in single query
  INSERT IGNORE INTO sass_lexical_link (ll_from_title, ll_to_page_id, ll_to_fragment)
  SELECT DISTINCT
    p_from.page_title as ll_from_title,
    p_target.page_id as ll_to_page_id,
    r.rd_fragment as ll_to_fragment
  FROM redirect r
  JOIN page p_from ON r.rd_from = p_from.page_id
  JOIN sass_page sp_from ON p_from.page_id = sp_from.page_id  -- Source must be in SASS
  JOIN page p_target ON p_target.page_title = r.rd_title 
    AND p_target.page_namespace = r.rd_namespace
  JOIN sass_page sp_target ON p_target.page_id = sp_target.page_id  -- Target must be in SASS
  WHERE r.rd_interwiki IS NULL
    AND p_from.page_content_model = 'wikitext'
    AND p_target.page_content_model = 'wikitext'
    AND r.rd_namespace IN (0, 14);
  
  SET v_total_processed = ROW_COUNT();
  
  -- Summary report
  SELECT 
    'Simple Lexical Build Complete' AS status,
    FORMAT(v_total_processed, 0) AS lexical_links_created,
    ROUND(UNIX_TIMESTAMP() - v_start_time, 2) AS total_time_sec;
  
  -- Sample results
  SELECT 
    CONVERT(ll_from_title, CHAR) AS redirect_from,
    CONVERT(sp.page_title, CHAR) AS target_title,
    sp.page_dag_level AS level,
    CASE WHEN sp.page_is_leaf = 1 THEN 'Article' ELSE 'Category' END AS type
  FROM sass_lexical_link sll
  JOIN sass_page sp ON sll.ll_to_page_id = sp.page_id
  LIMIT 10;

END//

DELIMITER ;

-- ========================================
-- UTILITY PROCEDURES
-- ========================================

-- Procedure to analyze redirect chain patterns
DROP PROCEDURE IF EXISTS AnalyzeLexicalPatterns;

DELIMITER //

CREATE PROCEDURE AnalyzeLexicalPatterns()
BEGIN
  -- Common redirect patterns analysis
  SELECT 
    'Common Redirect Patterns' AS analysis_type,
    'Acronyms' AS pattern_type,
    COUNT(*) AS pattern_count
  FROM sass_lexical_link sll
  JOIN sass_page sp ON sll.ll_to_page_id = sp.page_id
  WHERE CONVERT(sll.ll_from_title, CHAR) REGEXP '^[A-Z]{2,}$'
  
  UNION ALL
  
  SELECT 
    'Common Redirect Patterns',
    'Abbreviations',
    COUNT(*)
  FROM sass_lexical_link sll
  JOIN sass_page sp ON sll.ll_to_page_id = sp.page_id
  WHERE CONVERT(sll.ll_from_title, CHAR) LIKE '%.%'
  
  UNION ALL
  
  SELECT 
    'Common Redirect Patterns',
    'Alternative Spellings',
    COUNT(*)
  FROM sass_lexical_link sll
  JOIN sass_page sp ON sll.ll_to_page_id = sp.page_id
  WHERE CONVERT(sll.ll_from_title, CHAR) REGEXP '(ise|ize|our|or)$'
  
  UNION ALL
  
  SELECT 
    'Common Redirect Patterns',
    'Plural Forms',
    COUNT(*)
  FROM sass_lexical_link sll
  JOIN sass_page sp ON sll.ll_to_page_id = sp.page_id
  WHERE CONVERT(sll.ll_from_title, CHAR) LIKE '%s'
    AND CONVERT(sp.page_title, CHAR) = LEFT(CONVERT(sll.ll_from_title, CHAR), LENGTH(CONVERT(sll.ll_from_title, CHAR)) - 1);
  
  -- Top target pages by redirect volume
  SELECT 
    'Top Redirect Targets' AS analysis_type,
    CONVERT(sp.page_title, CHAR) AS target_title,
    COUNT(*) AS redirect_count,
    sp.page_dag_level AS target_level,
    CASE WHEN sp.page_is_leaf = 1 THEN 'Article' ELSE 'Category' END AS target_type
  FROM sass_lexical_link sll
  JOIN sass_page sp ON sll.ll_to_page_id = sp.page_id
  GROUP BY sll.ll_to_page_id
  ORDER BY COUNT(*) DESC
  LIMIT 10;

END//

DELIMITER ;

-- Procedure to test lexical search functionality
DROP PROCEDURE IF EXISTS TestLexicalSearch;

DELIMITER //

CREATE PROCEDURE TestLexicalSearch(
  IN p_search_term VARCHAR(255)
)
BEGIN
  -- Direct lexical search
  SELECT 
    'Lexical Search Results' AS search_type,
    CONVERT(sll.ll_from_title, CHAR) AS redirect_from,
    CONVERT(sp.page_title, CHAR) AS target_title,
    sp.page_dag_level AS target_level,
    CASE WHEN sp.page_is_leaf = 1 THEN 'Article' ELSE 'Category' END AS target_type,
    CONVERT(sll.ll_to_fragment, CHAR) AS fragment
  FROM sass_lexical_link sll
  JOIN sass_page sp ON sll.ll_to_page_id = sp.page_id
  WHERE CONVERT(sll.ll_from_title, CHAR) LIKE CONCAT('%', p_search_term, '%')
  ORDER BY 
    CASE WHEN CONVERT(sll.ll_from_title, CHAR) = p_search_term THEN 1 ELSE 2 END,
    LENGTH(CONVERT(sll.ll_from_title, CHAR)),
    CONVERT(sll.ll_from_title, CHAR)
  LIMIT 20;

END//

DELIMITER ;

-- ========================================
-- USAGE EXAMPLES AND DOCUMENTATION
-- ========================================

/*
-- LEXICAL LINK BUILD EXAMPLES

-- Standard build with chain resolution:
CALL BuildSASSLexicalLinks(5, 1);

-- Simple build (direct redirects only):
CALL BuildSASSLexicalLinksSimple();

-- Extended chain resolution (up to 10 levels):
CALL BuildSASSLexicalLinks(10, 1);

-- Build without cycle detection (faster):
CALL BuildSASSLexicalLinks(5, 0);

-- Analyze lexical patterns:
CALL AnalyzeLexicalPatterns();

-- Test lexical search:
CALL TestLexicalSearch('ML');
CALL TestLexicalSearch('AI');
CALL TestLexicalSearch('machine');

-- Check build status:
SELECT * FROM lexical_build_state ORDER BY updated_at DESC;

-- Sample queries on final data:

-- Find all ways to reference "Machine Learning":
SELECT 
  CONVERT(ll_from_title, CHAR) as redirect_title,
  CONVERT(ll_to_fragment, CHAR) as section
FROM sass_lexical_link sll
JOIN sass_page sp ON sll.ll_to_page_id = sp.page_id
WHERE CONVERT(sp.page_title, CHAR) = 'Machine_learning'
ORDER BY redirect_title;

-- Popular acronyms in SASS:
SELECT 
  CONVERT(ll_from_title, CHAR) as acronym,
  CONVERT(sp.page_title, CHAR) as full_title,
  sp.page_dag_level
FROM sass_lexical_link sll
JOIN sass_page sp ON sll.ll_to_page_id = sp.page_id
WHERE CONVERT(ll_from_title, CHAR) REGEXP '^[A-Z]{2,}$'
ORDER BY acronym;

PERFORMANCE NOTES:
- Simple build handles direct redirects only (fastest)
- Full build uses recursive CTE for chain resolution
- Both versions enforce SASS-only constraints on source and target
- Chain resolution limited by p_max_chain_depth parameter

QUALITY METRICS:
- Only SASS pages included in both ll_from_title and ll_to_page_id
- Fragment preservation maintains section-specific redirects
- Deduplication ensures single canonical mapping per redirect title
- Cycle detection prevents infinite loops in redirect chains
*/
