-- Diagnostic for sass_lexical_link build issues

-- Check if functions were created successfully
SHOW FUNCTION STATUS WHERE Name IN ('resolve_redirect_target', 'is_sass_page');

-- Check if tables exist
SHOW TABLES LIKE 'sass_%';

-- Simplified procedure without function dependencies
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

-- Test the simple procedure
SELECT 'Run: CALL BuildSASSLexicalLinksSimple();' AS next_step;
