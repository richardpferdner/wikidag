-- Optimized BSTEM Category Tree Builder (Idempotent)
-- Key improvements: LEFT JOIN anti-pattern, dynamic batching, transaction management

-- Create main table with optimized indexing (idempotent)
CREATE TABLE IF NOT EXISTS bstem_category_dag (
  page_id INT NOT NULL,
  page_title VARCHAR(255) NOT NULL,
  page_namespace INT NOT NULL,
  level INT NOT NULL,
  root_category VARCHAR(255) NOT NULL,
  is_leaf BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  PRIMARY KEY (page_id),
  INDEX idx_root_level (root_category, level),
  INDEX idx_level_leaf (level, is_leaf),
  INDEX idx_namespace (page_namespace),
  UNIQUE KEY uk_page_root (page_id, root_category)
) ENGINE=InnoDB;

-- Enhanced indexes on existing tables (idempotent)
CREATE INDEX IF NOT EXISTS idx_target_type ON categorylinks (cl_target_id, cl_type);
CREATE INDEX IF NOT EXISTS idx_namespace_title ON page (page_namespace, page_title);

-- Progress tracking with performance metrics (idempotent)
CREATE TABLE IF NOT EXISTS build_progress (
  iteration INT AUTO_INCREMENT PRIMARY KEY,
  root_category VARCHAR(255),
  start_level INT,
  end_level INT,
  batch_size INT,
  rows_added INT,
  execution_time_sec DECIMAL(10,3),
  avg_rows_per_sec DECIMAL(10,0),
  memory_usage_mb DECIMAL(10,2),
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Error handling table (idempotent)
CREATE TABLE IF NOT EXISTS build_errors (
  error_id INT AUTO_INCREMENT PRIMARY KEY,
  iteration INT,
  error_message TEXT,
  sql_state VARCHAR(5),
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Drop existing procedure before recreating (idempotent)
DROP PROCEDURE IF EXISTS BuildBSTEMTree;

-- Stored procedure for dynamic batch processing
DELIMITER $$
CREATE PROCEDURE BuildBSTEMTree(
  IN p_start_level INT DEFAULT 0,
  IN p_max_level INT DEFAULT 12,
  IN p_initial_batch_size INT DEFAULT 50000,
  IN p_target_rows_per_sec INT DEFAULT 100000
)
BEGIN
  DECLARE v_current_level INT DEFAULT p_start_level;
  DECLARE v_batch_size INT DEFAULT p_initial_batch_size;
  DECLARE v_rows_added INT DEFAULT 0;
  DECLARE v_start_time DECIMAL(14,3);
  DECLARE v_execution_time DECIMAL(10,3);
  DECLARE v_rows_per_sec DECIMAL(10,0);
  DECLARE v_continue BOOLEAN DEFAULT TRUE;
  DECLARE v_iteration INT DEFAULT 0;
  
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1
      @error_message = MESSAGE_TEXT,
      @sql_state = RETURNED_SQLSTATE;
    
    INSERT INTO build_errors (iteration, error_message, sql_state)
    VALUES (v_iteration, @error_message, @sql_state);
    
    ROLLBACK;
    RESIGNAL;
  END;

  -- Initialize root categories if empty (idempotent)
  INSERT IGNORE INTO bstem_category_dag (page_id, page_title, page_namespace, level, root_category, is_leaf)
  SELECT page_id, page_title, page_namespace, 0, page_title, FALSE
  FROM page 
  WHERE page_namespace = 14 
  AND page_title IN ('Business','Science','Technology','Engineering','Mathematics');

  -- Main processing loop
  WHILE v_current_level < p_max_level AND v_continue DO
    SET v_iteration = v_iteration + 1;
    SET v_start_time = UNIX_TIMESTAMP(3);
    
    START TRANSACTION;
    
    -- Create temporary table for current level processing
    DROP TEMPORARY TABLE IF EXISTS temp_current_level;
    CREATE TEMPORARY TABLE temp_current_level (
      page_id INT NOT NULL,
      root_category VARCHAR(255) NOT NULL,
      level INT NOT NULL,
      PRIMARY KEY (page_id),
      INDEX idx_root (root_category)
    ) ENGINE=MEMORY;
    
    -- Populate with categories to expand
    INSERT INTO temp_current_level
    SELECT page_id, root_category, level 
    FROM bstem_category_dag 
    WHERE level = v_current_level AND is_leaf = FALSE;
    
    -- Exit if no more categories to process
    IF ROW_COUNT() = 0 THEN
      SET v_continue = FALSE;
    ELSE
      -- Process next level using LEFT JOIN anti-pattern
      INSERT IGNORE INTO bstem_category_dag 
        (page_id, page_title, page_namespace, level, root_category, is_leaf)
      SELECT DISTINCT 
        p.page_id, 
        p.page_title, 
        p.page_namespace,
        tcl.level + 1,
        tcl.root_category,
        (p.page_namespace != 14) as is_leaf
      FROM temp_current_level tcl
      JOIN categorylinks cl ON tcl.page_id = cl.cl_target_id
      JOIN page p ON cl.cl_from = p.page_id
      LEFT JOIN bstem_category_dag existing ON p.page_id = existing.page_id
      WHERE cl.cl_type IN ('page', 'subcat')  -- Exclude files as per overview.md
        AND existing.page_id IS NULL  -- Anti-join pattern instead of NOT IN
        AND p.page_namespace IN (0, 14)  -- Articles and categories only
      LIMIT v_batch_size;
      
      SET v_rows_added = ROW_COUNT();
      SET v_execution_time = UNIX_TIMESTAMP(3) - v_start_time;
      SET v_rows_per_sec = CASE 
        WHEN v_execution_time > 0 THEN v_rows_added / v_execution_time 
        ELSE v_rows_added 
      END;
      
      -- Dynamic batch size adjustment
      IF v_rows_per_sec < p_target_rows_per_sec * 0.8 THEN
        SET v_batch_size = GREATEST(v_batch_size * 0.8, 10000);
      ELSEIF v_rows_per_sec > p_target_rows_per_sec * 1.2 THEN
        SET v_batch_size = LEAST(v_batch_size * 1.2, 500000);
      END IF;
      
      -- Log progress
      INSERT INTO build_progress 
        (root_category, start_level, end_level, batch_size, rows_added, 
         execution_time_sec, avg_rows_per_sec, memory_usage_mb)
      SELECT 'ALL', v_current_level, v_current_level + 1, v_batch_size, 
             v_rows_added, v_execution_time, v_rows_per_sec,
             (SELECT ROUND(SUM(DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2)
              FROM information_schema.TABLES 
              WHERE TABLE_NAME = 'bstem_category_dag');
    END IF;
    
    DROP TEMPORARY TABLE temp_current_level;
    COMMIT;
    
    SET v_current_level = v_current_level + 1;
    
    -- Safety check to prevent runaway processes
    IF v_rows_added = 0 THEN
      SET v_continue = FALSE;
    END IF;
  END WHILE;

END$$
DELIMITER ;

-- Optimized monitoring queries (idempotent)
CREATE OR REPLACE VIEW progress_summary AS
SELECT 
  root_category,
  COUNT(*) as total_pages,
  MAX(level) as max_level,
  SUM(CASE WHEN is_leaf = FALSE THEN 1 ELSE 0 END) as categories,
  SUM(CASE WHEN is_leaf = TRUE THEN 1 ELSE 0 END) as articles,
  MIN(created_at) as first_created,
  MAX(created_at) as last_created
FROM bstem_category_dag 
GROUP BY root_category
ORDER BY total_pages DESC;

-- Performance analysis query (idempotent)
CREATE OR REPLACE VIEW performance_metrics AS
SELECT 
  iteration,
  root_category,
  CONCAT('L', start_level, '→L', end_level) as level_range,
  FORMAT(rows_added, 0) as rows_added,
  ROUND(execution_time_sec, 2) as exec_time_sec,
  FORMAT(avg_rows_per_sec, 0) as rows_per_sec,
  memory_usage_mb,
  timestamp
FROM build_progress 
ORDER BY iteration DESC;

-- Execute the optimized build
-- CALL BuildBSTEMTree(0, 12, 50000, 100000);

-- Real-time progress monitoring
SELECT 
  CONCAT('Total: ', FORMAT(COUNT(*), 0), 
         ' | Max Level: ', MAX(level),
         ' | Categories: ', FORMAT(SUM(CASE WHEN is_leaf=0 THEN 1 ELSE 0 END), 0),
         ' | Articles: ', FORMAT(SUM(CASE WHEN is_leaf=1 THEN 1 ELSE 0 END), 0),
         ' | Last Updated: ', DATE_FORMAT(MAX(created_at), '%H:%i:%s')) as status
FROM bstem_category_dag;

-- Check for remaining work
SELECT 
  level,
  root_category,
  COUNT(*) as categories_to_expand
FROM bstem_category_dag 
WHERE is_leaf = FALSE 
GROUP BY level, root_category
HAVING COUNT(*) > 0
ORDER BY level, root_category;
