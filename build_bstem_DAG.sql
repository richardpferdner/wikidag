-- Create main table with enhanced indexing
CREATE TABLE bstem_categorylinks (
  page_id INT,
  page_title VARCHAR(255),
  page_namespace INT,
  level INT,
  root_category VARCHAR(255),
  is_leaf BOOLEAN,
  INDEX(page_id), 
  INDEX(root_category), 
  INDEX(level),
  INDEX(is_leaf),
  INDEX level_leaf (level, is_leaf)
);

-- Enhanced indexes on existing tables
ALTER TABLE categorylinks ADD INDEX target_type (cl_target_id, cl_type);

-- Create progress tracking table
CREATE TABLE build_progress (
  iteration INT AUTO_INCREMENT PRIMARY KEY,
  start_level INT,
  end_level INT,
  batch_size INT,
  rows_added INT,
  execution_time_sec INT,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert root categories (level 0)
INSERT INTO bstem_categorylinks 
SELECT page_id, page_title, page_namespace, 0, page_title, FALSE
FROM page 
WHERE page_namespace = 14 
AND page_title IN ('Science','Technology','Mathematics','Engineering','Business');

-- Set iteration parameters
SET @start_level = 0;
SET @end_level = 2;
SET @batch_size = 100000;
SET @start_time = UNIX_TIMESTAMP();

-- Create temporary work table for current iteration
CREATE TEMPORARY TABLE current_expandable AS 
SELECT page_id, root_category, level 
FROM bstem_categorylinks 
WHERE level >= @start_level AND level < @end_level AND is_leaf = FALSE;

-- Add index to temp table
ALTER TABLE current_expandable ADD INDEX(page_id);

-- Optimized main query
INSERT INTO bstem_categorylinks 
SELECT DISTINCT p.page_id, p.page_title, p.page_namespace, 
       ce.level + 1, ce.root_category,
       p.page_namespace != 14 as is_leaf
FROM current_expandable ce
JOIN categorylinks cl ON ce.page_id = cl.cl_target_id
JOIN page p ON cl.cl_from = p.page_id
WHERE cl.cl_type IN ('page', 'subcat')
AND p.page_id NOT IN (SELECT page_id FROM bstem_categorylinks)
LIMIT @batch_size;

-- Clean up temp table
DROP TEMPORARY TABLE current_expandable;

-- Log progress
INSERT INTO build_progress (start_level, end_level, batch_size, rows_added, execution_time_sec)
VALUES (@start_level, @end_level, @batch_size, ROW_COUNT(), UNIX_TIMESTAMP() - @start_time);

-- Check remaining work
SELECT 
  level,
  COUNT(*) as categories_to_expand,
  root_category
FROM bstem_categorylinks 
WHERE is_leaf = FALSE 
AND level >= @end_level
GROUP BY level, root_category
ORDER BY level, root_category;

-- Progress summary
SELECT 
  MAX(level) as max_level,
  COUNT(*) as total_pages,
  SUM(CASE WHEN is_leaf = FALSE THEN 1 ELSE 0 END) as categories,
  SUM(CASE WHEN is_leaf = TRUE THEN 1 ELSE 0 END) as articles,
  root_category
FROM bstem_categorylinks 
GROUP BY root_category
ORDER BY root_category;
