-- Create bstem_page table with pages from BSTEM category tree
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
  INDEX idx_leaf (is_leaf)
) ENGINE=InnoDB;

-- Insert only new pages from extended bstem_category_dag
INSERT IGNORE INTO bstem_page (
  page_id, page_namespace, page_title, page_is_redirect, page_is_new,
  page_random, page_touched, page_links_updated, page_latest, page_len,
  page_content_model, page_lang, min_level, root_categories, is_leaf
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
  MIN(bcl.level) as min_level,
  GROUP_CONCAT(DISTINCT bcl.root_category ORDER BY bcl.root_category) as root_categories,
  MAX(bcl.is_leaf) as is_leaf
FROM page p
JOIN bstem_category_dag bcl ON p.page_id = bcl.page_id
LEFT JOIN bstem_page existing ON p.page_id = existing.page_id
WHERE existing.page_id IS NULL
GROUP BY p.page_id, p.page_namespace, p.page_title, p.page_is_redirect, 
         p.page_is_new, p.page_random, p.page_touched, p.page_links_updated, 
         p.page_latest, p.page_len, p.page_content_model, p.page_lang;

-- Verification query
SELECT 
  CONCAT('Pages loaded: ', FORMAT(COUNT(*), 0)) as total_pages,
  CONCAT('Articles: ', FORMAT(SUM(CASE WHEN page_namespace = 0 THEN 1 ELSE 0 END), 0)) as articles,
  CONCAT('Categories: ', FORMAT(SUM(CASE WHEN page_namespace = 14 THEN 1 ELSE 0 END), 0)) as categories,
  CONCAT('Redirects: ', FORMAT(SUM(page_is_redirect), 0)) as redirects
FROM bstem_page;
