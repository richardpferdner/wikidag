Table Schemas

== From enwiki data dumps ==

categorylinks: 208,756,116 rows
+-------------------+------------------------------+------+-----+
| Field             | Type                         | Null | Key | 
+-------------------+------------------------------+------+-----+
| cl_from           | int unsigned                 | NO   | PRI | 
| cl_to             | varbinary(255)               | NO   | MUL |
| cl_sortkey        | varbinary(230)               | NO   |     |
| cl_timestamp      | timestamp                    | NO   |     |
| cl_sortkey_prefix | varbinary(255)               | NO   |     |
| cl_collation      | varbinary(32)                | NO   |     |
| cl_type           | enum('page','subcat','file') | NO   |     | 
| cl_collation_id   | smallint unsigned            | NO   |     | 
| cl_target_id      | bigint unsigned              | NO   | PRI | 
+-------------------+------------------------------+------+-----+
Notes:
- cl_type → what kind of page cl_from is. 
  - cl_type refers to the thing being categorized, i.e. the page identified by cl_from, not the category (cl_to)

page: 63,942,562 rows
+--------------------+------------------+------+-----+---------+
| Field              | Type             | Null | Key | Default | 
+--------------------+------------------+------+-----+---------+
| page_id            | int unsigned     | NO   | PRI | NULL    | 
| page_namespace     | int              | NO   | MUL | 0       |
| page_title         | varbinary(255)   | NO   |     |         |
| page_is_redirect   | tinyint unsigned | NO   | MUL | 0       |
| page_is_new        | tinyint unsigned | NO   |     | 0       |
| page_random        | double unsigned  | NO   | MUL | 0       |
| page_touched       | binary(14)       | NO   |     | NULL    |
| page_links_updated | binary(14)       | YES  |     | NULL    |
| page_latest        | int unsigned     | NO   |     | 0       |
| page_len           | int unsigned     | NO   | MUL | 0       |
| page_content_model | varbinary(32)    | YES  |     | NULL    |
| page_lang          | varbinary(35)    | YES  |     | NULL    |
+--------------------+------------------+------+-----+---------+

pagelinks: 1,586,173,596 rows
+-------------------+-----------------+------+-----+---------+
| Field             | Type            | Null | Key | Default | 
+-------------------+-----------------+------+-----+---------+
| pl_from           | int unsigned    | NO   | PRI | 0       |
| pl_from_namespace | int             | NO   | MUL | 0       |
| pl_target_id      | bigint unsigned | NO   | PRI | NULL    |
+-------------------+-----------------+------+-----+---------+

redirect: 14,843,089 rows
+---------------+------------------+------+-----+---------+
| Field         | Type             | Null | Key | Default |
+---------------+------------------+------+-----+---------+
| rd_from       | int(8) unsigned  | NO   | PRI | 0       |
| rd_namespace  | int(11)          | NO   | MUL | 0       |
| rd_title      | varbinary(255)   | NO   | MUL |         |
| rd_interwiki  | varbinary(32)    | YES  |     | NULL    |
| rd_fragment   | varbinary(255)   | YES  |     | NULL    |
+---------------+------------------+------+-----+---------+
Notes:
- rd_from → page_id of the redirect page
- rd_namespace, rd_title → target page location
- rd_interwiki → for interwiki redirects (usually empty for local redirects)
- rd_fragment → for redirects to page sections (usually empty)
- Maps redirect sources to their destinations


== New tables in this project ==

bstem_category_dag
+----------------+-------------+------+-----+-------------------+
| Field          | Type        | Null | Key | Default           |
+----------------+-------------+------+-----+-------------------+
| page_id        | int         | NO   | PRI | NULL              |
| page_title     | varchar(255)| NO   |     |                   |
| page_namespace | int         | NO   | MUL | NULL              |
| level          | int         | NO   | MUL | NULL              |
| root_category  | varchar(255)| NO   | MUL | NULL              |
| is_leaf        | tinyint(1)  | NO   | MUL | 0                 |
| created_at     | timestamp   | YES  |     | CURRENT_TIMESTAMP |
+----------------+-------------+------+-----+-------------------+

Notes:
- Materialized DAG tree of BSTEM (Business, Science, Technology, Engineering, Mathematics) categories and articles
- level → depth in category hierarchy (0 = root categories: Business, Science, Technology, Engineering, Mathematics)
- is_leaf → TRUE for articles (namespace 0), FALSE for categories (namespace 14) 
- root_category → which of the 5 main BSTEM domains this page belongs to
- Indexes: PRIMARY KEY (page_id), idx_root_level (root_category, level), idx_level_leaf (level, is_leaf), idx_namespace (page_namespace), UNIQUE KEY uk_page_root (page_id, root_category)

bstem_page
+--------------------+------------------+------+-----+-------------------+
| Field              | Type             | Null | Key | Default           |
+--------------------+------------------+------+-----+-------------------+
| page_id            | int unsigned     | NO   | PRI | NULL              |
| page_namespace     | int              | NO   | MUL | NULL              |
| page_title         | varchar(255)     | NO   | MUL |                   |
| page_is_redirect   | tinyint unsigned | NO   | MUL | 0                 |
| page_is_new        | tinyint unsigned | NO   |     | 0                 |
| page_random        | double unsigned  | NO   |     | 0                 |
| page_touched       | binary(14)       | NO   |     | NULL              |
| page_links_updated | binary(14)       | YES  |     | NULL              |
| page_latest        | int unsigned     | NO   |     | 0                 |
| page_len           | int unsigned     | NO   |     | 0                 |
| page_content_model | varchar(32)      | YES  |     | NULL              |
| page_lang          | varchar(35)      | YES  |     | NULL              |
| min_level          | int              | NO   | MUL | NULL              |
| root_categories    | text             | NO   |     |                   |
| is_leaf            | boolean          | NO   | MUL | NULL              |
| created_at         | timestamp        | YES  |     | CURRENT_TIMESTAMP |
+--------------------+------------------+------+-----+-------------------+

Notes:
- Materialized page table containing all pages from BSTEM category tree
- Deduplicates pages appearing in multiple BSTEM categories
- Preserves original Wikipedia page metadata (redirects, length, content model, etc.)
- min_level → minimum depth where page appears in hierarchy
- root_categories → comma-separated list of BSTEM domains page belongs to
- is_leaf → TRUE for articles (namespace 0), FALSE for categories (namespace 14)
- Indexes: PRIMARY KEY (page_id), idx_namespace (page_namespace), idx_title (page_title), idx_redirect (page_is_redirect), idx_min_level (min_level), idx_leaf (is_leaf)
