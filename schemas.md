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
High value: 
- cl_from - page.page_id of page in category
- cl_target_id - page.page_id of category 

Notes:
- cl_to - string of category, should match category's page_title
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
High value:
- page_id
- page_title
- page_namespace:
 - 0 = article
 - 14 = category
- page_content_model
 - 'wikitext' - most articles
- page_is_redirect: Boolean flag indicating if this page redirects to another page:
 - 1 = Redirect page (has entry in redirect table)
 - 0 = Normal content page
Notes:

pagelinks: 1,586,173,596 rows
+-------------------+-----------------+------+-----+---------+
| Field             | Type            | Null | Key | Default | 
+-------------------+-----------------+------+-----+---------+
| pl_from           | int unsigned    | NO   | PRI | 0       |
| pl_from_namespace | int             | NO   | MUL | 0       |
| pl_target_id      | bigint unsigned | NO   | PRI | NULL    |
+-------------------+-----------------+------+-----+---------+
High value:
- pl_from: page.page_id of page the link is coming from
- pl_target_id - page.page_id of page linked going to 
Notes:
- pl_from_namespace:
 - 0 = article
 - 14 = category
   
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
High value:
- rd_from: page_id of the redirect page
- rd_namespace: target page's type
 - 0 = article
 - 14 = category
- rd_title: string of target page title
 - this is the string used to match the target's page_title to find the page_id
- rd_interwiki: if not null, it is an external link (exclude)
- rd_fragment: if not null, a string containing the section in the page (e.g., "History")
Notes:

== New tables in this project ==

sass_page: 15,119,748 rows
DAG tree of category pages with article pages as leaves
+----------------+------------------+------+-----+--------------+
| Field          | Type             | Null | Key | Default      |
+----------------+------------------+------+-----+--------------+
| page_id        | int unsigned     | NO   | PRI | NULL         |
| page_title     | varchar(255)     | NO   | MUL |              |
| page_parent_id | int              | NO   | PRI | NULL         |
| page_root_id   | int              | NO   | PRI | NULL         |
| page_dag_level | int              | NO   | MUL | NULL         |
| page_is_leaf   | tinyint(1)       | NO   | MUL | 0            |
+----------------+------------------+------+-----+--------------+

Notes:
- Materialized DAG tree of SASS (Science and Social Science) categories and articles
- page_dag_level → DAG (directed acyclical graph) tree depth in category hierarchy (0 = root categories: Business, Science, Technology, Engineering, Mathematics)
- page_is_leaf → TRUE for articles (namespace 0), FALSE for categories (namespace 14) 
- page_root_id → which of the 5 main SASS domains this page belongs to

sass_lexical_link: 11,483,979 rows
Lexical links (this string connects to that page)
+----------------+------------------+------+-----+---------+
| Field          | Type             | Null | Key | Default |
+----------------+------------------+------+-----+---------+
| ll_from_title  | varbinary(255)   | NO   | MUL |         |
| ll_to_page_id  | int(8) unsigned  | NO   | PRI | 0       |
| ll_to_fragment | varbinary(255)   | YES  |     | NULL    |
+----------------+------------------+------+-----+---------+
Notes:
- ll_from_title: string from the enwiki rd_from's page.page_title
 - this is the lexical/sematic string to match on for redirect
- ll_to_page_id: page to redirect to
- ll_to_fragment: additional lexical/sematic string to find a section of the page (e.g., "History")

sass_associative_link:166,504,762 rows
Associative links (conceptual relationships between pages)
+----------------+------------------------------------------+------+-----+---------+
| Field          | Type                                     | Null | Key | Default |
+----------------+------------------------------------------+------+-----+---------+
| al_from_page_id| int unsigned                             | NO   | PRI | NULL    |
| al_to_page_id  | int unsigned                             | NO   | PRI | NULL    |
| al_type        | enum('pagelink','categorylink','both')   | NO   |     | NULL    |
+----------------+------------------------------------------+------+-----+---------+
Notes:
- al_from_page_id: The page ID where the link originates
- al_to_page_id: The page ID the link points to
- al_type: Specifies the origin of the relationship
  - 'pagelink': Relationship from pagelinks table (article-to-article links)
  - 'categorylink': Relationship from categorylinks table (category membership)
  - 'both': Relationship exists in both pagelinks and categorylinks
