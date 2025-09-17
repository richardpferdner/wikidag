Table Schemas

== From enwiki data dumps ==

categorylinks
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

page
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


pagelinks
+-------------------+-----------------+------+-----+---------+
| Field             | Type            | Null | Key | Default | 
+-------------------+-----------------+------+-----+---------+
| pl_from           | int unsigned    | NO   | PRI | 0       |
| pl_from_namespace | int             | NO   | MUL | 0       |
| pl_target_id      | bigint unsigned | NO   | PRI | NULL    |
+-------------------+-----------------+------+-----+---------+

== New tables in this project

