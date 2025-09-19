# Wikipedia Category Tree Builder

## Overview
Extract and materialize a DAG tree of Wikipedia categories and articles for Science and Social Sciences (SASS) domainsfrom existing Wikipedia database dumps across three phases.

## Problem Statement
- Need efficient access to SASS subset of Wikipedia's 63M pages
- Original categorylinks (208M rows) and pagelinks (1.5B rows) too large for targeted analysis  
- Category hierarchy traversal requires recursive queries that are slow on full dataset

## Goals
- Build materialized SASS category DAG covering ~30M articles and 2.5M categories
- Enable fast exploration through hierarchical relationships
- Maintain DAG structure integrity (no cycles, proper parent-child relationships)
- Complete build in <10 hours on Mac Studio M4 64GB
- Final system accesses only new sass_* tables, not original enwiki tables

## Solution

### Phase 1: Build SASS Page Tree
- Source tables: categorylinks, page → sass_page
- Build materialized DAG tree containing all SASS categories and articles
- Start with root categories: Business, Science, Technology, Engineering, Mathematics (level 0)
- Recursively traverse categorylinks to build complete hierarchy:
  - Categories become branch nodes (page_namespace = 14, is_leaf = FALSE)
  - Articles become leaf nodes (page_namespace = 0, is_leaf = TRUE)  
  - Exclude files (cl_type = 'file')
- Schema preserves essential Wikipedia metadata plus DAG-specific fields:
  - page_dag_level: minimum depth in DAG tree (0 = root SASS categories)
  - page_root_id: which of the 5 main SASS domains this page belongs to
  - page_is_leaf: TRUE for articles, FALSE for categories
- Handles pages appearing in multiple SASS categories through deduplication

sass_page: 9,392,822 rows
+-------+------------+------------+-----------+------------+
| level | page_count | percentage | articles  | categories |
+-------+------------+------------+-----------+------------+
|     0 | 2          | 0.0%       | 0         | 2          |
|     1 | 154        | 0.0%       | 67        | 87         |
|     2 | 5,685      | 0.1%       | 4,525     | 1,160      |
|     3 | 57,505     | 0.6%       | 48,334    | 9,171      |
|     4 | 426,707    | 4.5%       | 379,143   | 47,564     |
|     5 | 1,125,751  | 12.0%      | 942,877   | 182,874    |
|     6 | 3,049,982  | 32.5%      | 2,598,199 | 451,783    |
|     7 | 2,777,766  | 29.6%      | 2,088,360 | 689,406    |
|     8 | 1,409,783  | 15.0%      | 828,023   | 581,760    |
|     9 | 445,851    | 4.7%       | 280,152   | 165,699    |
|    10 | 93,636     | 1.0%       | 60,221    | 33,415     |
+-------+------------+------------+-----------+------------+

### Phase 2: Build Lexical Search Mapping  
- Source tables: redirect → sass_lexical_link
- Create semantic equivalence mapping for alternative page titles
- Enables lexical search by mapping redirects (e.g., "ML" → "Machine Learning") to canonical pages
- Schema:
  - ll_from_title: lexical/semantic string from redirect page title
  - ll_to_page_id: target page_id in sass_page
  - ll_to_fragment: optional section anchor within target page
- Use Comprehensive Chain Resolution: recursive CTE
- Similar to build_sass_page.sql, display results after each level is completed

### Phase 3: Build Associative Link Network
- Source tables: pagelinks, categorylinks → sass_associative_link
- Create filtered link datasets for SASS associative link relationships
- sass_associative_link: Unified relationship tracking with type classification
  - al_from_page_id: source page ID
  - al_to_page_id: target page ID
  - al_type: relationship origin ('pagelink', 'categorylink', 'both')
- Only include links where both source and target exist in sass_page
- Exclude self-links
- Maintains link relationships within SASS knowledge domain
- Similar to build_sass_page.sql, display results after each level is completed
