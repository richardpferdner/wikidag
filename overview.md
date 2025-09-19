# Wikipedia Category Tree Builder

## Overview
Extract and materialize a DAG tree of Wikipedia categories and articles for Geography, Science, and Social Sciences (GSSS) domainsfrom existing Wikipedia database dumps across three phases.

## Problem Statement
- Need efficient access to GSSS subset of Wikipedia's 63M pages
- Original categorylinks (208M rows) and pagelinks (1.5B rows) too large for targeted analysis  
- Category hierarchy traversal requires recursive queries that are slow on full dataset

## Goals
- Build materialized GSSS category DAG covering ~30M articles and 2.5M categories
- Enable fast exploration through hierarchical relationships
- Maintain DAG structure integrity (no cycles, proper parent-child relationships)
- Complete build in <10 hours on Mac Studio M4 64GB
- Final system accesses only new gsss_* tables, not original enwiki tables

## Solution

### Phase 1: Build GSSS Page Tree
- Source tables: categorylinks, page → gsss_page
- Build materialized DAG tree containing all GSSS categories and articles
- Start with root categories: Business, Science, Technology, Engineering, Mathematics (level 0)
- Recursively traverse categorylinks to build complete hierarchy:
  - Categories become branch nodes (page_namespace = 14, is_leaf = FALSE)
  - Articles become leaf nodes (page_namespace = 0, is_leaf = TRUE)  
  - Exclude files (cl_type = 'file')
- Schema preserves essential Wikipedia metadata plus DAG-specific fields:
  - page_dag_level: minimum depth in DAG tree (0 = root GSSS categories)
  - page_root_id: which of the 5 main GSSS domains this page belongs to
  - page_is_leaf: TRUE for articles, FALSE for categories
- Handles pages appearing in multiple GSSS categories through deduplication

gsss_page: 15,119,748 rows
+-------+------------+------------+
| level | page_count | percentage |
+-------+------------+------------+
|     0 | 3          | 0.0%       |
|     1 | 214        | 0.0%       |
|     2 | 7,025      | 0.0%       |
|     3 | 86,583     | 0.6%       |
|     4 | 510,744    | 3.4%       |
|     5 | 1,451,833  | 9.6%       |
|     6 | 3,434,177  | 22.7%      |
|     7 | 2,745,676  | 18.2%      |
|     8 | 1,526,221  | 10.1%      |
|     9 | 5,337,837  | 35.3%      |
|    10 | 13,722     | 0.1%       |
|    11 | 2,514      | 0.0%       |
|    12 | 695        | 0.0%       |
|    13 | 357        | 0.0%       |
|    14 | 490        | 0.0%       |
|    15 | 691        | 0.0%       |
|    16 | 961        | 0.0%       |
|    17 | 3          | 0.0%       |
|    18 | 1          | 0.0%       |
|    19 | 1          | 0.0%       |
+-------+------------+------------+

### Phase 2: Build Lexical Search Mapping  
- Source tables: redirect → gsss_lexical_link
- Create semantic equivalence mapping for alternative page titles
- Enables lexical search by mapping redirects (e.g., "ML" → "Machine Learning") to canonical pages
- Schema:
  - ll_from_title: lexical/semantic string from redirect page title
  - ll_to_page_id: target page_id in gsss_page
  - ll_to_fragment: optional section anchor within target page
- Two implementation approaches:
  - Simple Single-Hop: direct JOIN (85-90% coverage, 2-5 minutes)
  - Comprehensive Chain Resolution: recursive CTE (95-98% coverage, 8-15 minutes)

### Phase 3: Build Associative Link Network
- Source tables: pagelinks, categorylinks → gsss_associative_link
- Create filtered link datasets for GSSS associative link relationships
- gsss_associative_link: Unified relationship tracking with type classification
  - al_from_page_id: source page ID
  - al_to_page_id: target page ID
  - al_type: relationship origin ('pagelink', 'categorylink', 'both')

- Only include links where both source and target exist in gsss_page
- Exclude self-links
- Maintains link relationships within GSSS knowledge domain
