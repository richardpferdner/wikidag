# BSTEM Wikipedia Category Tree Builder

## Overview
Extract and materialize a DAG tree of Wikipedia categories and articles for Business, Science, Technology, Engineering, and Mathematics domains from existing Wikipedia database dumps across three phases.

## Problem Statement
- Need efficient access to BSTEM subset of Wikipedia's 63M pages
- Original categorylinks (208M rows) and pagelinks (1.5B rows) too large for targeted analysis  
- Category hierarchy traversal requires recursive queries that are slow on full dataset

## Goals
- Build materialized BSTEM category DAG covering ~30M articles and 2.5M categories
- Enable fast exploration through hierarchical relationships
- Maintain DAG structure integrity (no cycles, proper parent-child relationships)
- Complete build in <1 hour on Mac Studio M4 64GB
- Final system accesses only new bstem_* tables, not original enwiki tables

## Solution

### Phase 1: Build BSTEM Page Tree
- Source tables: categorylinks, page → bstem_page
- Build materialized DAG tree containing all BSTEM categories and articles
- Start with root categories: Business, Science, Technology, Engineering, Mathematics (level 0)
- Recursively traverse categorylinks to build complete hierarchy:
  - Categories become branch nodes (page_namespace = 14, is_leaf = FALSE)
  - Articles become leaf nodes (page_namespace = 0, is_leaf = TRUE)  
  - Exclude files (cl_type = 'file')
- Schema preserves essential Wikipedia metadata plus DAG-specific fields:
  - min_level: minimum depth in DAG tree (0 = root BSTEM categories)
  - root_categories: comma-separated list of root BSTEM domains this page belongs to
  - is_leaf: TRUE for articles, FALSE for categories
- Handles pages appearing in multiple BSTEM categories through deduplication

### Phase 2: Build Lexical Search Mapping  
- Source tables: redirect → bstem_redirect
- Create semantic equivalence mapping for alternative page titles
- Enables lexical search by mapping redirects (e.g., "ML" → "Machine Learning") to canonical pages
- Schema:
  - rd_from_title: lexical/semantic string from redirect page title
  - rd_to_page_id: target page_id in bstem_page
  - rd_to_fragment: optional section anchor within target page
- Two implementation approaches:
  - Simple Single-Hop: direct JOIN (85-90% coverage, 2-5 minutes)
  - Comprehensive Chain Resolution: recursive CTE (95-98% coverage, 8-15 minutes)

### Phase 3: Build Associative Link Network
- Source tables: pagelinks → bstem_pagelink  
- Create filtered pagelinks dataset for BSTEM tree
- Only include links where both source and target exist in bstem_page
- Exclude self-links
- Schema:
  - pl_from_page_id: source page ID
  - pl_to_page_id: target page ID
- Maintains link relationships within BSTEM knowledge domain
     
