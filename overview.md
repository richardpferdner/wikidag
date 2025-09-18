# BSTEM Wikipedia Category Tree Builder

## Overview
Extract and materialize a DAG tree of Wikipedia categories and articles for Business, Science, Technology, Engineering, and Mathematics domains from existing Wikipedia database dumps across five phases.

## Summary

### Phase 1: Hierarchical/Classify Knowledge
- tables: categorylinks > bstem_category_dag
  - add column to table: level 0 for top categories
- create DAG tree of these categories from categorylinks
  - only include top categories of BSTEM (business, science, technology, engineering, mathematics)
  - each category is a branch of the tree
  - add page title to each branch category
  - recursively add all descendant pages (leaves on branches) to each category from categorylinks
    - add page title to each leaf page
    - exclude files (cl_type = 'file')
    - expand tree through subcategories only, collect articles as terminal nodes

### Phase 2: Prune Page Tree to BSTEM
- tables: page > bstem_page
- create materialized page table containing all pages from BSTEM category tree
  - deduplicate pages appearing in multiple BSTEM categories
  - preserve essential Wikipedia page metadata (page_id, page_title)
  - add DAG-specific metadata:
    - page_parent_id: direct parent category in the DAG hierarchy
    - page_root_id: ID of the root BSTEM category (Business, Science, Technology, Engineering, or Mathematics)
    - page_dag_level: depth in the DAG tree (0 = root BSTEM categories)
    - page_is_leaf: TRUE for articles (namespace 0), FALSE for categories (namespace 14)
  - supports efficient DAG traversal and hierarchical queries for BSTEM subset

### Phase 3: Lexical/Search Knowledge
- tables: redirect > bstem_redirect
- create semantic equivalence mapping for alternative page titles pointing to BSTEM pages
- enables lexical search by mapping redirects (e.g., "ML" → "Machine Learning") to canonical pages
- simplified schema:
  - rd_from_title: lexical/semantic string from the redirect page's title
  - rd_to_page_id: target page_id in bstem_page
  - rd_to_fragment: optional section anchor within the target page (e.g., "History")
- two implementation approaches available:

#### Approach 1: Simple Single-Hop (Recommended)
- direct JOIN between redirect and bstem_page tables
- captures ~85-90% of redirect relationships (single redirects only)
- execution time: 2-5 minutes
- complexity: low (single 4-table JOIN)
- best for initial implementation

#### Approach 2: Comprehensive Chain Resolution  
- two-stage process with recursive CTE for redirect chains
- captures ~95-98% of redirect relationships (includes redirect→redirect→target)
- execution time: 8-15 minutes  
- complexity: high (recursive queries, 4x more code)
- use if lexical search coverage proves insufficient with Approach 1

### Phase 4: Associative/Connect Knowledge
- tables: pagelinks > bstem_pagelink 
- create filtered pagelinks dataset for BSTEM tree
  - both source and target pages must exist in bstem_page
  - exclude self-links (same page references)
  - simplified schema:
    - pl_from_page_id: source page ID
    - pl_to_page_id: target page ID
  - maintain link relationships within BSTEM knowledge domain

### Phase 5: Clean up
- Remove unnecessary table elements from original Wikipedia schema
  - page_is_new
  - page_random
  - page_touched
  - page_links_updated
  - page_latest
  - page_len
  - Focus on essential fields for DAG navigation and content identification
     
## Problem Statement
- Need efficient access to BSTEM subset of Wikipedia's 63M pages
- Original categorylinks table (208M rows) and pagelinks table (1.5B rows) too large for targeted analysis
- Category hierarchy traversal requires recursive queries that are slow on full dataset

## Goals
- Build materialized BSTEM category DAG covering ~30M articles and 2.5M categories
- Enable fast exploration and analysis of BSTEM domain relationships through parent/root relationships
- Maintain DAG structure integrity (no cycles, proper parent-child relationships)
- Complete build in <1 hour on Mac Studio M4 64GB
