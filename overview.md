# BSTEM Wikipedia Category Tree Builder

## Overview
Extract and materialize a DAG tree of Wikipedia categories and articles for Business, Science, Technology, Engineering, and Mathematics domains from existing Wikipedia database dumps across three phases.

## Summary

### Phase 1: Hierarchical/Classify Knowledge
    - tables: categorylinks > bstem_categoryl_dag
      - add column to table: level 0 for top categories
    - create DAG tree of these categories from categorylinks
      - only include top categories of BSTEM (business, science, technology, engineering, mathematics)
      - each category is a branch of the tree
      - add page title to each branch category
      - recursively add all descendant pages (leaves on branches) to each category from categorylinks
        - add page title to each leaf page
        - exclude files (cl_type = 'file')
        - expand tree through subcategories only, collect articles as terminal nodes

### Phase 2: Redue Page Tree
    - tables: page > bstem_page
    - create materialized page table containing all pages from BSTEM category tree
      - deduplicate pages appearing in multiple BSTEM categories
      - preserve original Wikipedia page metadata (redirects, length, content model, etc.)
      - add BSTEM-specific metadata:
        - min_level: minimum depth where page appears in hierarchy
        - root_categories: comma-separated list of BSTEM domains page belongs to
        - is_leaf: whether page is article (namespace 0) or category (namespace 14)
      - supports efficient page lookups and metadata queries for BSTEM subset

### Phase 3: Lexical/Search Knowledge
    - tables: bstem_redirects
    - create set of page redirects to the pages in bstem_categoryl_dag

### Phase 4: Associative/Connect Knowledge
    - tables: pagelinks > bstem_pagelinks  
    - create filtered pagelinks dataset for BSTEM tree
      - both source and target pages must exist in bstem_page
      - exclude self-links (same page references)
      - maintain link relationships within BSTEM knowledge domain

## Problem Statement
- Need efficient access to BSTEM subset of Wikipedia's 63M pages
- Original categorylinks table (208M rows) and pagelinks table (1.5B rows) too large for targeted analysis
- Category hierarchy traversal requires recursive queries that are slow on full dataset

## Goals
- Build materialized BSTEM category tree covering ~30M articles and 2.5M categories
- Enable fast exploration and analysis of BSTEM domain relationships
- Maintain DAG structure integrity (no cycles, single page instances)
- Complete build in <1 hour on Mac Studio M4 64GB
