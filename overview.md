# BSTEM Wikipedia Category Tree Builder

## Overview
Extract and materialize a DAG tree of Wikipedia categories and articles for Business, Science, Technology, Engineering, and Mathematics domains from existing Wikipedia database dumps across three phases.

## Summary

### Phase 1: Hierarchical/Classify Knowledge
    - tables: categorylinks > bstem_categorylinks
      - add column to table: level 0 for top categories
    - create DAG tree of these categories from categorylinks
      - only include top categories of BSTEM (business, science, technology, engineering, mathematics)
      - each category is a branch of the tree
      - add page title to each branch category
      - recursively add all descendant pages (leaves on branches) to each category from categorylinks
        - add page title to each leaf page
        - exclude files (cl_type = 'file')
        - expand tree through subcategories only, collect articles as terminal nodes

### Phase 2: Lexical/Search Knowledge
    - tables: page > bstem_page
    - create materialized page table containing all pages from BSTEM category tree
      - deduplicate pages appearing in multiple BSTEM categories
      - preserve original Wikipedia page metadata (redirects, length, content model, etc.)
      - add BSTEM-specific metadata:
        - min_level: minimum depth where page appears in hierarchy
        - root_categories: comma-separated list of BSTEM domains page belongs to
        - is_leaf: whether page is article (namespace 0) or category (namespace 14)
      - supports efficient page lookups and metadata queries for BSTEM subset
    - later: 
      - import into postgres vector database
      - add semantic/lexical similarity vectors based on page titles
  
### Phase 3: Associative/Connect Knowledge
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

## Technical Approach

### Database Schema
```sql
CREATE TABLE bstem_categorylinks (
  page_id INT,
  page_title VARCHAR(255),
  page_namespace INT,
  level INT,
  root_category VARCHAR(255),
  is_leaf BOOLEAN,
  -- Enhanced indexes for performance
);
```

### Algorithm
1. Start with 5 root categories: Business, Science, Technology, Engineering, Mathematics
2. Iteratively expand through subcategories (namespace 14) only
3. Collect articles as terminal nodes at each level
4. Use temporary work tables for staged processing

## Implementation Plan

### Phase 1: Setup (5 minutes)
- Create optimized tables with enhanced indexing
- Add composite indexes to existing categorylinks table
- Initialize progress tracking system

### Phase 2: Iterative Tree Building (40 minutes)
- Process levels 0-12 using batched approach
- Level-by-level expansion with temporary work tables
- Real-time progress monitoring
- Automatic performance logging

### Phase 3: Validation (3 minutes)
- Verify DAG properties (no cycles)
- Validate row counts against estimates
- Performance analysis and optimization recommendations

## Success Criteria
- [ ] 30M articles extracted from BSTEM domains
- [ ] 2.5M categories in complete hierarchy
- [ ] Build completes in <60 minutes
- [ ] DAG structure verified (no duplicate pages)
- [ ] All 5 root categories fully expanded
- [ ] Query performance >400K rows/sec sustained

## Risk Assessment

### High Risk
- **Memory exhaustion** at levels 4-7 (10-28M rows)
  - *Mitigation*: Temporary table staging, batch processing
- **Query timeout** on large joins
  - *Mitigation*: Enhanced indexing, level-by-level approach

### Medium Risk  
- **Incomplete category coverage** if root categories are misnamed
  - *Mitigation*: Verify exact category names in page table
- **Performance degradation** as table grows
  - *Mitigation*: Progress monitoring, adaptive batch sizes

### Low Risk
- **Disk space** requirements (~50GB for intermediate tables)
  - *Mitigation*: Monitor space during build, cleanup temp tables

## Monitoring & Observability
- Real-time progress query: `SELECT CONCAT('Total: ', FORMAT(COUNT(*), 0), ' | L', MAX(level), ' @ ', DATE_FORMAT(NOW(), '%H:%i:%s')) FROM bstem_categorylinks;`
- Performance tracking in `build_progress` table
- Level-by-level completion verification

## Deliverables
1. Optimized `bstem_categorylinks` table with 32.5M rows
2. Performance analysis and optimization recommendations  
3. Validation report confirming DAG structure
4. Documentation for ongoing maintenance and updates
