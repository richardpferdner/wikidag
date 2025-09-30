# Wikipedia Category Tree Builder - Pipeline Overview

## Problem Statement
- Need efficient access to SASS (Science and Social Sciences) subset of Wikipedia's 63M pages
- Original categorylinks (208M rows) and pagelinks (1.5B rows) too large for targeted analysis  
- Category hierarchy traversal requires recursive queries that are slow on full dataset

## Goals
- Build materialized SASS category DAG covering articles and categories
- Enable fast exploration through hierarchical relationships
- Maintain DAG structure integrity (no cycles, proper parent-child relationships)
- Deduplicate pages with identical titles to canonical representatives
- Support lexical search through redirect mapping
- Create filtered associative link network

## Pipeline Architecture

### Phase 1A: Build SASS Page Tree (MySQL)
**Script:** `build_sass_page.sql`  
**Output:** `sass_page` table (~9.4M rows with duplicates)

**Process:**
1. Uses pre-computed `wiki_top3_levels` for levels 0-2 (root categories and immediate children)
2. Recursively traverses `categorylinks` to build levels 3-10
3. Applies optional filtering to exclude maintenance/administrative categories
4. Each page appears once per parent category (creates duplicates for pages in multiple categories)

**Schema:**
- `page_id`: Wikipedia page ID
- `page_title`: Original title (with duplicates)
- `page_parent_id`: Parent category ID
- `page_root_id`: Root domain (Business, Science, Technology, Engineering, Mathematics)
- `page_dag_level`: Depth in hierarchy (0-10)
- `page_is_leaf`: TRUE for articles, FALSE for categories

**Distribution:**
- Level 0: 2 pages (root categories)
- Levels 1-10: ~9.4M total pages
- ~77% articles, ~23% categories
- Contains duplicate titles at different levels

### Phase 1B: Deduplicate and Clean Titles (MySQL)
**Script:** `convert_sass_page_clean.sql`  
**Output:** `sass_page_clean` (~2.1M representatives) + `sass_identity_pages` (~9.4M mappings)

**Process:**
1. Applies enhanced title normalization to all pages
2. Selects ONE representative per unique title using priority:
   - First: Original hierarchy pages (levels 0-2)
   - Then: Deepest in hierarchy
   - Then: Categories over articles
   - Finally: Lowest page_id
3. Creates `sass_page_clean` with representatives only
4. Creates `sass_identity_pages` mapping all 9.4M pages to their representatives
5. Auto-repairs orphan references caused by deduplication
6. Prevents self-reference loops using grandparent fallback

**Title Cleaning:**
- Converts spaces/underscores to normalized underscores
- Removes parenthetical content
- Normalizes punctuation and symbols
- Preserves readability

**Tables:**
- `sass_page_clean`: ~2.1M unique representative pages (no duplicates)
- `sass_identity_pages`: ~9.4M rows mapping original page_id → representative_page_id

### Phase 1C: Export to PostgreSQL (MySQL → CSV → PostgreSQL)
**Script:** `export_sass_page_clean_to_postgres.sql`  
**Output:** CSV file(s) containing representatives only

**Process:**
1. Exports ONLY representative pages from `sass_page_clean` (~2.1M rows)
2. Uses `sass_identity_pages` to identify representatives (where page_id = representative_page_id)
3. Provides multiple export methods:
   - Single CSV file with sampling
   - Chunked CSV files for large datasets
   - String output for manual handling
4. Index-free export for faster PostgreSQL import
5. Proper CSV escaping for PostgreSQL compatibility

**Export Options:**
- Sampling: Export subset for testing (e.g., 1% sample)
- Chunking: Split into multiple files to avoid memory issues
- File location: Uses MySQL's `secure_file_priv` directory

**PostgreSQL Import:**
- Import representatives into PostgreSQL for analysis
- Build indices after import for performance
- Estimated time: 45-60 minutes for full dataset

### Phase 2: Build Lexical Search Mapping (MySQL)
**Script:** `build_sass_lexical_link.sql`  
**Source:** `redirect` table → `sass_lexical_link`  
**Output:** ~11.5M lexical links

**Process:**
1. Processes Wikipedia redirect table
2. Maps alternative titles (redirects) to canonical representative pages
3. Accepts redirects from ANY Wikipedia page to SASS domain
4. Preserves section anchors (fragments)
5. Detects 2-level redirect chains and cycles

**Use Cases:**
- "ML" → "Machine_learning" (representative)
- "AI" → "Artificial_intelligence" (representative)
- "Neural_networks" → "Artificial_neural_network#History" (with fragment)

**Schema:**
- `ll_from_title`: Lexical/semantic string (redirect source)
- `ll_to_page_id`: Target representative page_id
- `ll_to_fragment`: Optional section anchor

### Phase 3: Build Associative Link Network (MySQL)
**Script:** `build_sass_associative_link.sql`  
**Source:** `pagelinks` + `categorylinks` → `sass_associative_link`  
**Output:** ~166.5M associative links

**Process:**
1. Filters pagelinks (article-to-article) and categorylinks (category membership)
2. Includes links where BOTH source AND target exist in SASS domain
3. Resolves all links to representative pages using `sass_identity_pages`
4. Aggregates link types ('pagelink', 'categorylink', 'both')
5. Excludes self-links (source = target after representative resolution)
6. Streams large datasets in batches for memory efficiency

**Schema:**
- `al_from_page_id`: Source representative page_id
- `al_to_page_id`: Target representative page_id
- `al_type`: Relationship origin ('pagelink', 'categorylink', 'both')

## Data Flow Summary

```
Wikipedia dumps (page, categorylinks, pagelinks, redirect)
    ↓
[Phase 1A] build_sass_page.sql
    ↓
sass_page (9.4M rows with duplicates)
    ↓
[Phase 1B] convert_sass_page_clean.sql
    ↓
sass_page_clean (2.1M representatives) + sass_identity_pages (9.4M mappings)
    ↓
[Phase 1C] export_sass_page_clean_to_postgres.sql
    ↓
PostgreSQL (2.1M representatives for analysis)
    ↓
[Phase 2] build_sass_lexical_link.sql
    ↓
sass_lexical_link (11.5M lexical links to representatives)
    ↓
[Phase 3] build_sass_associative_link.sql
    ↓
sass_associative_link (166.5M links between representatives)
```

## Key Requirements

**Prerequisites:**
1. Wikipedia database dumps imported (page, categorylinks, pagelinks, redirect)
2. `wiki_top3_levels` table pre-computed (for Phase 1A)
3. MySQL with sufficient memory (64GB recommended)
4. PostgreSQL instance for final data warehouse

**System Requirements:**
- MySQL: 64GB RAM, 500GB+ storage
- PostgreSQL: 32GB RAM, 200GB+ storage
- Estimated total build time: 4-6 hours

**Build Order:**
1. Run `build_sass_page.sql` (creates sass_page with duplicates)
2. Run `convert_sass_page_clean.sql` (deduplicates to representatives)
3. Run `export_sass_page_clean_to_postgres.sql` (exports to CSV)
4. Import CSV to PostgreSQL
5. Run `build_sass_lexical_link.sql` (creates lexical mappings)
6. Run `build_sass_associative_link.sql` (creates link network)

5. **Associative Links Scope:** Links are resolved to representatives. Does this lose granularity when multiple pages share the same title?

6. **Performance:** Phase 3 (associative links) estimates 2-3 hours. Can this be optimized further with better indexing strategies?
