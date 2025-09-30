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
- Filter weak branches (categories with <5 children) for quality
- Complete build in <2.5 hours on Mac Studio M4 64GB

## Pipeline Architecture

### Phase 1A: Build SASS Page Tree (MySQL)
**Script:** `build_sass_page.sql`  
**Output:** `sass_page` table (~9.4M rows with duplicates)

**Prerequisites:**
- `wiki_top3_levels` table (pre-computed levels 0-2)
- `sass_filter_patterns` table (maintenance category filters)

**Process:**
1. Uses `wiki_top3_levels` for levels 0-2 (root categories: Business, Science, Technology, Engineering, Mathematics)
2. Recursively traverses `categorylinks` to build levels 3-10
3. Applies filtering to exclude maintenance/administrative categories
4. Each page appears once per parent category (creates duplicates for pages in multiple categories)

**Procedure:**
- `BuildSASSPageTreeFiltered(begin_level, end_level, enable_filtering)`
- Default: `(0, 10, 1)` - builds all 10 levels with filtering enabled

**Schema:**
- `page_id`: Wikipedia page ID
- `page_title`: Original title (with duplicates)
- `page_parent_id`: Parent category ID
- `page_root_id`: Root domain ID (1-5 for SASS categories)
- `page_dag_level`: Depth in hierarchy (0-10)
- `page_is_leaf`: TRUE for articles (namespace 0), FALSE for categories (namespace 14)

**Distribution:**
- Level 0: 2 pages (root categories)
- Levels 1-10: ~9.4M total pages
- ~77% articles, ~23% categories
- Contains duplicate titles at different levels

**Supporting Tables:**
- `sass_roots`: Maps root_id to root category names
- `sass_cycles`: Tracks detected cycles during build
- `sass_filter_patterns`: Category exclusion patterns

**Console Logging:**
- Progress report after each level completed
- Shows: pages found, pages added, filtered count, elapsed time
- Final summary with level distribution

### Phase 1B: Deduplicate and Clean Titles (MySQL)
**Script:** `convert_sass_page_clean.sql`  
**Output:** `sass_page_clean` (~2.1M representatives)

**Prerequisites:**
- `sass_page` table from Phase 1A

**Process:**
1. Applies 4-step title normalization to all pages
2. Selects ONE representative per unique title using priority:
   - **First:** Original hierarchy pages (levels 0-2) preserved as representatives
   - **Then:** Deepest in hierarchy (highest page_dag_level)
   - **Then:** Categories over articles (page_is_leaf = 0)
   - **Finally:** Lowest page_id (oldest page)
3. Creates `sass_page_clean` with representatives only
4. Repairs orphan references (children pointing to non-representative parents)
5. Removes all self-reference loops

**Procedure:**
- `ConvertSASSPageCleanSimple()`
- No parameters - uses optimized defaults

**Title Cleaning (4 Steps):**
1. Normalize whitespace and underscores
2. Remove parenthetical content
3. Basic punctuation handling
4. Trim leading/trailing underscores

**Orphan Repair:**
- Detects children pointing to missing parents (after deduplication)
- Maps orphaned parent_id to its representative
- Updates child.page_parent_id to representative
- Removes any resulting self-references (page_id = page_parent_id)

**Schema:**
- Same as `sass_page`
- No duplicates, cleaned titles
- All parent references point to valid representatives

**Console Logging:**
- Progress report during deduplication
- Orphan repair statistics (standard orphans fixed, self-references removed)
- Final summary with representative count and validation

### Phase 1C: Filter Weak Branches (MySQL)
**Script:** `convert_sass_page_clean.sql` (integrated)  
**Output:** `sass_page_clean` filtered (~1.85M representatives)

**Prerequisites:**
- `sass_page_clean` from Phase 1B with repaired references

**Process:**
1. Count direct children for each category (page_is_leaf = 0)
2. Process levels 10→3 (bottom-up), protecting levels 0-2
3. For categories with <5 direct children:
   - Reparent children to grandparent (move up one level)
   - Convert weak category to leaf (page_is_leaf = 1)
4. Iterate 2-3 times until stable (each pass may create new weak categories)
5. Delete orphaned leaves if no surviving ancestor exists

**Procedure:**
- `FilterWeakBranches()` or integrated into `ConvertSASSPageCleanSimple()`

**Filtering Criteria:**
- **Categories:** Must have ≥5 direct children to remain a category
- **Articles:** Always kept (have no children)
- **Levels 0-2:** Never filtered (protected)

**Expected Impact:**
- ~250k weak categories converted to leaves
- ~12% reduction in total representatives (2.1M → 1.85M)
- Stronger, more navigable hierarchy
- Eliminates sparse, underdeveloped branches

**Console Logging:**
- Iteration number (1, 2, 3)
- Categories filtered per level per iteration
- Children reparented count
- Convergence status (stable when no changes)
- Final filtered representative count

### Phase 1D: Export to PostgreSQL (MySQL → CSV → PostgreSQL)
**Script:** `export_sass_page_clean_to_postgres.sql`  
**Output:** CSV files containing filtered representatives (~200k rows each, ~9-10 files)

**Prerequisites:**
- Filtered `sass_page_clean` from Phase 1C
- MySQL `secure_file_priv` directory configured

**Process:**
1. Exports filtered representative pages only
2. Splits into chunks of ~200k rows each
3. Creates 9-10 CSV files with proper PostgreSQL escaping
4. Each file includes header row

**Procedure:**
- `ExportSASSPageClean(export_directory, chunk_size)`
- Default: `('/path/to/export/', 200000)` - creates ~9-10 files

**Export Format:**
- CSV with header: page_id, page_title, page_parent_id, page_root_id, page_dag_level, page_is_leaf
- Double-quote escaping for PostgreSQL compatibility
- UTF-8 encoding

**Console Logging:**
- Export configuration (directory, chunk size, total rows)
- Progress report after each chunk (chunk #, rows, elapsed time)
- Final summary (total files, total rows, completion time)

**PostgreSQL Import:**
```sql
-- Import representatives into PostgreSQL
COPY sass_page_clean FROM '/path/to/chunk_*.csv' 
  WITH (FORMAT csv, HEADER true);

-- Build indices after import
CREATE INDEX idx_title ON sass_page_clean(page_title);
CREATE INDEX idx_parent ON sass_page_clean(page_parent_id);
CREATE INDEX idx_root ON sass_page_clean(page_root_id);
CREATE INDEX idx_level ON sass_page_clean(page_dag_level);
CREATE INDEX idx_leaf ON sass_page_clean(page_is_leaf);
```

## Data Flow Summary

```
Wikipedia dumps (page, categorylinks)
    ↓
wiki_top3_levels (pre-computed)
    ↓
[Phase 1A] build_sass_page.sql
    ↓
sass_page (9.4M rows with duplicates)
    ↓
[Phase 1B] convert_sass_page_clean.sql
    ↓
sass_page_clean (2.1M representatives, deduplicated)
    ↓
[Phase 1C] convert_sass_page_clean.sql
    ↓
sass_page_clean (1.85M representatives, filtered)
    ↓
[Phase 1D] export_sass_page_clean_to_postgres.sql
    ↓
CSV files (9-10 files, ~200k rows each)
    ↓
PostgreSQL (1.85M representatives for analysis)
```

## Build Instructions

### Step 1: Build SASS Page Tree
```sql
-- Standard build with filtering (levels 0-10)
CALL BuildSASSPageTreeFiltered(0, 10, 1);

-- Verify results
SELECT page_dag_level, COUNT(*) 
FROM sass_page 
GROUP BY page_dag_level 
ORDER BY page_dag_level;
```

### Step 2: Deduplicate, Clean, and Filter
```sql
-- Single procedure handles deduplication + filtering
CALL ConvertSASSPageCleanSimple();

-- Verify representatives
SELECT COUNT(*) as total_representatives 
FROM sass_page_clean;

-- Check data quality
SELECT COUNT(*) as self_references 
FROM sass_page_clean 
WHERE page_id = page_parent_id AND page_dag_level > 0;
-- Should return 0

SELECT COUNT(*) as orphans 
FROM sass_page_clean c 
LEFT JOIN sass_page_clean p ON c.page_parent_id = p.page_id 
WHERE c.page_dag_level > 0 AND p.page_id IS NULL;
-- Should return 0

-- Category size distribution
SELECT 
  CASE 
    WHEN child_count = 0 THEN '0 (leaves)'
    WHEN child_count < 5 THEN '1-4 (should be zero after filtering)'
    WHEN child_count < 20 THEN '5-19'
    WHEN child_count < 100 THEN '20-99'
    ELSE '100+'
  END as size_range,
  COUNT(*) as category_count
FROM (
  SELECT p.page_id, COUNT(c.page_id) as child_count
  FROM sass_page_clean p
  LEFT JOIN sass_page_clean c ON p.page_id = c.page_parent_id
  WHERE p.page_is_leaf = 0
  GROUP BY p.page_id
) counts
GROUP BY size_range;
```

### Step 3: Export to PostgreSQL
```sql
-- Export with default chunk size (200k rows, ~9-10 files)
CALL ExportSASSPageClean('/private/tmp/mysql_export/', 200000);

-- Verify files created in export directory
-- Files named: sass_page_clean_representatives_chunk_000000000001.csv, etc.
```

### Step 4: Import to PostgreSQL
```bash
# Create table in PostgreSQL
psql -U postgres -d mydb -c "
CREATE TABLE sass_page_clean (
  page_id INTEGER PRIMARY KEY,
  page_title VARCHAR(255) NOT NULL,
  page_parent_id INTEGER NOT NULL,
  page_root_id INTEGER NOT NULL,
  page_dag_level INTEGER NOT NULL,
  page_is_leaf BOOLEAN NOT NULL
);
"

# Import all chunk files
for file in /private/tmp/mysql_export/sass_page_clean_representatives_chunk_*.csv; do
  psql -U postgres -d mydb -c "\COPY sass_page_clean FROM '$file' WITH (FORMAT csv, HEADER true);"
done

# Build indices
psql -U postgres -d mydb -c "
CREATE INDEX idx_title ON sass_page_clean(page_title);
CREATE INDEX idx_parent ON sass_page_clean(page_parent_id);
CREATE INDEX idx_root ON sass_page_clean(page_root_id);
CREATE INDEX idx_level ON sass_page_clean(page_dag_level);
CREATE INDEX idx_leaf ON sass_page_clean(page_is_leaf);
"
```

## System Requirements

**MySQL Server:**
- 64GB RAM recommended
- 500GB+ storage for Wikipedia dumps and working tables
- MySQL 8.0+ or MariaDB 10.5+

**PostgreSQL Server:**
- 32GB RAM recommended  
- 200GB+ storage for final data warehouse
- PostgreSQL 13+

**Estimated Runtimes (Mac Studio M4 64GB):**
- Phase 1A (Build tree): 45-60 minutes
- Phase 1B (Deduplicate): 15-20 minutes
- Phase 1C (Filter weak branches): 10-15 minutes
- Phase 1D (Export): 5-10 minutes
- PostgreSQL import: 40-50 minutes
- **Total: ~2-2.5 hours**

## Key Design Decisions

1. **Level Limit:** Stops at level 10 (not deeper) to balance coverage vs. noise
2. **Hierarchy Preservation:** Levels 0-2 pages always become representatives (maintains original structure)
3. **Deduplication Strategy:** One representative per unique cleaned title (reduces 9.4M → 2.1M)
4. **Weak Branch Filtering:** Categories with <5 children converted to leaves (2.1M → 1.85M)
5. **No Self-References:** All self-reference loops removed during orphan repair
6. **No Identity Mapping:** Removed sass_identity_pages table (99.9% redundant 1:1 mappings)
7. **Export Format:** ~9-10 files of 200k rows each for manageable import sizes
8. **No Resumability:** Builds restart from scratch on failure (acceptable for <2.5 hour runtime)
9. **Console Logging:** Progress tracking via SELECT statements, no persistent tracking tables

## Data Quality

**Expected Results:**
- ~1.85M representative pages in sass_page_clean (after filtering)
- Zero self-references (page_id = page_parent_id where level > 0)
- Zero orphans after repair
- All level 0-2 pages preserved as representatives
- All categories (except levels 0-2) have ≥5 children
- ~85% articles, ~15% categories (shifted by weak branch filtering)

**Known Limitations:**
- Some pages may appear at wrong level if filtering removes true parent
- Edge cases with complex Unicode characters in titles may not normalize perfectly
- Disambiguation pages with identical cleaned titles arbitrarily select one representative
- Very deep specialization (levels 9-10) may be over-pruned if parent categories are weak

## Future Phases (Not Yet Implemented)

### Phase 2: Build Lexical Search Mapping
- Source: `redirect` table → `sass_lexical_link`
- Maps alternative titles to representative pages
- Will be refactored to work with sass_page_clean only

### Phase 3: Build Associative Link Network  
- Source: `pagelinks` + `categorylinks` → `sass_associative_link`
- Links between representative pages only
- Will be refactored to work with sass_page_clean o
