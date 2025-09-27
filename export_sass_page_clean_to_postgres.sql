-- ========================================
-- SASS Page Clean PostgreSQL Import Script
-- Imports SASS representative page data into PostgreSQL data-stack
-- Optimized for Docker PostgreSQL with pgvector and Apache AGE
-- UPDATED: Index-free optimization, representative pages, Docker integration
-- ========================================

-- Enable timing and progress reporting
\timing on
\set ECHO all

-- Set client encoding for proper character handling
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

-- ========================================
-- TABLE CREATION (INDEX-FREE OPTIMIZATION)
-- ========================================

-- Create sass_page_clean table WITHOUT INDEXES (performance optimization)
CREATE TABLE IF NOT EXISTS sass_page_clean (
    page_id BIGINT NOT NULL,
    page_title VARCHAR(255) NOT NULL,
    page_parent_id INTEGER NOT NULL,
    page_root_id INTEGER NOT NULL,
    page_dag_level INTEGER NOT NULL,
    page_is_leaf BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create PERSISTENT staging table for import (NOT TEMP)
DROP TABLE IF EXISTS sass_page_clean_staging;
CREATE TABLE sass_page_clean_staging (
    page_id BIGINT,
    page_title VARCHAR(255),
    page_parent_id INTEGER,
    page_root_id INTEGER,
    page_dag_level INTEGER,
    page_is_leaf BOOLEAN
);

-- ========================================
-- IMPORT PROCEDURES
-- ========================================

-- Function to import from CSV file (representative pages)
CREATE OR REPLACE FUNCTION import_sass_page_clean_from_csv(
    file_path TEXT,
    chunk_size INTEGER DEFAULT 100000
) RETURNS TABLE (
    status TEXT,
    rows_imported BIGINT,
    total_time_ms BIGINT
) AS $func$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    rows_count BIGINT;
    import_command TEXT;
BEGIN
    start_time := clock_timestamp();
    
    -- Build dynamic COPY command
    import_command := format('COPY sass_page_clean_staging FROM %L WITH (FORMAT csv, HEADER true, DELIMITER %L)', 
                           file_path, ',');
    
    -- Clear staging table
    TRUNCATE sass_page_clean_staging;
    
    -- Import data to staging table
    EXECUTE import_command;
    GET DIAGNOSTICS rows_count = ROW_COUNT;
    
    -- Data validation and cleaning
    UPDATE sass_page_clean_staging 
    SET page_title = trim(page_title)
    WHERE page_title IS NOT NULL;
    
    -- Remove invalid records
    DELETE FROM sass_page_clean_staging 
    WHERE page_id IS NULL 
       OR page_title IS NULL 
       OR page_title = '';
    
    -- Insert into main table (no conflict resolution - index-free design)
    INSERT INTO sass_page_clean (
        page_id, page_title, page_parent_id, 
        page_root_id, page_dag_level, page_is_leaf
    )
    SELECT 
        page_id, page_title, page_parent_id,
        page_root_id, page_dag_level, page_is_leaf
    FROM sass_page_clean_staging;
    
    GET DIAGNOSTICS rows_count = ROW_COUNT;
    end_time := clock_timestamp();
    
    RETURN QUERY SELECT 
        'SUCCESS'::TEXT,
        rows_count,
        EXTRACT(EPOCH FROM (end_time - start_time))::BIGINT * 1000;
END;
$func$ LANGUAGE plpgsql;

-- Enhanced chunked import for representative pages
CREATE OR REPLACE FUNCTION import_sass_page_clean_chunked(
    base_path TEXT,
    file_pattern TEXT DEFAULT 'sass_page_clean_representatives_chunk_%s.csv',
    max_chunks INTEGER DEFAULT 100
) RETURNS TABLE (
    chunk_number INTEGER,
    status TEXT,
    rows_imported BIGINT,
    chunk_time_ms BIGINT,
    cumulative_rows BIGINT
) AS $func$
DECLARE
    chunk_counter INTEGER := 1;
    chunk_file TEXT;
    import_result RECORD;
    total_rows BIGINT := 0;
    chunk_exists BOOLEAN;
BEGIN
    WHILE chunk_counter <= max_chunks LOOP
        -- Format chunk file path with 12-digit numbering
        chunk_file := base_path || format(file_pattern, lpad(chunk_counter::TEXT, 12, '0'));
        
        -- Check if file exists (basic validation)
        BEGIN
            PERFORM pg_stat_file(chunk_file);
            chunk_exists := TRUE;
        EXCEPTION
            WHEN OTHERS THEN
                chunk_exists := FALSE;
        END;
        
        EXIT WHEN NOT chunk_exists;
        
        -- Import chunk
        BEGIN
            SELECT * INTO import_result 
            FROM import_sass_page_clean_from_csv(chunk_file);
            
            total_rows := total_rows + import_result.rows_imported;
            
            RETURN QUERY SELECT 
                chunk_counter,
                import_result.status,
                import_result.rows_imported,
                import_result.total_time_ms,
                total_rows;
                
        EXCEPTION
            WHEN OTHERS THEN
                RETURN QUERY SELECT 
                    chunk_counter,
                    ('ERROR: ' || SQLERRM)::TEXT,
                    0::BIGINT,
                    0::BIGINT,
                    total_rows;
        END;
        
        chunk_counter := chunk_counter + 1;
    END LOOP;
    
    -- Final summary
    RETURN QUERY SELECT 
        0 AS chunk_number,
        ('COMPLETED: ' || total_rows::TEXT || ' representative pages imported')::TEXT,
        total_rows,
        0::BIGINT,
        total_rows;
END;
$func$ LANGUAGE plpgsql;

-- Drop existing function to avoid conflicts
DROP FUNCTION IF EXISTS test_single_import(text);

-- Test single file import
CREATE OR REPLACE FUNCTION test_single_import(file_path TEXT)
RETURNS TABLE (
    test_status TEXT,
    file_readable BOOLEAN,
    sample_rows BIGINT,
    estimated_total BIGINT
) AS $
DECLARE
    test_count BIGINT;
BEGIN
    -- Test file readability
    BEGIN
        TRUNCATE sass_page_clean_staging;
        EXECUTE format('COPY sass_page_clean_staging FROM %L WITH (FORMAT csv, HEADER true, DELIMITER %L)', 
                      file_path, ',');
        GET DIAGNOSTICS test_count = ROW_COUNT;
        
        RETURN QUERY SELECT 
            'SUCCESS'::TEXT,
            TRUE,
            test_count,
            test_count * 100; -- Rough estimate
            
    EXCEPTION
        WHEN OTHERS THEN
            RETURN QUERY SELECT 
                ('ERROR: ' || SQLERRM)::TEXT,
                FALSE,
                0::BIGINT,
                0::BIGINT;
    END;
END;
$ LANGUAGE plpgsql;

-- Validate representative pages import
CREATE OR REPLACE FUNCTION validate_sass_page_clean_import()
RETURNS TABLE (
    validation_type TEXT,
    metric TEXT,
    value BIGINT,
    expected_range TEXT,
    status TEXT
) AS $func$
DECLARE
    total_count BIGINT;
    level_distribution TEXT;
BEGIN
    SELECT COUNT(*) INTO total_count FROM sass_page_clean;
    
    -- Total count validation (representative pages expected ~9.16M)
    RETURN QUERY SELECT 
        'Row Count'::TEXT,
        'Total Representative Pages'::TEXT,
        total_count,
        '9.0M - 9.5M'::TEXT,
        CASE 
            WHEN total_count BETWEEN 9000000 AND 9500000 THEN 'PASS'
            ELSE 'REVIEW'
        END;
    
    -- DAG level distribution
    RETURN QUERY 
    SELECT 
        'DAG Levels'::TEXT,
        ('Level ' || page_dag_level::TEXT)::TEXT,
        COUNT(*)::BIGINT,
        'Varies by level'::TEXT,
        'INFO'::TEXT
    FROM sass_page_clean 
    GROUP BY page_dag_level 
    ORDER BY page_dag_level;
    
    -- Leaf node distribution
    RETURN QUERY
    SELECT 
        'Leaf Distribution'::TEXT,
        CASE WHEN page_is_leaf THEN 'Leaf Pages' ELSE 'Non-Leaf Pages' END,
        COUNT(*)::BIGINT,
        'Expected mix'::TEXT,
        'INFO'::TEXT
    FROM sass_page_clean 
    GROUP BY page_is_leaf;
    
    -- Data quality checks
    RETURN QUERY
    SELECT 
        'Data Quality'::TEXT,
        'NULL/Empty Titles'::TEXT,
        COUNT(*)::BIGINT,
        '0'::TEXT,
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM sass_page_clean 
    WHERE page_title IS NULL OR page_title = '';
    
    -- ID range validation
    RETURN QUERY
    SELECT 
        'ID Range'::TEXT,
        'Min-Max Page ID'::TEXT,
        (MAX(page_id) - MIN(page_id))::BIGINT,
        'Large range expected'::TEXT,
        'INFO'::TEXT
    FROM sass_page_clean;
END;
$func$ LANGUAGE plpgsql;

-- Optimize table after import (create indexes)
CREATE OR REPLACE FUNCTION optimize_sass_page_clean_table()
RETURNS TABLE (
    optimization_step TEXT,
    execution_time_sec NUMERIC,
    status TEXT
) AS $func$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    -- Primary Key Index
    start_time := clock_timestamp();
    
    BEGIN
        EXECUTE 'CREATE UNIQUE INDEX CONCURRENTLY idx_sass_page_clean_pkey ON sass_page_clean (page_id)';
        EXECUTE 'ALTER TABLE sass_page_clean ADD CONSTRAINT sass_page_clean_pkey PRIMARY KEY USING INDEX idx_sass_page_clean_pkey';
        
        end_time := clock_timestamp();
        RETURN QUERY SELECT 
            'Primary Key Creation'::TEXT,
            EXTRACT(EPOCH FROM (end_time - start_time))::NUMERIC,
            'SUCCESS'::TEXT;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN QUERY SELECT 
                'Primary Key Creation'::TEXT,
                0::NUMERIC,
                ('ERROR: ' || SQLERRM)::TEXT;
    END;
    
    -- Secondary Indexes (can be run in parallel)
    FOR i IN 1..6 LOOP
        start_time := clock_timestamp();
        
        BEGIN
            CASE i
                WHEN 1 THEN
                    EXECUTE 'CREATE INDEX CONCURRENTLY idx_sass_page_clean_title ON sass_page_clean (page_title)';
                WHEN 2 THEN
                    EXECUTE 'CREATE INDEX CONCURRENTLY idx_sass_page_clean_parent ON sass_page_clean (page_parent_id)';
                WHEN 3 THEN
                    EXECUTE 'CREATE INDEX CONCURRENTLY idx_sass_page_clean_root ON sass_page_clean (page_root_id)';
                WHEN 4 THEN
                    EXECUTE 'CREATE INDEX CONCURRENTLY idx_sass_page_clean_level ON sass_page_clean (page_dag_level)';
                WHEN 5 THEN
                    EXECUTE 'CREATE INDEX CONCURRENTLY idx_sass_page_clean_leaf ON sass_page_clean (page_is_leaf)';
                WHEN 6 THEN
                    EXECUTE 'CREATE INDEX CONCURRENTLY idx_sass_page_clean_created ON sass_page_clean (created_at)';
            END CASE;
            
            end_time := clock_timestamp();
            RETURN QUERY SELECT 
                ('Secondary Index ' || i::TEXT)::TEXT,
                EXTRACT(EPOCH FROM (end_time - start_time))::NUMERIC,
                'SUCCESS'::TEXT;
        EXCEPTION
            WHEN OTHERS THEN
                RETURN QUERY SELECT 
                    ('Secondary Index ' || i::TEXT)::TEXT,
                    0::NUMERIC,
                    ('ERROR: ' || SQLERRM)::TEXT;
        END;
    END LOOP;
    
    -- Final Analysis
    start_time := clock_timestamp();
    EXECUTE 'ANALYZE sass_page_clean';
    end_time := clock_timestamp();
    
    RETURN QUERY SELECT 
        'Table Analysis'::TEXT,
        EXTRACT(EPOCH FROM (end_time - start_time))::NUMERIC,
        'SUCCESS'::TEXT;
END;
$func$ LANGUAGE plpgsql;

-- ========================================
-- DOCKER INTEGRATION & USAGE INSTRUCTIONS
-- ========================================

-- DOCKER USAGE INSTRUCTIONS:
--
-- 1. Prepare PostgreSQL container:
--    docker exec -it safe-ai-data-postgres psql -U postgres -d mydb
--
-- 2. Copy the script to the container:
--    docker cp import_sass_page_clean_into_postgres.sql safe-ai-data-postgres:/tmp/
--
-- 3. Run the script:
--    docker exec -i safe-ai-data-postgres psql -U postgres -d mydb -f /tmp/import_sass_page_clean_into_postgres.sql
--
-- 4. Copy data files to container:
--    docker cp /private/tmp/mysql_export/. safe-ai-data-postgres:/tmp/mysql_export/
--
-- 5. Fix file permissions (CRITICAL):
--    docker exec -u root safe-ai-data-postgres chmod 644 /tmp/mysql_export/*.csv
--
-- 6. Test single file first:
--    docker exec -i safe-ai-data-postgres psql -U postgres -d mydb -c "SELECT * FROM test_single_import('/tmp/mysql_export/sass_page_clean_representatives_chunk_000000000001.csv');"
--
-- 7. Run chunked import (estimated 45-60 minutes):
--    docker exec -i safe-ai-data-postgres psql -U postgres -d mydb -c "SELECT * FROM import_sass_page_clean_chunked('/tmp/mysql_export/');"
--
-- 8. Validate import:
--    docker exec -i safe-ai-data-postgres psql -U postgres -d mydb -c "SELECT * FROM validate_sass_page_clean_import();"
--
-- 9. Optimize after import (estimated 30-45 minutes):
--    docker exec -i safe-ai-data-postgres psql -U postgres -d mydb -c "SELECT * FROM optimize_sass_page_clean_table();"
--
-- ALTERNATIVE SINGLE FILE IMPORT:
--    SELECT * FROM import_sass_page_clean_from_csv('/tmp/mysql_export/sass_page_clean_representatives_1_0pct.csv');
--
-- PERFORMANCE ESTIMATES (Representative Pages):
-- - Data Import: 45-60 minutes (~9.16M representative pages)
-- - Index Creation: 30-45 minutes (can run in parallel)
-- - Total Migration Time: 1.5-2 hours
-- - Expected Row Count: ~9.16M representative pages

-- ========================================
-- PARALLEL INDEX CREATION COMMANDS
-- ========================================

-- FOR PARALLEL INDEX CREATION (Run in separate sessions):
--
-- Session 1 (Primary Key - MUST complete first):
-- CREATE UNIQUE INDEX CONCURRENTLY idx_sass_page_clean_pkey ON sass_page_clean (page_id);
-- ALTER TABLE sass_page_clean ADD CONSTRAINT sass_page_clean_pkey PRIMARY KEY USING INDEX idx_sass_page_clean_pkey;
--
-- Session 2:
-- CREATE INDEX CONCURRENTLY idx_sass_page_clean_title ON sass_page_clean (page_title);
--
-- Session 3:
-- CREATE INDEX CONCURRENTLY idx_sass_page_clean_parent ON sass_page_clean (page_parent_id);
--
-- Session 4:
-- CREATE INDEX CONCURRENTLY idx_sass_page_clean_root ON sass_page_clean (page_root_id);
--
-- Session 5:
-- CREATE INDEX CONCURRENTLY idx_sass_page_clean_level ON sass_page_clean (page_dag_level);
--
-- Session 6:
-- CREATE INDEX CONCURRENTLY idx_sass_page_clean_leaf ON sass_page_clean (page_is_leaf);
--
-- Final step (after all indexes complete):
-- ANALYZE sass_page_clean;

-- ========================================
-- COMPLETION MESSAGE
-- ========================================

\echo 'SASS Page Clean PostgreSQL Import Script Loaded Successfully'
\echo ''
\echo 'OPTIMIZED FOR REPRESENTATIVE PAGES (~9.16M rows)'
\echo 'INDEX-FREE IMPORT STRATEGY FOR MAXIMUM PERFORMANCE'
\echo ''
\echo 'Available functions:'
\echo '  - import_sass_page_clean_from_csv(file_path, chunk_size)'
\echo '  - import_sass_page_clean_chunked(base_path, file_pattern, max_chunks)'
\echo '  - test_single_import(file_path)'
\echo '  - validate_sass_page_clean_import()'
\echo '  - optimize_sass_page_clean_table()'
\echo ''
\echo 'Tables created:'
\echo '  - sass_page_clean (main table, index-free until optimization)'
\echo '  - sass_page_clean_staging (persistent import staging)'
\echo ''
\echo 'CRITICAL: Run this command to fix file permissions:'
\echo 'docker exec -u root safe-ai-data-postgres chmod 644 /tmp/mysql_export/*.csv'
\echo ''
\echo 'Expected performance:'
\echo '  - Import: 45-60 minutes'
\echo '  - Index creation: 30-45 minutes'
\echo '  - Total: 1.5-2 hours'
\echo ''
\echo 'Ready for representative pages import!'
