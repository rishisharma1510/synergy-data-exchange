-- =============================================================================
-- SQL Server initialization for large data operations (PATH 3: AWS Batch)
-- Optimizes memory, tempdb, and I/O settings for >60GB datasets
-- =============================================================================

-- Configure memory based on container allocation
-- This will be dynamically set based on EC2 instance memory
DECLARE @TotalMemoryMB INT = (SELECT physical_memory_kb / 1024 FROM sys.dm_os_sys_info);
DECLARE @SQLMemoryMB INT = @TotalMemoryMB * 0.8;  -- Reserve 80% for SQL Server

-- Minimum memory (4GB) and maximum based on instance
IF @SQLMemoryMB < 4096 SET @SQLMemoryMB = 4096;
IF @SQLMemoryMB > 131072 SET @SQLMemoryMB = 131072;  -- Cap at 128GB

EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;

EXEC sp_configure 'max server memory (MB)', @SQLMemoryMB;
RECONFIGURE;

-- Optimize for parallel operations
EXEC sp_configure 'max degree of parallelism', 0;  -- Use all cores
EXEC sp_configure 'cost threshold for parallelism', 5;
RECONFIGURE;

-- Enable backup compression by default
EXEC sp_configure 'backup compression default', 1;
RECONFIGURE;

-- Optimize tempdb for large operations
-- Add multiple tempdb files based on CPU count (up to 8)
DECLARE @NumCPU INT = (SELECT cpu_count FROM sys.dm_os_sys_info);
DECLARE @NumFiles INT = CASE WHEN @NumCPU > 8 THEN 8 ELSE @NumCPU END;
DECLARE @i INT = 2;
DECLARE @SQL NVARCHAR(MAX);

WHILE @i <= @NumFiles
BEGIN
    SET @SQL = 'ALTER DATABASE tempdb ADD FILE (
        NAME = tempdev' + CAST(@i AS NVARCHAR(2)) + ',
        FILENAME = ''/data/tempdb/tempdb' + CAST(@i AS NVARCHAR(2)) + '.ndf'',
        SIZE = 1024MB,
        FILEGROWTH = 512MB
    )';
    EXEC sp_executesql @SQL;
    SET @i = @i + 1;
END

-- Configure trace flags for performance
DBCC TRACEON(1117, -1);  -- Uniform extent allocation
DBCC TRACEON(1118, -1);  -- Full extents only
DBCC TRACEON(3226, -1);  -- Suppress backup success messages

PRINT 'SQL Server optimized for large data operations';
PRINT 'Memory allocated: ' + CAST(@SQLMemoryMB AS VARCHAR(10)) + ' MB';
PRINT 'Tempdb files: ' + CAST(@NumFiles AS VARCHAR(2));
