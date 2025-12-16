# Lightning IMPORT INTO Backend Design Document

| Author(s) | Created | Last Updated | Status |
|-----------|---------|--------------|--------|
| TiDB Team | 2025-12-11 | 2025-12-11 | Draft |

## Table of Contents

- [1. Introduction](#1-introduction)
  - [1.1 Background](#11-background)
  - [1.2 Motivation](#12-motivation)
  - [1.3 Goals](#13-goals)
  - [1.4 Non-Goals](#14-non-goals)
- [2. Architecture Overview](#2-architecture-overview)
  - [2.1 High-Level Architecture](#21-high-level-architecture)
  - [2.2 Component Diagram](#22-component-diagram)
- [3. Import SDK (`pkg/importsdk`)](#3-import-sdk-pkgimportsdk)
  - [3.1 Overview](#31-overview)
  - [3.2 Core Interfaces](#32-core-interfaces)
  - [3.3 FileScanner](#33-filescanner)
  - [3.4 SQLGenerator](#34-sqlgenerator)
  - [3.5 JobManager](#35-jobmanager)
  - [3.6 Configuration Options](#36-configuration-options)
- [4. Lightning Import Into Backend (`pkg/lightning/importinto`)](#4-lightning-import-into-backend-pkglightningimportinto)
  - [4.1 Overview](#41-overview)
  - [4.2 Importer](#42-importer)
  - [4.3 Job Orchestrator](#43-job-orchestrator)
  - [4.4 Job Submitter](#44-job-submitter)
  - [4.5 Job Monitor](#45-job-monitor)
  - [4.6 Checkpoint Manager](#46-checkpoint-manager)
  - [4.7 Precheck System](#47-precheck-system)
- [5. Data Flow](#5-data-flow)
  - [5.1 Import Workflow](#51-import-workflow)
  - [5.2 Checkpoint Recovery Flow](#52-checkpoint-recovery-flow)
  - [5.3 Error Handling Flow](#53-error-handling-flow)
- [6. API Reference](#6-api-reference)
  - [6.1 SDK API](#61-sdk-api)
  - [6.2 Import Options](#62-import-options)
  - [6.3 Job Status](#63-job-status)
- [7. Configuration](#7-configuration)
  - [7.1 Lightning Configuration Mapping](#71-lightning-configuration-mapping)
  - [7.2 Checkpoint Configuration](#72-checkpoint-configuration)
- [8. Testing Strategy](#8-testing-strategy)
- [9. Future Enhancements](#9-future-enhancements)
- [10. References](#10-references)

---

## 1. Introduction

### 1.1 Background

TiDB Lightning is a high-performance data import tool that supports multiple backends:
- **Local Backend**: Directly generates SST files and ingests them into TiKV
- **TiDB Backend**: Imports data via SQL INSERT statements

TiDB 7.x introduced the `IMPORT INTO` SQL statement, which provides a serverless, distributed import capability built directly into TiDB. This new mechanism offers several advantages over traditional Lightning backends.

### 1.2 Motivation

The motivation for creating a new "import-into" backend for Lightning includes:

1. **Cloud-Native Design**: Well-suited for next-gen cloud environments.

2. **Unified Import Experience**: Users can continue using familiar Lightning configurations and workflows while benefiting from TiDB's native `IMPORT INTO` capabilities.

3. **Distributed Execution**: `IMPORT INTO` leverages TiDB's distributed task framework, providing better scalability and resource utilization compared to single-node Lightning.

4. **Simplified Operations**: No need to deploy and manage a separate Lightning process; the import workload runs within the TiDB cluster.

5. **Better Integration**: Native support for TiDB features like table mode, analyze and resource control.

### 1.3 Goals

- Provide a Lightning backend that delegates import work to TiDB's `IMPORT INTO` statement
- Maintain compatibility with existing Lightning configuration files and workflows
- Support checkpoint-based recovery for interrupted imports
- Enable concurrent table imports with configurable parallelism
- Support all major data formats: CSV, SQL, Parquet
- Provide comprehensive progress monitoring and error reporting

### 1.4 Non-Goals

- Modifying the core `IMPORT INTO` execution engine
- Supporting all Lightning's physical import mode features (like pre duplicate detection)

---

## 2. Architecture Overview

### 2.1 High-Level Architecture

```
┌───────────────────────────────────────────────────────────────────────────┐
│                      External Storage (Data Source)                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │ Local FS     │  │ Amazon S3    │  │ Google GCS   │  │ Azure Blob   │   │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘   │
└───────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                              TiDB Lightning                                │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                     Lightning Server (Existing)                     │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────────┐  │   │
│  │  │ Local       │  │ TiDB        │  │ IMPORT INTO Backend (New)   │  │   │
│  │  │ Backend     │  │ Backend     │  │                             │  │   │
│  │  └─────────────┘  └─────────────┘  └─────────────────────────────┘  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                           Import SDK (New)                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                      │
│  │ FileScanner  │  │ SQLGenerator │  │ JobManager   │                      │
│  └──────────────┘  └──────────────┘  └──────────────┘                      │
└────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                              TiDB Cluster                                 │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │                        IMPORT INTO Engine                          │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │   │
│  │  │ Parser &     │  │ Distributed  │  │ Data         │              │   │
│  │  │ Planner      │  │ Scheduler    │  │ Importer     │              │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘              │   │
│  └────────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                         TiKV Cluster (Data Sink)                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │ TiKV Node 1  │  │ TiKV Node 2  │  │ TiKV Node 3  │  │    ...       │   │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘   │
└───────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Component Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     pkg/lightning/importinto                                 │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                           Importer                                      │ │
│  │  - Entry point for import operations                                    │ │
│  │  - Manages lifecycle and configuration                                  │ │
│  │  - Coordinates all sub-components                                       │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                         │
│           ┌────────────────────────┼────────────────────────┐               │
│           ▼                        ▼                        ▼               │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐         │
│  │ JobOrchestrator │    │ CheckpointMgr   │    │ PrecheckRunner  │         │
│  │                 │    │                 │    │                 │         │
│  │ - Submit jobs   │    │ - Persist state │    │ - Version check │         │
│  │ - Wait for done │    │ - Resume logic  │    │ - Checkpoint    │         │
│  │ - Cancel jobs   │    │ - File/MySQL    │    │   validation    │         │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘         │
│           │                                                                  │
│           ├──────────────────────────────────────┐                          │
│           ▼                                      ▼                          │
│  ┌─────────────────┐                    ┌─────────────────┐                 │
│  │  JobSubmitter   │                    │   JobMonitor    │                 │
│  │                 │                    │                 │                 │
│  │ - Build SQL     │                    │ - Poll status   │                 │
│  │ - Submit to DB  │                    │ - Log progress  │                 │
│  │ - Return JobID  │                    │ - Handle errors │                 │
│  └─────────────────┘                    └─────────────────┘                 │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           pkg/importsdk                                      │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                              SDK                                      │   │
│  │  Unified interface combining FileScanner + SQLGenerator + JobManager │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│           ┌────────────────────────┼────────────────────────┐               │
│           ▼                        ▼                        ▼               │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐         │
│  │  FileScanner    │    │  SQLGenerator   │    │   JobManager    │         │
│  │                 │    │                 │    │                 │         │
│  │ - Scan sources  │    │ - Build IMPORT  │    │ - Submit jobs   │         │
│  │ - Get metadata  │    │   INTO SQL      │    │ - Query status  │         │
│  │ - Create DDL    │    │ - Format opts   │    │ - Cancel jobs   │         │
│  │ - Wildcard gen  │    │ - CSV config    │    │ - Group summary │         │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘         │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Import SDK (`pkg/importsdk`)

### 3.1 Overview

The Import SDK is a reusable library that provides low-level operations for interacting with TiDB's `IMPORT INTO` functionality. It is designed to be:

- **Modular**: Each component (FileScanner, SQLGenerator, JobManager) can be used independently
- **Testable**: All components are interface-based for easy mocking
- **Extensible**: New features can be added without modifying existing code

### 3.2 Core Interfaces

```go
// SDK is the main entry point combining all capabilities
type SDK interface {
    FileScanner
    JobManager
    SQLGenerator
    Close() error
}

// FileScanner handles source file discovery and metadata extraction
type FileScanner interface {
    CreateSchemasAndTables(ctx context.Context) error
    CreateSchemaAndTableByName(ctx context.Context, schema, table string) error
    GetTableMetas(ctx context.Context) ([]*TableMeta, error)
    GetTableMetaByName(ctx context.Context, db, table string) (*TableMeta, error)
    GetTotalSize(ctx context.Context) int64
    Close() error
}

// SQLGenerator builds IMPORT INTO SQL statements
type SQLGenerator interface {
    GenerateImportSQL(tableMeta *TableMeta, options *ImportOptions) (string, error)
}

// JobManager handles import job lifecycle
type JobManager interface {
    SubmitJob(ctx context.Context, query string) (int64, error)
    GetJobStatus(ctx context.Context, jobID int64) (*JobStatus, error)
    CancelJob(ctx context.Context, jobID int64) error
    GetGroupSummary(ctx context.Context, groupKey string) (*GroupStatus, error)
    GetJobsByGroup(ctx context.Context, groupKey string) ([]*JobStatus, error)
}
```

### 3.3 FileScanner

The FileScanner component is responsible for:

1. **Source Discovery**: Scanning external storage (S3, GCS, local filesystem) for data files
2. **Metadata Extraction**: Identifying databases, tables, and data files from dump formats
3. **Schema Creation**: Executing DDL statements to create databases and tables
4. **Wildcard Generation**: Creating glob patterns for `IMPORT INTO` file specifications

#### Supported Data Formats

| Format | Extension | Description |
|--------|-----------|-------------|
| SQL | `.sql` | MySQL dump format with INSERT statements |
| CSV | `.csv` | Comma-separated values |
| Parquet | `.parquet` | Apache Parquet columnar format |

#### Wildcard Pattern Generation

The FileScanner generates wildcard patterns for table data files using two strategies:

1. **Mydumper Pattern**: For standard mydumper naming convention (`{schema}.{table}.{seq}.{ext}`)
2. **Prefix/Suffix Pattern**: Generic pattern using longest common prefix and suffix

```go
// Example: mydumper pattern
// Files: db.table.001.csv, db.table.002.csv, db.table.003.csv
// Pattern: db.table.*.csv

// Example: prefix/suffix pattern
// Files: data_part1.csv, data_part2.csv, data_part3.csv
// Pattern: data_part*.csv
```

### 3.4 SQLGenerator

The SQLGenerator builds `IMPORT INTO` SQL statements with support for all import options:

```sql
IMPORT INTO `database`.`table`
FROM 's3://bucket/path/*.csv?access-key=xxx&secret-access-key=yyy'
FORMAT 'csv'
WITH
    THREAD=4,
    DISK_QUOTA='100GB',
    MAX_WRITE_SPEED='100MiB',
    DETACHED,
    GROUP_KEY='lightning-xxx',
    CHARACTER_SET='utf8mb4',
    FIELDS_TERMINATED_BY=',',
    FIELDS_ENCLOSED_BY='"',
    FIELDS_ESCAPED_BY='\\',
    LINES_TERMINATED_BY='\n',
    SKIP_ROWS=1,
    FIELDS_DEFINED_NULL_BY='NULL'
```

#### Import Options Structure

```go
type ImportOptions struct {
    Format                string           // csv, sql, parquet
    CSVConfig             *config.CSVConfig
    Thread                int              // Concurrency level
    DiskQuota             string           // e.g., "100GB"
    MaxWriteSpeed         string           // e.g., "100MiB"
    SplitFile             bool             // Enable file splitting
    RecordErrors          int64            // Max error records to keep
    Detached              bool             // Run asynchronously
    CloudStorageURI       string           // External sorting storage
    GroupKey              string           // Job grouping key
    SkipRows              int              // Header rows to skip
    CharacterSet          string           // Character encoding
    ChecksumTable         string           // Checksum mode
    DisableTiKVImportMode bool             // Disable TiKV import mode
    MaxEngineSize         string           // Max engine size
    DisablePrecheck       bool             // Skip prechecks
    ResourceParameters    string           // Storage credentials
}
```

### 3.5 JobManager

The JobManager handles the lifecycle of import jobs:

#### Job Submission
```go
// Submit a job and get the job ID
jobID, err := sdk.SubmitJob(ctx, importSQL)
```

#### Job Status Monitoring
```go
// Get status of a specific job
status, err := sdk.GetJobStatus(ctx, jobID)

// JobStatus structure
type JobStatus struct {
    JobID          int64
    GroupKey       string
    DataSource     string
    TargetTable    string
    TableID        int64
    Phase          string  // "import", "postprocess"
    Status         string  // "pending", "running", "finished", "failed", "cancelled"
    SourceFileSize string
    ImportedRows   int64
    ResultMessage  string
    CreateTime     time.Time
    StartTime      time.Time
    EndTime        time.Time
    // Progress fields
    ProcessedSize  string
    TotalSize      string
    Percent        string
    Speed          string
    ETA            string
}
```

#### Group Operations
```go
// Get summary for a group of jobs
summary, err := sdk.GetGroupSummary(ctx, groupKey)

// Get all jobs in a group
jobs, err := sdk.GetJobsByGroup(ctx, groupKey)

// GroupStatus structure
type GroupStatus struct {
    GroupKey           string
    TotalJobs          int64
    Pending            int64
    Running            int64
    Completed          int64
    Failed             int64
    Cancelled          int64
    FirstJobCreateTime time.Time
    LastJobUpdateTime  time.Time
}
```

### 3.6 Configuration Options

```go
// SDK configuration options
sdk, err := importsdk.NewImportSDK(ctx, sourcePath, db,
    importsdk.WithConcurrency(4),           // DDL creation concurrency
    importsdk.WithSQLMode(mysql.ModeANSI),  // SQL mode for parsing
    importsdk.WithFilter([]string{"*.*"}),  // Table filter
    importsdk.WithFileRouters(rules),       // Custom file routing
    importsdk.WithRoutes(routes),           // Table routing/renaming
    importsdk.WithCharset("utf8mb4"),       // Character set
    importsdk.WithMaxScanFiles(10000),      // File scan limit
    importsdk.WithSkipInvalidFiles(true),   // Skip problematic files
    importsdk.WithLogger(logger),           // Custom logger
)
```

---

## 4. Lightning Import Into Backend (`pkg/lightning/importinto`)

### 4.1 Overview

The Lightning Import Into Backend provides a complete import solution built on top of the Import SDK. It adds:

- Checkpoint-based recovery
- Concurrent job orchestration
- Progress monitoring and logging
- Precheck validation
- Pause/Resume support

### 4.2 Importer

The `Importer` is the main entry point:

```go
type Importer struct {
    cfg          *config.Config
    db           *sql.DB
    sdk          importsdk.SDK
    logger       log.Logger
    cpMgr        CheckpointManager
    orchestrator JobOrchestrator
    groupKey     string
}

// Create and run an import
importer, err := importinto.NewImporter(ctx, cfg, db)
if err != nil {
    return err
}
defer importer.Close()

err = importer.Run(ctx)
```

#### Lifecycle Methods

| Method | Description |
|--------|-------------|
| `Run(ctx)` | Execute the full import workflow |
| `Pause(ctx)` | Cancel running jobs (can be resumed) |
| `Resume(ctx)` | Resume a paused import |
| `Close()` | Release all resources |

### 4.3 Job Orchestrator

The `JobOrchestrator` coordinates job submission and monitoring:

```go
type JobOrchestrator interface {
    SubmitAndWait(ctx context.Context, tables []*importsdk.TableMeta) error
    Cancel(ctx context.Context) error
}
```

#### Orchestration Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    SubmitAndWait()                               │
│                                                                  │
│  1. For each table (with concurrency limit):                     │
│     ┌────────────────────────────────────────────────────────┐  │
│     │ a. Check checkpoint                                     │  │
│     │    - If finished: skip                                  │  │
│     │    - If running: resume existing job                    │  │
│     │    - Otherwise: submit new job                          │  │
│     │ b. Record submission in checkpoint                      │  │
│     │ c. Add job to active list                               │  │
│     └────────────────────────────────────────────────────────┘  │
│                                                                  │
│  2. Wait for all jobs to complete:                               │
│     ┌────────────────────────────────────────────────────────┐  │
│     │ - Poll job statuses periodically                        │  │
│     │ - Update checkpoints on completion                      │  │
│     │ - Fast-fail on any job failure                          │  │
│     │ - Log progress at configured intervals                  │  │
│     └────────────────────────────────────────────────────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 4.4 Job Submitter

The `JobSubmitter` translates Lightning configuration to `IMPORT INTO` options:

```go
type JobSubmitter interface {
    SubmitTable(ctx context.Context, tableMeta *importsdk.TableMeta) (*ImportJob, error)
    GetGroupKey() string
}
```

#### Configuration Mapping

| Lightning Config | IMPORT INTO Option |
|------------------|-------------------|
| `tikv-importer.disk-quota` | `DISK_QUOTA` |
| `tikv-importer.store-write-bw-limit` | `MAX_WRITE_SPEED` |
| `tikv-importer.engine-mem-cache-size` | `MAX_ENGINE_SIZE` |
| `mydumper.data-character-set` | `CHARACTER_SET` |
| `mydumper.csv.*` | CSV format options |
| `post-restore.checksum` | `CHECKSUM_TABLE` |
| `app.max-error.type` | `RECORD_ERRORS` |
| `app.check-requirements` | `DISABLE_PRECHECK` |

### 4.5 Job Monitor

The `JobMonitor` tracks job progress and handles completion:

```go
type JobMonitor interface {
    WaitForJobs(ctx context.Context, jobs []*ImportJob) error
}
```

#### Monitoring Features

- **Periodic Polling**: Configurable poll interval (default: 2s)
- **Progress Logging**: Configurable log interval (default: 5min)
- **Fast-Fail**: Immediately cancels remaining jobs on failure
- **Checkpoint Updates**: Records completion status for recovery

### 4.6 Checkpoint Manager

The `CheckpointManager` enables recovery from interrupted imports:

```go
type CheckpointManager interface {
    Initialize(ctx context.Context) error
    Get(dbName, tableName string) (*TableCheckpoint, error)
    Update(ctx context.Context, cp *TableCheckpoint) error
    Remove(ctx context.Context, dbName, tableName string) error
    IgnoreError(ctx context.Context, dbName, tableName string) error
    DestroyError(ctx context.Context, dbName, tableName string) error
    GetCheckpoints(ctx context.Context) ([]*TableCheckpoint, error)
    Close() error
}
```

#### Checkpoint States

```
┌─────────┐    Submit    ┌─────────┐    Complete    ┌──────────┐
│ Pending │ ──────────▶  │ Running │ ─────────────▶ │ Finished │
└─────────┘              └─────────┘                └──────────┘
                              │
                              │ Error
                              ▼
                         ┌─────────┐
                         │ Failed  │
                         └─────────┘
```

#### Storage Backends

| Driver | Configuration | Description |
|--------|---------------|-------------|
| `file` | `checkpoint.dsn=/path/to/checkpoint.json` | Local JSON file |
| `mysql` | `checkpoint.dsn=user:pass@tcp(host)/db` | MySQL/TiDB table |

#### Checkpoint Data Structure

```go
type TableCheckpoint struct {
    DBName    string           // Database name
    TableName string           // Table name
    JobID     int64            // TiDB import job ID
    Status    CheckpointStatus // pending/running/finished/failed
    Message   string           // Error message if failed
    GroupKey  string           // Job group identifier
}
```

### 4.7 Precheck System

The precheck system validates the environment before import:

```go
type PrecheckRunner struct {
    checkers []precheck.Checker
}

// Register and run prechecks
runner := NewPrecheckRunner()
runner.Register(NewCheckpointCheckItem(cfg, cpMgr))
runner.Register(NewClusterVersionCheckItem(db))
err := runner.Run(ctx)
```

#### Built-in Prechecks

| Check | Description |
|-------|-------------|
| `CheckCheckpoints` | Validates checkpoint state is resumable |
| `CheckTargetClusterVersion` | Ensures TiDB version >= 7.1.0 |

---

## 5. Data Flow

### 5.1 Import Workflow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           Complete Import Workflow                            │
└──────────────────────────────────────────────────────────────────────────────┘

    User                    Lightning                    TiDB Cluster
      │                         │                             │
      │  Start Import           │                             │
      │ ──────────────────────▶ │                             │
      │                         │                             │
      │                         │  1. Initialize SDK          │
      │                         │  ──────────────────────────▶│
      │                         │                             │
      │                         │  2. Scan Source Files       │
      │                         │  ──────────────────────────▶│
      │                         │     (External Storage)      │
      │                         │                             │
      │                         │  3. Create Schemas/Tables   │
      │                         │  ──────────────────────────▶│
      │                         │     (DDL Execution)         │
      │                         │                             │
      │                         │  4. Run Prechecks           │
      │                         │  ──────────────────────────▶│
      │                         │                             │
      │                         │  5. Submit Import Jobs      │
      │                         │  (for each table)           │
      │                         │  ──────────────────────────▶│
      │                         │     IMPORT INTO ... DETACHED│
      │                         │                             │
      │                         │  6. Poll Job Status         │
      │                         │  ◀─────────────────────────▶│
      │                         │     (until all complete)    │
      │                         │                             │
      │  Import Complete        │                             │
      │ ◀────────────────────── │                             │
      │                         │                             │
```

### 5.2 Checkpoint Recovery Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Checkpoint Recovery Flow                              │
└──────────────────────────────────────────────────────────────────────────────┘

    Previous Run                Current Run
         │                           │
         │  Table A: Finished        │  Load Checkpoints
         │  Table B: Running         │  ──────────────────▶
         │  Table C: Pending         │
         │                           │
         ▼                           │  Table A: Skip (already finished)
    [Checkpoint File]                │  ──────────────────────────────────▶
                                     │
                                     │  Table B: Resume (check job status)
                                     │  ───────────────────────────────────┐
                                     │                                     │
                                     │  ◀──────────────────────────────────┘
                                     │    If still running: wait
                                     │    If finished: update checkpoint
                                     │    If failed: resubmit
                                     │
                                     │  Table C: Submit new job
                                     │  ──────────────────────────────────▶
                                     │
```

### 5.3 Error Handling Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                            Error Handling Flow                                │
└──────────────────────────────────────────────────────────────────────────────┘

    Job A: Running ────▶ Failed ────▶ Detected by Monitor
                                            │
                                            ▼
                                     ┌─────────────────┐
                                     │ Update A's      │
                                     │ checkpoint to   │
                                     │ "failed"        │
                                     └────────┬────────┘
                                              │
                                              ▼
                                     ┌─────────────────┐
                                     │ Cancel remaining│
                                     │ jobs (B, C, ...) │
                                     └────────┬────────┘
                                              │
                                              ▼
                                     ┌─────────────────┐
                                     │ Return error to │
                                     │ user with       │
                                     │ failure details │
                                     └─────────────────┘

    Recovery Options:
    ┌─────────────────────────────────────────────────────────────────────────┐
    │ 1. Fix the issue and re-run Lightning                                   │
    │    - Finished tables will be skipped                                    │
    │    - Failed tables will be resubmitted                                  │
    │                                                                         │
    │ 2. Use tidb-lightning-ctl --checkpoint-error-ignore='db.table'          │
    │    - Mark the failed table as pending for retry                         │
    │                                                                         │
    │ 3. Use tidb-lightning-ctl --checkpoint-error-destroy='db.table'         │
    │    - Remove checkpoint and start fresh for that table                   │
    └─────────────────────────────────────────────────────────────────────────┘
```

---

## 6. API Reference

### 6.1 SDK API

#### Creating the SDK

```go
import "github.com/pingcap/tidb/pkg/importsdk"

// Create SDK with options
sdk, err := importsdk.NewImportSDK(
    ctx,
    "s3://bucket/path?access-key=xxx&secret-access-key=yyy",
    db,
    importsdk.WithConcurrency(4),
    importsdk.WithLogger(logger),
)
if err != nil {
    return err
}
defer sdk.Close()
```

#### File Operations

```go
// Create all schemas and tables from source
err := sdk.CreateSchemasAndTables(ctx)

// Create specific schema and table
err := sdk.CreateSchemaAndTableByName(ctx, "mydb", "mytable")

// Get metadata for all tables
metas, err := sdk.GetTableMetas(ctx)

// Get metadata for specific table
meta, err := sdk.GetTableMetaByName(ctx, "mydb", "mytable")

// Get total data size
totalSize := sdk.GetTotalSize(ctx)
```

#### SQL Generation

```go
// Generate IMPORT INTO SQL
options := &importsdk.ImportOptions{
    Format:   "csv",
    Thread:   4,
    Detached: true,
    GroupKey: "my-import-group",
}
sql, err := sdk.GenerateImportSQL(meta, options)
```

#### Job Management

```go
// Submit import job
jobID, err := sdk.SubmitJob(ctx, sql)

// Get job status
status, err := sdk.GetJobStatus(ctx, jobID)

// Cancel job
err := sdk.CancelJob(ctx, jobID)

// Get group summary
summary, err := sdk.GetGroupSummary(ctx, "my-import-group")

// Get all jobs in group
jobs, err := sdk.GetJobsByGroup(ctx, "my-import-group")
```

### 6.2 Import Options

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `Format` | string | Data format (csv/sql/parquet) | Auto-detect |
| `Thread` | int | Import concurrency | TiDB default |
| `DiskQuota` | string | Disk space limit | Unlimited |
| `MaxWriteSpeed` | string | Write speed limit | Unlimited |
| `SplitFile` | bool | Enable file splitting | false |
| `RecordErrors` | int64 | Max error records | 0 |
| `Detached` | bool | Async execution | true (recommended) |
| `CloudStorageURI` | string | External sort storage | None |
| `GroupKey` | string | Job group identifier | Generated |
| `SkipRows` | int | Header rows to skip | 0 |
| `CharacterSet` | string | Character encoding | Auto |
| `ChecksumTable` | string | Checksum mode | required |
| `DisableTiKVImportMode` | bool | Disable import mode | false |
| `MaxEngineSize` | string | Max engine size | TiDB default |
| `DisablePrecheck` | bool | Skip prechecks | false |
| `ResourceParameters` | string | Storage credentials | From source URL |

### 6.3 Job Status

| Field | Type | Description |
|-------|------|-------------|
| `JobID` | int64 | Unique job identifier |
| `GroupKey` | string | Job group key |
| `DataSource` | string | Source file path |
| `TargetTable` | string | Target table name |
| `TableID` | int64 | Internal table ID |
| `Phase` | string | Current phase (import/postprocess) |
| `Status` | string | Job status |
| `SourceFileSize` | string | Source data size |
| `ImportedRows` | int64 | Rows imported |
| `ResultMessage` | string | Result or error message |
| `CreateTime` | time.Time | Job creation time |
| `StartTime` | time.Time | Job start time |
| `EndTime` | time.Time | Job completion time |
| `ProcessedSize` | string | Data processed |
| `TotalSize` | string | Total data size |
| `Percent` | string | Progress percentage |
| `Speed` | string | Import speed |
| `ETA` | string | Estimated time remaining |

#### Job Status Values

| Status | Description |
|--------|-------------|
| `pending` | Job submitted, waiting to start |
| `running` | Job is actively importing |
| `finished` | Job completed successfully |
| `failed` | Job failed with error |
| `cancelled` | Job was cancelled |

---

## 7. Configuration

### 7.1 Lightning Configuration Mapping

```toml
# tidb-lightning.toml

[lightning]
backend = "import-into"  # Use the new backend

[tikv-importer]
# These options are translated to IMPORT INTO WITH clauses
disk-quota = "100GB"                # → DISK_QUOTA='100GB'
store-write-bw-limit = "100MiB"     # → MAX_WRITE_SPEED='100MiB'
engine-mem-cache-size = "1GB"       # → MAX_ENGINE_SIZE='1GB'

[mydumper]
data-source-dir = "s3://bucket/path"
data-character-set = "utf8mb4"      # → CHARACTER_SET='utf8mb4'
filter = ['*.*']

[mydumper.csv]
separator = ','                      # → FIELDS_TERMINATED_BY=','
delimiter = '"'                      # → FIELDS_ENCLOSED_BY='"'
header = true                        # → SKIP_ROWS=1
null = '\N'                          # → FIELDS_DEFINED_NULL_BY='\N'

[post-restore]
checksum = "required"               # → CHECKSUM_TABLE='required'

[app]
table-concurrency = 6               # Concurrent table submissions
check-requirements = true           # Enable prechecks

[app.max-error]
type = 100                          # → RECORD_ERRORS=100
```

### 7.2 Checkpoint Configuration

```toml
[checkpoint]
enable = true
driver = "file"                     # or "mysql"
dsn = "/tmp/lightning_checkpoint.pb"
# For MySQL: dsn = "user:pass@tcp(127.0.0.1:4000)/lightning_checkpoint"
schema = "lightning_checkpoint"
keep-after-success = "remove"       # or "origin"
```

---

## 8. Testing Strategy

### Unit Tests

- `pkg/importsdk/*_test.go`: SDK component tests with mock database
- `pkg/lightning/importinto/*_test.go`: Backend component tests

### Integration Tests

- `tests/realtikvtest/importintotest/sdk_test.go`: End-to-end SDK tests with real TiDB
- Tests cover:
  - File scanning and metadata extraction
  - Schema/table creation
  - SQL generation
  - Job submission and monitoring
  - Checkpoint recovery
  - Error handling and cancellation

### Lightning Integration Test Porting

Port existing Lightning integration tests from `lightning/tests/` to validate the new `import-into` backend:

- Verify backward compatibility with existing Lightning test cases
- Ensure all supported data formats (CSV, SQL, Parquet) work correctly
- Validate checkpoint recovery scenarios match original behavior
- Test error handling and edge cases from legacy test suite

### Test Coverage Targets

| Component | Target Coverage |
|-----------|-----------------|
| importsdk | > 90% |
| importinto | > 85% |

---

## 9. Future Enhancements

1. **Incremental Import**: Support for incremental imports

2. **Auto-Provisioned Shared Storage**: Automatically provision and manage shared storage (e.g., TiDB's built-in shared storage or temporary cloud storage), eliminating the need for users to manually upload data to external storage services like S3 or GCS. This simplifies the import workflow by allowing users to import directly from local files without pre-staging data.

3. **Automatic Column Mapping**: Intelligently match CSV column names to table columns regardless of column order. When the CSV header column names match table column names but appear in a different order, the system will automatically map them correctly, reducing manual configuration and preventing data import errors.

---

## 10. References

- [TiDB IMPORT INTO Documentation](https://docs.pingcap.com/tidb/stable/sql-statement-import-into)
- [TiDB Lightning User Guide](https://docs.pingcap.com/tidb/stable/tidb-lightning-overview)
- [TiDB Distributed Task Framework](https://docs.pingcap.com/tidb/stable/tidb-distributed-execution-framework)
- [External Storage Configuration](https://docs.pingcap.com/tidb/stable/backup-and-restore-storages)

---

## Appendix A: Error Codes

| Error | Description | Resolution |
|-------|-------------|------------|
| `ErrNoDatabasesFound` | No databases found in source | Check source path and filter rules |
| `ErrSchemaNotFound` | Specified schema not found | Verify schema name exists in source |
| `ErrTableNotFound` | Specified table not found | Verify table name exists in source |
| `ErrNoTableDataFiles` | Table has no data files | Check file naming convention |
| `ErrWildcardNotSpecific` | Cannot generate unique wildcard | Files may have conflicting patterns |
| `ErrJobNotFound` | Import job not found | Job may have been cleaned up |
| `ErrNoJobIDReturned` | No job ID in response | Check TiDB connection and permissions |
| `ErrMultipleFieldsDefinedNullBy` | Multiple NULL definitions | Use single NULL definition |

## Appendix B: SQL Statements Used

```sql
-- Create database/table (via mydump.SchemaImporter)
CREATE DATABASE IF NOT EXISTS `mydb`;
CREATE TABLE IF NOT EXISTS `mydb`.`mytable` (...);

-- Submit import job
IMPORT INTO `mydb`.`mytable`
FROM 's3://bucket/path/*.csv'
FORMAT 'csv'
WITH THREAD=4, DETACHED, GROUP_KEY='xxx';

-- Query job status
SHOW IMPORT JOB <job_id>;
SHOW IMPORT JOBS WHERE GROUP_KEY = 'xxx';
SHOW IMPORT GROUP 'xxx';

-- Cancel job
CANCEL IMPORT JOB <job_id>;
```
