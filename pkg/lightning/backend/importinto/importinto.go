// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importinto

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
)

type importIntoBackend struct {
	db            *sql.DB
	cfg           *config.Config
	checkpointsDB checkpoints.DB
	tableName     string   // Store single table name directly
	filePaths     []string // Store file paths directly without mapping
	baseURI       string   // Base URI for source files
	importJobID   string   // Track the current import job ID
}

func NewImportIntoBackend(
	ctx context.Context,
	db *sql.DB,
	cfg *config.Config,
	checkpointsDB checkpoints.DB,
) backend.Backend {
	return &importIntoBackend{
		db:            db,
		cfg:           cfg,
		checkpointsDB: checkpointsDB,
		filePaths:     make([]string, 0),
	}
}

func (be *importIntoBackend) Close() {
	// cleanup resources
}

func (be *importIntoBackend) RetryImportDelay() time.Duration {
	return time.Second
}

func (be *importIntoBackend) ShouldPostProcess() bool {
	return false
}

func (be *importIntoBackend) OpenEngine(ctx context.Context, config *backend.EngineConfig, engineUUID uuid.UUID) error {
	// Store the table name directly
	be.tableName = common.UniqueTable(config.TableInfo.DB, config.TableInfo.Name)
	be.baseURI = config.ImportInto.BaseURI

	// Collect files directly
	if config.ImportInto.TableMeta != nil {
		be.collectDataFiles(config.ImportInto.TableMeta)
	}

	return nil
}

// Simplified to collect files directly without table mapping
func (be *importIntoBackend) collectDataFiles(tableMeta *mydump.MDTableMeta) {
	for _, dataFile := range tableMeta.DataFiles {
		be.filePaths = append(be.filePaths, dataFile.FileMeta.Path)
	}
}

func (be *importIntoBackend) CloseEngine(ctx context.Context, config *backend.EngineConfig, engineUUID uuid.UUID) error {
	return nil
}

func (be *importIntoBackend) ImportEngine(ctx context.Context, engineUUID uuid.UUID, regionSplitSize, regionSplitKeys int64) error {
	// No need to look up by engine UUID anymore
	if len(be.filePaths) == 0 {
		return nil
	}

	if be.tableName == "" {
		return fmt.Errorf("table name not set")
	}

	// Check if we already have a job ID stored in the backend
	if be.importJobID != "" {
		// Check if job is complete
		complete, err := be.checkJobStatus(ctx, be.importJobID)
		if err != nil {
			return err
		}

		if complete {
			return nil // Job already completed
		}
		return nil // Job is still running
	}

	// Check for existing job ID in checkpoint
	existingJobID, err := be.getTableJobID(ctx, be.tableName)
	if err != nil {
		return err
	}

	if existingJobID != "" {
		// Check if job is complete
		complete, err := be.checkJobStatus(ctx, existingJobID)
		if err != nil {
			return err
		}

		if complete {
			// Job already completed successfully
			be.importJobID = existingJobID // Remember it for future calls
			return nil
		}
		// Job is still running
		be.importJobID = existingJobID // Remember it for future calls
		return nil
	}

	// Start new import job
	jobID, err := be.executeImportInto(ctx, be.tableName, be.filePaths)
	if err != nil {
		return err
	}

	// Remember job ID for future calls
	be.importJobID = jobID

	// Update checkpoint with job ID
	return be.updateTableJobID(ctx, be.tableName, jobID)
}

func (be *importIntoBackend) executeImportInto(ctx context.Context, tableName string, filePaths []string) (string, error) {
	wildcardPath, err := generateWildcard(filePaths)
	if err != nil {
		return "", err
	}

	fullURI, err := be.buildFullURIWithAuth(wildcardPath)
	if err != nil {
		return "", err
	}

	importSQL := fmt.Sprintf(
		`IMPORT INTO %s FROM "%s" WITH DETACHED`,
		tableName,
		fullURI,
	)

 // Use SQLWithRetry to handle SQL execution with retries
    exec := common.SQLWithRetry{
        DB:     be.db,
    }
    
    var jobID string
    err = exec.QueryRow(ctx, "execute import into", importSQL, func(row *sql.Row) error {
        // Scan the first column (Job_ID) from the result
        return row.Scan(&jobID)
    })
    
    return jobID, err
}

// getTableJobID retrieves any existing job ID from the table checkpoint
func (be *importIntoBackend) getTableJobID(ctx context.Context, tableName string) (string, error) {
	cp, err := be.checkpointsDB.Get(ctx, tableName)
	if err != nil {
		// If checkpoint not found, return empty string
		if strings.Contains(err.Error(), "not found") {
			return "", nil
		}
		return "", err
	}

	// Return job ID from table checkpoint
	return cp.ImportJobID, nil
}

// checkJobStatus queries TiDB to check if a job has completed
func (be *importIntoBackend) checkJobStatus(ctx context.Context, jobID string) (bool, error) {
	query := fmt.Sprintf("SHOW JOB %s", jobID)

	var status string
	err := be.db.QueryRowContext(ctx, query).Scan(&status)
	if err != nil {
		return false, err
	}

	// Return true if job completed successfully
	return status == "COMPLETED", nil
}

// updateTableJobID updates the table checkpoint with job ID
func (be *importIntoBackend) updateTableJobID(ctx context.Context, tableName string, jobID string) error {
	// Create job ID merger for table checkpoint
	merger := &checkpoints.TableJobIDMerger{
		JobID: jobID,
	}

	// Create checkpoint diff
	cpd := checkpoints.NewTableCheckpointDiff()
	merger.MergeInto(cpd)

	// Update checkpoint directly
	return be.checkpointsDB.Update(ctx, map[string]*checkpoints.TableCheckpointDiff{
		tableName: cpd,
	})
}

func (be *importIntoBackend) buildFullURIWithAuth(wildcardPath string) (string, error) {
	parsedURI, err := url.Parse(be.cfg.Mydumper.SourceDir)
	if err != nil {
		return "", fmt.Errorf("failed to parse base URI: %v", err)
	}

	return be.rebuildCloudStorageURI(parsedURI, wildcardPath)
}

// rebuildCloudStorageURI 重新构建云存储 URI，保留认证参数
func (be *importIntoBackend) rebuildCloudStorageURI(parsedURI *url.URL, wildcardPath string) (string, error) {
	newPath := strings.TrimSuffix(parsedURI.Path, "/") + "/" + wildcardPath

	newURI := &url.URL{
		Scheme:   parsedURI.Scheme,
		Host:     parsedURI.Host,
		Path:     newPath,
		RawQuery: parsedURI.RawQuery,
	}

	return newURI.String(), nil
}

// generateWildcard creates a wildcard pattern that matches only this table's files
func generateWildcard(files []string) (string, error) {
	if len(files) == 0 {
		return "", errors.New("no data files to generate wildcard pattern")
	}

	// If there's only one file, we can just return its path
	if len(files) == 1 {
		return files[0], nil
	}

	p := generateMydumperPattern(files)
	if p != "" {
		return p, nil
	}

	p = generatePrefixSuffixPattern(files)
	if p != "" {
		return p, nil
	}

	return "", errors.New("unable to generate a specific wildcard pattern for this table's data files")
}

// generateMydumperPattern creates a pattern optimized for Mydumper naming conventions
func generateMydumperPattern(paths []string) string {
	// Check if paths appear to follow Mydumper naming convention
	if len(paths) == 0 {
		return ""
	}

	// Extract common database and table names from filenames
	dbName, tableName := extractMydumperNames(paths)
	if dbName == "" || tableName == "" {
		return "" // Not a Mydumper pattern or inconsistent names
	}

	// Check if there's a directory component in the paths
	dirPrefix := extractCommonDirectory(paths)

	// Generate pattern based on Mydumper format
	if dirPrefix == "" {
		// Files are in the current directory
		return dbName + "." + tableName + ".*.sql"
	}

	// Files are in a specific directory
	return dirPrefix + dbName + "." + tableName + ".*.sql"
}

// extractMydumperNames extracts database and table names from Mydumper-formatted paths
func extractMydumperNames(paths []string) (dbName, tableName string) {
	// Extract filenames from paths
	filenames := make([]string, 0, len(paths))
	for _, path := range paths {
		// Get just the filename part
		lastSlash := strings.LastIndex(path, "/")
		if lastSlash >= 0 {
			filenames = append(filenames, path[lastSlash+1:])
		} else {
			filenames = append(filenames, path)
		}
	}

	for i, filename := range filenames {
		// Skip schema files
		if strings.HasSuffix(filename, "-schema.sql") || strings.HasSuffix(filename, "-schema-create.sql") {
			continue
		}

		// Skip non-SQL files (Mydumper typically produces .sql files)
		if !strings.HasSuffix(filename, ".sql") {
			return "", ""
		}

		// Parse "{db}.{table}.sql" or "{db}.{table}.{part}.sql"
		parts := strings.Split(filename, ".")
		if len(parts) < 3 {
			return "", "" // Not enough parts for Mydumper format
		}

		currentDB := parts[0]
		currentTable := parts[1]

		// In first iteration, set the names
		if i == 0 {
			dbName = currentDB
			tableName = currentTable
			continue
		}

		// In subsequent iterations, verify consistency
		if dbName != currentDB || tableName != currentTable {
			return "", "" // Inconsistent names, not following Mydumper pattern
		}
	}

	return dbName, tableName
}

// extractCommonDirectory gets the common directory prefix from paths
func extractCommonDirectory(paths []string) string {
	if len(paths) == 0 {
		return ""
	}

	// Find common prefix for all paths
	prefix := longestCommonPrefix(paths)

	// Find last directory separator in prefix
	lastSlash := strings.LastIndex(prefix, "/")
	if lastSlash >= 0 {
		return prefix[:lastSlash+1]
	}

	return ""
}

// longestCommonPrefix finds the longest string that is a prefix of all strings in the slice
func longestCommonPrefix(strs []string) string {
	if len(strs) == 0 {
		return ""
	}

	prefix := strs[0]
	for _, s := range strs[1:] {
		i := 0
		for i < len(prefix) && i < len(s) && prefix[i] == s[i] {
			i++
		}
		prefix = prefix[:i]
		if prefix == "" {
			break
		}
	}

	return prefix
}

// longestCommonSuffix finds the longest string that is a suffix of all strings in the slice
func longestCommonSuffix(strs []string) string {
	if len(strs) == 0 {
		return ""
	}

	suffix := strs[0]
	for _, s := range strs[1:] {
		i := 0
		for i < len(suffix) && i < len(s) && suffix[len(suffix)-i-1] == s[len(s)-i-1] {
			i++
		}
		suffix = suffix[len(suffix)-i:]
		if suffix == "" {
			break
		}
	}

	return suffix
}

// generatePrefixSuffixPattern creates a wildcard pattern using common prefix and suffix
func generatePrefixSuffixPattern(paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	if len(paths) == 1 {
		return paths[0]
	}

	prefix := longestCommonPrefix(paths)
	suffix := longestCommonSuffix(paths)

	minLen := len(paths[0])
	for _, p := range paths[1:] {
		if len(p) < minLen {
			minLen = len(p)
		}
	}
	maxSuffixLen := minLen - len(prefix)
	if len(suffix) > maxSuffixLen {
		suffix = suffix[len(suffix)-maxSuffixLen:]
	}

	return prefix + "*" + suffix
}

func (be *importIntoBackend) CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
	return nil
}

func (be *importIntoBackend) FlushEngine(ctx context.Context, engineUUID uuid.UUID) error {
	return nil // no-op for import into
}

func (be *importIntoBackend) FlushAllEngines(ctx context.Context) error {
	return nil // no-op for import into
}

// LocalWriter 返回一个 no-op writer
func (be *importIntoBackend) LocalWriter(ctx context.Context, cfg *backend.LocalWriterConfig, engineUUID uuid.UUID) (backend.EngineWriter, error) {
	return nil, nil
}
