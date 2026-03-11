package postgresql

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// LSNManager manages LSN (Log Sequence Number) tracking and checkpoints
type LSNManager struct {
	pool            *pgxpool.Pool
	checkpointFile  string
	mutex           sync.RWMutex
	lastCommittedLSN string
	lastReceivedLSN  string
}

// LSNCheckpoint represents a PostgreSQL checkpoint with LSN information
type LSNCheckpoint struct {
	JobID           string    `json:"job_id"`
	LSN             string    `json:"lsn"`
	Timeline        uint32    `json:"timeline"`
	SlotName        string    `json:"slot_name"`
	Publication     string    `json:"publication"`
	LastUpdate      time.Time `json:"last_update"`
	CommitFrequency int       `json:"commit_frequency"`
	BatchSize       int       `json:"batch_size"`
}

// NewLSNManager creates a new LSN manager
func NewLSNManager(pool *pgxpool.Pool, checkpointFile string) *LSNManager {
	return &LSNManager{
		pool:           pool,
		checkpointFile: checkpointFile,
	}
}

// GetCurrentLSN retrieves the current WAL LSN from PostgreSQL
func (lm *LSNManager) GetCurrentLSN(ctx context.Context) (string, error) {
	var lsn string
	err := lm.pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()").Scan(&lsn)
	if err != nil {
		return "", fmt.Errorf("failed to get current LSN: %w", err)
	}
	return lsn, nil
}

// GetFlushLSN retrieves the current WAL flush LSN
func (lm *LSNManager) GetFlushLSN(ctx context.Context) (string, error) {
	var lsn string
	err := lm.pool.QueryRow(ctx, "SELECT pg_current_wal_flush_lsn()").Scan(&lsn)
	if err != nil {
		return "", fmt.Errorf("failed to get flush LSN: %w", err)
	}
	return lsn, nil
}

// GetInsertLSN retrieves the current WAL insert LSN
func (lm *LSNManager) GetInsertLSN(ctx context.Context) (string, error) {
	var lsn string
	err := lm.pool.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()").Scan(&lsn)
	if err != nil {
		return "", fmt.Errorf("failed to get insert LSN: %w", err)
	}
	return lsn, nil
}

// CompareLSN compares two LSN values
// Returns: -1 if lsn1 < lsn2, 0 if equal, 1 if lsn1 > lsn2
func (lm *LSNManager) CompareLSN(lsn1, lsn2 string) (int, error) {
	// Parse LSN format: XXXXXXXX/XXXXXXXX
	parts1 := strings.Split(lsn1, "/")
	parts2 := strings.Split(lsn2, "/")

	if len(parts1) != 2 || len(parts2) != 2 {
		return 0, fmt.Errorf("invalid LSN format")
	}

	// Parse high and low parts
	high1, err := strconv.ParseUint(parts1[0], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse LSN high part: %w", err)
	}
	low1, err := strconv.ParseUint(parts1[1], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse LSN low part: %w", err)
	}

	high2, err := strconv.ParseUint(parts2[0], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse LSN high part: %w", err)
	}
	low2, err := strconv.ParseUint(parts2[1], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse LSN low part: %w", err)
	}

	// Compare as 64-bit values
	val1 := (high1 << 32) | low1
	val2 := (high2 << 32) | low2

	if val1 < val2 {
		return -1, nil
	} else if val1 > val2 {
		return 1, nil
	}
	return 0, nil
}

// GetLSNDifference calculates the byte difference between two LSNs
func (lm *LSNManager) GetLSNDifference(ctx context.Context, lsn1, lsn2 string) (int64, error) {
	var diff int64
	err := lm.pool.QueryRow(ctx, "SELECT pg_wal_lsn_diff($1, $2)", lsn1, lsn2).Scan(&diff)
	if err != nil {
		return 0, fmt.Errorf("failed to calculate LSN difference: %w", err)
	}
	return diff, nil
}

// AdvanceLSN advances LSN by specified bytes
func (lm *LSNManager) AdvanceLSN(lsn string, bytes int64) (string, error) {
	// Parse current LSN
	parts := strings.Split(lsn, "/")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid LSN format: %s", lsn)
	}

	high, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return "", fmt.Errorf("failed to parse LSN high part: %w", err)
	}
	low, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return "", fmt.Errorf("failed to parse LSN low part: %w", err)
	}

	// Convert to 64-bit value and advance
	val := (high << 32) | low
	val += uint64(bytes)

	// Convert back to LSN format
	newHigh := uint32(val >> 32)
	newLow := uint32(val & 0xFFFFFFFF)

	return fmt.Sprintf("%X/%X", newHigh, newLow), nil
}

// SetLastCommittedLSN updates the last committed LSN
func (lm *LSNManager) SetLastCommittedLSN(lsn string) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()
	lm.lastCommittedLSN = lsn
}

// GetLastCommittedLSN retrieves the last committed LSN
func (lm *LSNManager) GetLastCommittedLSN() string {
	lm.mutex.RLock()
	defer lm.mutex.RUnlock()
	return lm.lastCommittedLSN
}

// SetLastReceivedLSN updates the last received LSN
func (lm *LSNManager) SetLastReceivedLSN(lsn string) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()
	lm.lastReceivedLSN = lsn
}

// GetLastReceivedLSN retrieves the last received LSN
func (lm *LSNManager) GetLastReceivedLSN() string {
	lm.mutex.RLock()
	defer lm.mutex.RUnlock()
	return lm.lastReceivedLSN
}

// SaveCheckpoint saves checkpoint information to file
func (lm *LSNManager) SaveCheckpoint(checkpoint *LSNCheckpoint) error {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	// Load existing checkpoints
	checkpoints, err := lm.loadCheckpointsFromFile()
	if err != nil {
		log.Printf("Warning: failed to load existing checkpoints: %v", err)
		checkpoints = make(map[string]*LSNCheckpoint)
	}

	// Update checkpoint
	checkpoint.LastUpdate = time.Now()
	checkpoints[checkpoint.JobID] = checkpoint

	// Save to file
	data, err := json.MarshalIndent(checkpoints, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoints: %w", err)
	}

	if err := os.WriteFile(lm.checkpointFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint file: %w", err)
	}

	return nil
}

// LoadCheckpoint loads checkpoint information for a specific job
func (lm *LSNManager) LoadCheckpoint(jobID string) (*LSNCheckpoint, error) {
	lm.mutex.RLock()
	defer lm.mutex.RUnlock()

	checkpoints, err := lm.loadCheckpointsFromFile()
	if err != nil {
		return nil, err
	}

	checkpoint, exists := checkpoints[jobID]
	if !exists {
		return nil, fmt.Errorf("no checkpoint found for job %s", jobID)
	}

	return checkpoint, nil
}

// DeleteCheckpoint removes checkpoint for a specific job
func (lm *LSNManager) DeleteCheckpoint(jobID string) error {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	checkpoints, err := lm.loadCheckpointsFromFile()
	if err != nil {
		return err
	}

	delete(checkpoints, jobID)

	// Save updated checkpoints
	data, err := json.MarshalIndent(checkpoints, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoints: %w", err)
	}

	if err := os.WriteFile(lm.checkpointFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint file: %w", err)
	}

	log.Printf("Deleted checkpoint for job %s", jobID)
	return nil
}

// ListCheckpoints returns all checkpoints
func (lm *LSNManager) ListCheckpoints() (map[string]*LSNCheckpoint, error) {
	lm.mutex.RLock()
	defer lm.mutex.RUnlock()

	return lm.loadCheckpointsFromFile()
}

// loadCheckpointsFromFile loads checkpoints from the file
func (lm *LSNManager) loadCheckpointsFromFile() (map[string]*LSNCheckpoint, error) {
	data, err := os.ReadFile(lm.checkpointFile)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]*LSNCheckpoint), nil
		}
		return nil, fmt.Errorf("failed to read checkpoint file: %w", err)
	}

	var checkpoints map[string]*LSNCheckpoint
	if err := json.Unmarshal(data, &checkpoints); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoints: %w", err)
	}

	return checkpoints, nil
}

// GetReplicationLag calculates replication lag for a slot
func (lm *LSNManager) GetReplicationLag(ctx context.Context, slotName string) (*ReplicationLagInfo, error) {
	lagInfo := &ReplicationLagInfo{
		SlotName:  slotName,
		Timestamp: time.Now(),
	}

	query := `
		SELECT 
			COALESCE(restart_lsn::text, '') as restart_lsn,
			COALESCE(confirmed_flush_lsn::text, '') as confirmed_flush_lsn,
			active,
			CASE 
				WHEN confirmed_flush_lsn IS NOT NULL AND pg_current_wal_lsn() IS NOT NULL
				THEN pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
				ELSE 0
			END as lag_bytes,
			CASE 
				WHEN confirmed_flush_lsn IS NOT NULL AND pg_current_wal_lsn() IS NOT NULL
				THEN EXTRACT(epoch FROM (now() - pg_stat_file('pg_wal/' || pg_walfile_name(confirmed_flush_lsn))))
				ELSE 0
			END as lag_seconds
		FROM pg_replication_slots 
		WHERE slot_name = $1`

	err := lm.pool.QueryRow(ctx, query, slotName).Scan(
		&lagInfo.RestartLSN,
		&lagInfo.ConfirmedFlushLSN,
		&lagInfo.Active,
		&lagInfo.LagBytes,
		&lagInfo.LagSeconds,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get replication lag: %w", err)
	}

	// Get current LSN
	currentLSN, err := lm.GetCurrentLSN(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get current LSN: %w", err)
	}
	lagInfo.CurrentLSN = currentLSN

	return lagInfo, nil
}

// ValidateLSN validates LSN format
func (lm *LSNManager) ValidateLSN(lsn string) error {
	parts := strings.Split(lsn, "/")
	if len(parts) != 2 {
		return fmt.Errorf("invalid LSN format: expected XXXXXXXX/XXXXXXXX")
	}

	for i, part := range parts {
		if len(part) != 8 {
			return fmt.Errorf("invalid LSN part %d: expected 8 hex digits", i+1)
		}
		if _, err := strconv.ParseUint(part, 16, 32); err != nil {
			return fmt.Errorf("invalid LSN part %d: not valid hex: %w", i+1, err)
		}
	}

	return nil
}

// ReplicationLagInfo contains replication lag information
type ReplicationLagInfo struct {
	SlotName           string    `json:"slot_name"`
	CurrentLSN         string    `json:"current_lsn"`
	RestartLSN         string    `json:"restart_lsn"`
	ConfirmedFlushLSN  string    `json:"confirmed_flush_lsn"`
	Active             bool      `json:"active"`
	LagBytes           int64     `json:"lag_bytes"`
	LagSeconds         float64   `json:"lag_seconds"`
	Timestamp          time.Time `json:"timestamp"`
}

// CheckpointManager manages periodic checkpoint commits
type CheckpointManager struct {
	lsnManager      *LSNManager
	slotManager     *ReplicationSlotManager
	commitFrequency time.Duration
	batchSize       int
	ctx             context.Context
	cancel          context.CancelFunc
	mutex           sync.Mutex
	running         bool
}

// NewCheckpointManager creates a new checkpoint manager
func NewCheckpointManager(lsnManager *LSNManager, slotManager *ReplicationSlotManager) *CheckpointManager {
	return &CheckpointManager{
		lsnManager:      lsnManager,
		slotManager:     slotManager,
		commitFrequency: 30 * time.Second, // Default commit frequency
		batchSize:       1000,              // Default batch size
	}
}

// Start starts the checkpoint manager
func (cm *CheckpointManager) Start(ctx context.Context, jobID, slotName string) error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if cm.running {
		return fmt.Errorf("checkpoint manager already running")
	}

	cm.ctx, cm.cancel = context.WithCancel(ctx)
	cm.running = true

	// Start checkpoint goroutine
	go cm.run(jobID, slotName)
	
	log.Printf("CheckpointManager started for job %s, slot %s", jobID, slotName)
	return nil
}

// Stop stops the checkpoint manager
func (cm *CheckpointManager) Stop() error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if !cm.running {
		return nil
	}

	cm.cancel()
	cm.running = false
	
	log.Printf("CheckpointManager stopped")
	return nil
}

// SetCommitFrequency sets the checkpoint commit frequency
func (cm *CheckpointManager) SetCommitFrequency(frequency time.Duration) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	cm.commitFrequency = frequency
}

// SetBatchSize sets the batch size for commits
func (cm *CheckpointManager) SetBatchSize(size int) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	cm.batchSize = size
}

// run is the main loop for the checkpoint manager
func (cm *CheckpointManager) run(jobID, slotName string) {
	ticker := time.NewTicker(cm.commitFrequency)
	defer ticker.Stop()

	for {
		select {
		case <-cm.ctx.Done():
			return
		case <-ticker.C:
			if err := cm.performCheckpoint(jobID, slotName); err != nil {
				log.Printf("Failed to perform checkpoint: %v", err)
			}
		}
	}
}

// performCheckpoint performs a checkpoint operation
func (cm *CheckpointManager) performCheckpoint(jobID, slotName string) error {
	// Get last received LSN
	lastLSN := cm.lsnManager.GetLastReceivedLSN()
	if lastLSN == "" {
		return nil // No LSN to commit
	}

	// Create checkpoint
	checkpoint := &LSNCheckpoint{
		JobID:           jobID,
		LSN:             lastLSN,
		SlotName:        slotName,
		CommitFrequency: int(cm.commitFrequency.Seconds()),
		BatchSize:       cm.batchSize,
	}

	// Save checkpoint
	if err := cm.lsnManager.SaveCheckpoint(checkpoint); err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	// Advance replication slot
	if err := cm.slotManager.AdvanceReplicationSlot(cm.ctx, slotName, lastLSN); err != nil {
		log.Printf("Warning: failed to advance replication slot: %v", err)
		// Don't return error as checkpoint was saved successfully
	}

	// Update last committed LSN
	cm.lsnManager.SetLastCommittedLSN(lastLSN)

	return nil
}
